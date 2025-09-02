package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"deltacat/backend/pkg/models"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Logger handles security audit logging
type Logger struct {
	db           *gorm.DB
	cache        *redis.Client
	logger       *zap.Logger
	retentionDays int
	asyncQueue   chan *Event
}

// Event represents a security audit event
type Event struct {
	Type         string
	UserID       *uuid.UUID
	ClientID     string
	Resource     string
	Action       string
	Result       string
	IPAddress    string
	UserAgent    string
	Metadata     map[string]interface{}
	Timestamp    time.Time
}

// NewLogger creates a new audit logger
func NewLogger(db *gorm.DB, cache *redis.Client, logger *zap.Logger, retentionDays int) *Logger {
	al := &Logger{
		db:            db,
		cache:         cache,
		logger:        logger,
		retentionDays: retentionDays,
		asyncQueue:    make(chan *Event, 1000),
	}

	// Start async worker
	go al.asyncWorker()

	// Schedule cleanup
	go al.scheduleCleanup()

	return al
}

// LogOAuthEvent logs an OAuth-related event
func (l *Logger) LogOAuthEvent(eventType string, userID *uuid.UUID, clientID string, scopes []string, c *fiber.Ctx, success bool, errorMsg string) {
	metadata := make(map[string]interface{})
	if len(scopes) > 0 {
		metadata["scopes"] = scopes
	}
	if c != nil {
		metadata["request_id"] = c.Get("X-Request-ID")
		metadata["session_id"] = c.Get("X-Session-ID")
	}

	event := &Event{
		Type:      eventType,
		UserID:    userID,
		ClientID:  clientID,
		Action:    eventType,
		Result:    l.resultString(success),
		IPAddress: l.getClientIP(c),
		UserAgent: l.getUserAgent(c),
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	if !success && errorMsg != "" {
		event.Metadata["error"] = errorMsg
	}

	l.Log(event)
}

// LogDataOperation logs data access operations
func (l *Logger) LogDataOperation(userID uuid.UUID, operation, resource string, c *fiber.Ctx, success bool, details map[string]interface{}) {
	event := &Event{
		Type:      "DATA_OPERATION",
		UserID:    &userID,
		Resource:  resource,
		Action:    operation,
		Result:    l.resultString(success),
		IPAddress: l.getClientIP(c),
		UserAgent: l.getUserAgent(c),
		Metadata:  details,
		Timestamp: time.Now(),
	}

	l.Log(event)
}

// LogSecurityEvent logs general security events
func (l *Logger) LogSecurityEvent(eventType, action string, userID *uuid.UUID, c *fiber.Ctx, success bool, metadata map[string]interface{}) {
	event := &Event{
		Type:      eventType,
		UserID:    userID,
		Action:    action,
		Result:    l.resultString(success),
		IPAddress: l.getClientIP(c),
		UserAgent: l.getUserAgent(c),
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	l.Log(event)
}

// Log sends an event to the async queue
func (l *Logger) Log(event *Event) {
	select {
	case l.asyncQueue <- event:
		// Event queued successfully
	default:
		// Queue is full, log synchronously
		l.persistEvent(event)
	}
}

// asyncWorker processes events from the queue
func (l *Logger) asyncWorker() {
	for event := range l.asyncQueue {
		l.persistEvent(event)
	}
}

// persistEvent saves an event to the database
func (l *Logger) persistEvent(event *Event) {
	// Create audit log record
	auditLog := models.OAuthAuditLog{
		ID:        uuid.New(),
		EventType: event.Type,
		UserID:    event.UserID,
		ClientID:  event.ClientID,
		IPAddress: event.IPAddress,
		UserAgent: event.UserAgent,
		Success:   event.Result == "SUCCESS",
		Metadata:  models.JSONB(event.Metadata),
		CreatedAt: event.Timestamp,
	}

	// Add resource and action to metadata if present
	if event.Resource != "" {
		auditLog.Metadata["resource"] = event.Resource
	}
	if event.Action != "" && event.Action != event.Type {
		auditLog.Metadata["action"] = event.Action
	}

	// Save to database
	if err := l.db.Create(&auditLog).Error; err != nil {
		l.logger.Error("Failed to persist audit event",
			zap.String("event_type", event.Type),
			zap.Error(err))
		
		// Try to cache failed events for retry
		l.cacheFailedEvent(event)
	} else {
		// Update metrics
		l.updateMetrics(event)
	}
}

// cacheFailedEvent caches events that failed to persist
func (l *Logger) cacheFailedEvent(event *Event) {
	ctx := context.Background()
	key := fmt.Sprintf("audit:failed:%d", time.Now().UnixNano())
	
	data, err := json.Marshal(event)
	if err != nil {
		l.logger.Error("Failed to marshal failed event", zap.Error(err))
		return
	}

	// Cache for 24 hours
	l.cache.Set(ctx, key, data, 24*time.Hour)
}

// retryFailedEvents retries persisting failed events
func (l *Logger) retryFailedEvents() {
	ctx := context.Background()
	pattern := "audit:failed:*"
	
	iter := l.cache.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		
		data, err := l.cache.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var event Event
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}

		// Retry persistence
		l.persistEvent(&event)
		
		// Remove from cache if successful
		l.cache.Del(ctx, key)
	}
}

// updateMetrics updates audit metrics in cache
func (l *Logger) updateMetrics(event *Event) {
	ctx := context.Background()
	
	// Increment counters
	l.cache.Incr(ctx, fmt.Sprintf("audit:metrics:total"))
	l.cache.Incr(ctx, fmt.Sprintf("audit:metrics:type:%s", event.Type))
	
	if event.Result == "SUCCESS" {
		l.cache.Incr(ctx, fmt.Sprintf("audit:metrics:success"))
	} else {
		l.cache.Incr(ctx, fmt.Sprintf("audit:metrics:failure"))
	}

	// Track by user if available
	if event.UserID != nil {
		l.cache.Incr(ctx, fmt.Sprintf("audit:metrics:user:%s", event.UserID.String()))
	}
}

// GetMetrics returns current audit metrics
func (l *Logger) GetMetrics() (map[string]int64, error) {
	ctx := context.Background()
	metrics := make(map[string]int64)

	// Get all metric keys
	pattern := "audit:metrics:*"
	iter := l.cache.Scan(ctx, 0, pattern, 100).Iterator()
	
	for iter.Next(ctx) {
		key := iter.Val()
		val, err := l.cache.Get(ctx, key).Int64()
		if err == nil {
			// Remove prefix for cleaner key names
			cleanKey := key[len("audit:metrics:"):]
			metrics[cleanKey] = val
		}
	}

	return metrics, nil
}

// scheduleCleanup schedules periodic cleanup of old audit logs
func (l *Logger) scheduleCleanup() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		l.cleanup()
		l.retryFailedEvents()
	}
}

// cleanup removes old audit logs based on retention policy
func (l *Logger) cleanup() {
	if l.retentionDays <= 0 {
		return // No cleanup if retention is disabled
	}

	cutoff := time.Now().AddDate(0, 0, -l.retentionDays)
	
	result := l.db.Where("created_at < ?", cutoff).Delete(&models.OAuthAuditLog{})
	if result.Error != nil {
		l.logger.Error("Failed to cleanup old audit logs", zap.Error(result.Error))
	} else if result.RowsAffected > 0 {
		l.logger.Info("Cleaned up old audit logs", zap.Int64("count", result.RowsAffected))
	}
}

// QueryLogs queries audit logs with filters
func (l *Logger) QueryLogs(filters map[string]interface{}, limit, offset int) ([]models.OAuthAuditLog, int64, error) {
	var logs []models.OAuthAuditLog
	var total int64

	query := l.db.Model(&models.OAuthAuditLog{})

	// Apply filters
	if userID, ok := filters["user_id"].(string); ok {
		query = query.Where("user_id = ?", userID)
	}
	if clientID, ok := filters["client_id"].(string); ok {
		query = query.Where("client_id = ?", clientID)
	}
	if eventType, ok := filters["event_type"].(string); ok {
		query = query.Where("event_type = ?", eventType)
	}
	if success, ok := filters["success"].(bool); ok {
		query = query.Where("success = ?", success)
	}
	if startTime, ok := filters["start_time"].(time.Time); ok {
		query = query.Where("created_at >= ?", startTime)
	}
	if endTime, ok := filters["end_time"].(time.Time); ok {
		query = query.Where("created_at <= ?", endTime)
	}

	// Get total count
	query.Count(&total)

	// Get paginated results
	err := query.Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&logs).Error

	return logs, total, err
}

// DetectAnomalies checks for suspicious patterns in audit logs
func (l *Logger) DetectAnomalies() []string {
	var anomalies []string
	ctx := context.Background()

	// Check for excessive failed attempts
	failedAttempts, _ := l.cache.Get(ctx, "audit:metrics:failure").Int64()
	totalAttempts, _ := l.cache.Get(ctx, "audit:metrics:total").Int64()
	
	if totalAttempts > 0 && float64(failedAttempts)/float64(totalAttempts) > 0.3 {
		anomalies = append(anomalies, fmt.Sprintf("High failure rate: %.2f%%", 
			float64(failedAttempts)/float64(totalAttempts)*100))
	}

	// Check for rapid requests from same IP
	// This would require more sophisticated tracking

	return anomalies
}

// Helper methods

func (l *Logger) resultString(success bool) string {
	if success {
		return "SUCCESS"
	}
	return "FAILURE"
}

func (l *Logger) getClientIP(c *fiber.Ctx) string {
	if c == nil {
		return ""
	}
	
	// Check X-Forwarded-For header first
	if xff := c.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	
	// Check X-Real-IP header
	if xri := c.Get("X-Real-IP"); xri != "" {
		return xri
	}
	
	// Fall back to remote IP
	return c.IP()
}

func (l *Logger) getUserAgent(c *fiber.Ctx) string {
	if c == nil {
		return ""
	}
	return c.Get("User-Agent")
}