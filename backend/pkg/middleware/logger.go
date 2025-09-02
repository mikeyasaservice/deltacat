package middleware

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// ZapLogger creates a Fiber middleware for Zap logging
func ZapLogger(logger *zap.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		
		// Continue processing
		err := c.Next()
		
		// Log the request
		fields := []zap.Field{
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.String("ip", c.IP()),
			zap.Duration("latency", time.Since(start)),
			zap.String("request_id", c.Locals("requestid").(string)),
		}
		
		// Add user ID if authenticated
		if userID := c.Locals("user_id"); userID != nil {
			fields = append(fields, zap.String("user_id", userID.(string)))
		}
		
		// Add error if exists
		if err != nil {
			fields = append(fields, zap.Error(err))
			logger.Error("Request failed", fields...)
		} else if c.Response().StatusCode() >= 400 {
			logger.Warn("Request error", fields...)
		} else {
			logger.Info("Request completed", fields...)
		}
		
		return err
	}
}