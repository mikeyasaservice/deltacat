package oauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"deltacat/backend/pkg/models"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// KeyManager handles signing key management and rotation
type KeyManager struct {
	db     *gorm.DB
	cache  *redis.Client
	logger *zap.Logger
}

// JWK represents a JSON Web Key
type JWK struct {
	KTY string `json:"kty"`
	KID string `json:"kid"`
	ALG string `json:"alg"`
	USE string `json:"use"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
}

// JWKS represents a JSON Web Key Set
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// NewKeyManager creates a new key manager
func NewKeyManager(db *gorm.DB, cache *redis.Client, logger *zap.Logger) (*KeyManager, error) {
	km := &KeyManager{
		db:     db,
		cache:  cache,
		logger: logger,
	}

	// Ensure at least one active key exists
	if err := km.ensureActiveKey(); err != nil {
		return nil, err
	}

	return km, nil
}

// GetActiveSigningKey returns the current active signing key
func (km *KeyManager) GetActiveSigningKey() (*models.OAuthSigningKey, error) {
	// Check cache first
	ctx := context.Background()
	cacheKey := "oauth:signing_key:active"
	
	cached, err := km.cache.Get(ctx, cacheKey).Result()
	if err == nil && cached != "" {
		var key models.OAuthSigningKey
		if err := json.Unmarshal([]byte(cached), &key); err == nil {
			return &key, nil
		}
	}

	// Get from database
	var key models.OAuthSigningKey
	err = km.db.Where("active = ? AND expires_at > ?", true, time.Now()).
		Order("created_at DESC").
		First(&key).Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// Generate new key if none exists
			return km.generateNewKey()
		}
		return nil, err
	}

	// Cache the key
	keyData, _ := json.Marshal(key)
	km.cache.Set(ctx, cacheKey, keyData, 5*time.Minute)

	return &key, nil
}

// GetJWKS returns the JSON Web Key Set for public key validation
func (km *KeyManager) GetJWKS() (*JWKS, error) {
	// Get all active and recently rotated keys
	var keys []models.OAuthSigningKey
	cutoff := time.Now().Add(-24 * time.Hour) // Include keys rotated in last 24h
	
	err := km.db.Where("(active = ? OR rotated_at > ?) AND expires_at > ?", 
		true, cutoff, time.Now()).Find(&keys).Error
	if err != nil {
		return nil, err
	}

	jwks := &JWKS{Keys: make([]JWK, 0, len(keys))}
	
	for _, key := range keys {
		// Parse public key and convert to JWK
		jwk, err := km.publicKeyToJWK(key.KID, key.Algorithm, key.PublicKey)
		if err != nil {
			km.logger.Error("Failed to convert public key to JWK", 
				zap.String("kid", key.KID), zap.Error(err))
			continue
		}
		jwks.Keys = append(jwks.Keys, *jwk)
	}

	return jwks, nil
}

// RotateKeys performs key rotation
func (km *KeyManager) RotateKeys() error {
	km.logger.Info("Starting key rotation")

	// Get current active key
	var currentKey models.OAuthSigningKey
	err := km.db.Where("active = ?", true).First(&currentKey).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("failed to get current key: %w", err)
	}

	// Generate new key
	newKey, err := km.generateNewKey()
	if err != nil {
		return fmt.Errorf("failed to generate new key: %w", err)
	}

	// Begin transaction
	tx := km.db.Begin()

	// Deactivate old key if exists
	if currentKey.KID != "" {
		now := time.Now()
		currentKey.Active = false
		currentKey.RotatedAt = &now
		if err := tx.Save(&currentKey).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to deactivate old key: %w", err)
		}
	}

	// Clear cache
	ctx := context.Background()
	km.cache.Del(ctx, "oauth:signing_key:active")
	km.cache.Del(ctx, "oauth:jwks")

	tx.Commit()

	// Log rotation event
	km.logKeyRotation(currentKey.KID, newKey.KID)

	km.logger.Info("Key rotation completed", 
		zap.String("old_kid", currentKey.KID),
		zap.String("new_kid", newKey.KID))

	return nil
}

// ensureActiveKey ensures at least one active signing key exists
func (km *KeyManager) ensureActiveKey() error {
	var count int64
	km.db.Model(&models.OAuthSigningKey{}).Where("active = ? AND expires_at > ?", true, time.Now()).Count(&count)
	
	if count == 0 {
		km.logger.Info("No active signing key found, generating new key")
		_, err := km.generateNewKey()
		return err
	}

	return nil
}

// generateNewKey generates a new signing key pair
func (km *KeyManager) generateNewKey() (*models.OAuthSigningKey, error) {
	// Generate RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate RSA key: %w", err)
	}

	// Convert private key to PEM
	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}
	privateKeyStr := string(pem.EncodeToMemory(privateKeyPEM))

	// Convert public key to PEM
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}
	publicKeyPEM := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}
	publicKeyStr := string(pem.EncodeToMemory(publicKeyPEM))

	// Create signing key record
	key := &models.OAuthSigningKey{
		KID:                 uuid.New().String(),
		Algorithm:           "RS256",
		PublicKey:          publicKeyStr,
		PrivateKeyEncrypted: km.encryptPrivateKey(privateKeyStr), // Implement encryption
		Active:             true,
		CreatedAt:          time.Now(),
		ExpiresAt:          time.Now().Add(90 * 24 * time.Hour), // 90 days
	}

	if err := km.db.Create(key).Error; err != nil {
		return nil, fmt.Errorf("failed to save signing key: %w", err)
	}

	return key, nil
}

// encryptPrivateKey encrypts a private key for storage
func (km *KeyManager) encryptPrivateKey(privateKey string) string {
	// TODO: Implement proper encryption using KMS or similar
	// For now, return base64 encoded (NOT SECURE - just for development)
	return base64.StdEncoding.EncodeToString([]byte(privateKey))
}

// decryptPrivateKey decrypts a stored private key
func (km *KeyManager) decryptPrivateKey(encryptedKey string) (string, error) {
	// TODO: Implement proper decryption using KMS or similar
	// For now, decode base64 (NOT SECURE - just for development)
	decoded, err := base64.StdEncoding.DecodeString(encryptedKey)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

// publicKeyToJWK converts a public key to JWK format
func (km *KeyManager) publicKeyToJWK(kid, alg, publicKeyPEM string) (*JWK, error) {
	// Parse PEM encoded public key
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA public key")
	}

	// Convert to JWK
	jwk := &JWK{
		KTY: "RSA",
		KID: kid,
		ALG: alg,
		USE: "sig",
		N:   base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(rsaPub.N.Bytes()),
		E:   base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(big.NewInt(int64(rsaPub.E)).Bytes()),
	}

	return jwk, nil
}

// logKeyRotation logs a key rotation event
func (km *KeyManager) logKeyRotation(oldKID, newKID string) {
	event := models.OAuthAuditLog{
		EventType: models.OAuthEventKeyRotated,
		Success:   true,
		Metadata: models.JSONB{
			"old_kid": oldKID,
			"new_kid": newKID,
		},
		CreatedAt: time.Now(),
	}

	if err := km.db.Create(&event).Error; err != nil {
		km.logger.Error("Failed to log key rotation event", zap.Error(err))
	}
}

// ScheduleRotation schedules automatic key rotation
func (km *KeyManager) ScheduleRotation(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			if err := km.RotateKeys(); err != nil {
				km.logger.Error("Scheduled key rotation failed", zap.Error(err))
			}
		}
	}()
}