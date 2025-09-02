package models

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

// OAuthClient represents an OAuth 2.0 client application
type OAuthClient struct {
	ID               uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	ClientID         string         `gorm:"uniqueIndex;not null" json:"client_id"`
	ClientSecretHash string         `gorm:"not null" json:"-"` // Never expose the hash
	ClientName       string         `json:"client_name"`
	RedirectURIs     pq.StringArray `gorm:"type:text[]" json:"redirect_uris"`
	GrantTypes       pq.StringArray `gorm:"type:text[]" json:"grant_types"`
	Scopes           pq.StringArray `gorm:"type:text[]" json:"scopes"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	DeletedAt        gorm.DeletedAt `gorm:"index" json:"-"`
}

// OAuthAuthorizationCode represents a temporary authorization code
type OAuthAuthorizationCode struct {
	Code                 string         `gorm:"primaryKey" json:"code"`
	ClientID             string         `gorm:"not null" json:"client_id"`
	UserID               uuid.UUID      `gorm:"type:uuid;not null" json:"user_id"`
	Scopes               pq.StringArray `gorm:"type:text[]" json:"scopes"`
	RedirectURI          string         `json:"redirect_uri"`
	CodeChallenge        string         `json:"code_challenge,omitempty"`
	CodeChallengeMethod  string         `json:"code_challenge_method,omitempty"`
	Nonce                string         `json:"nonce,omitempty"`
	State                string         `json:"state,omitempty"`
	ExpiresAt            time.Time      `gorm:"not null" json:"expires_at"`
	UsedAt               *time.Time     `json:"used_at,omitempty"`
	CreatedAt            time.Time      `json:"created_at"`
}

// OAuthAccessToken tracks issued access tokens for revocation
type OAuthAccessToken struct {
	JTI        string         `gorm:"primaryKey" json:"jti"` // JWT ID
	UserID     uuid.UUID      `gorm:"type:uuid;not null" json:"user_id"`
	ClientID   string         `gorm:"not null" json:"client_id"`
	Scopes     pq.StringArray `gorm:"type:text[]" json:"scopes"`
	ExpiresAt  time.Time      `gorm:"not null" json:"expires_at"`
	RevokedAt  *time.Time     `json:"revoked_at,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

// OAuthRefreshToken represents a refresh token
type OAuthRefreshToken struct {
	TokenHash  string         `gorm:"primaryKey" json:"-"` // SHA256 hash of token
	UserID     uuid.UUID      `gorm:"type:uuid;not null" json:"user_id"`
	ClientID   string         `gorm:"not null" json:"client_id"`
	Scopes     pq.StringArray `gorm:"type:text[]" json:"scopes"`
	ExpiresAt  time.Time      `gorm:"not null" json:"expires_at"`
	UsedAt     *time.Time     `json:"used_at,omitempty"`
	RevokedAt  *time.Time     `json:"revoked_at,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

// OAuthSigningKey represents a signing key for JWT tokens
type OAuthSigningKey struct {
	KID                  string         `gorm:"primaryKey" json:"kid"` // Key ID
	Algorithm            string         `gorm:"not null" json:"alg"`
	PublicKey            string         `gorm:"not null" json:"-"`
	PrivateKeyEncrypted  string         `gorm:"not null" json:"-"`
	CreatedAt            time.Time      `json:"created_at"`
	RotatedAt            *time.Time     `json:"rotated_at,omitempty"`
	ExpiresAt            time.Time      `gorm:"not null" json:"expires_at"`
	Active               bool           `gorm:"default:true" json:"active"`
}

// OAuthAuditLog represents an OAuth-related audit event
type OAuthAuditLog struct {
	ID           uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	EventType    string         `gorm:"not null" json:"event_type"`
	UserID       *uuid.UUID     `gorm:"type:uuid" json:"user_id,omitempty"`
	ClientID     string         `json:"client_id,omitempty"`
	Scopes       pq.StringArray `gorm:"type:text[]" json:"scopes,omitempty"`
	IPAddress    string         `json:"ip_address,omitempty"`
	UserAgent    string         `json:"user_agent,omitempty"`
	Success      bool           `json:"success"`
	ErrorMessage string         `json:"error_message,omitempty"`
	Metadata     JSONB          `gorm:"type:jsonb" json:"metadata,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}

// TokenResponse represents the OAuth token endpoint response
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
}

// AuthorizationRequest represents an OAuth authorization request
type AuthorizationRequest struct {
	ResponseType        string `json:"response_type"`
	ClientID            string `json:"client_id"`
	RedirectURI         string `json:"redirect_uri"`
	Scope               string `json:"scope"`
	State               string `json:"state"`
	CodeChallenge       string `json:"code_challenge,omitempty"`
	CodeChallengeMethod string `json:"code_challenge_method,omitempty"`
	Nonce               string `json:"nonce,omitempty"`
}

// TokenRequest represents an OAuth token request
type TokenRequest struct {
	GrantType    string `json:"grant_type"`
	Code         string `json:"code,omitempty"`
	RedirectURI  string `json:"redirect_uri,omitempty"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret,omitempty"`
	CodeVerifier string `json:"code_verifier,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

// JSONB is a custom type for JSONB fields
type JSONB map[string]interface{}

// Value implements driver.Valuer interface
func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan implements sql.Scanner interface
func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = make(JSONB)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, j)
}

// BeforeCreate hooks
func (c *OAuthClient) BeforeCreate(tx *gorm.DB) error {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	return nil
}

func (l *OAuthAuditLog) BeforeCreate(tx *gorm.DB) error {
	if l.ID == uuid.Nil {
		l.ID = uuid.New()
	}
	if l.Metadata == nil {
		l.Metadata = make(JSONB)
	}
	return nil
}

// OAuth Event Types for audit logging
const (
	OAuthEventAuthRequest      = "AUTH_REQUEST"
	OAuthEventAuthApproved     = "AUTH_APPROVED"
	OAuthEventAuthDenied       = "AUTH_DENIED"
	OAuthEventCodeIssued       = "CODE_ISSUED"
	OAuthEventCodeExchanged    = "CODE_EXCHANGED"
	OAuthEventTokenIssued      = "TOKEN_ISSUED"
	OAuthEventTokenRefreshed   = "TOKEN_REFRESHED"
	OAuthEventTokenRevoked     = "TOKEN_REVOKED"
	OAuthEventTokenIntrospect  = "TOKEN_INTROSPECT"
	OAuthEventClientCreated    = "CLIENT_CREATED"
	OAuthEventClientUpdated    = "CLIENT_UPDATED"
	OAuthEventClientDeleted    = "CLIENT_DELETED"
	OAuthEventKeyRotated       = "KEY_ROTATED"
)

// Grant Types
const (
	GrantTypeAuthorizationCode = "authorization_code"
	GrantTypeRefreshToken      = "refresh_token"
	GrantTypeClientCredentials = "client_credentials"
)

// Response Types
const (
	ResponseTypeCode  = "code"
	ResponseTypeToken = "token"
)