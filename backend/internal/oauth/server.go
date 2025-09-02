package oauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"deltacat/backend/internal/auth"
	"deltacat/backend/pkg/models"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// Server implements OAuth 2.0 authorization server
type Server struct {
	db           *gorm.DB
	cache        *redis.Client
	logger       *zap.Logger
	supabaseAuth *auth.Service
	config       *Config
	keyManager   *KeyManager
}

// Config holds OAuth server configuration
type Config struct {
	Issuer               string
	AuthorizationEndpoint string
	TokenEndpoint        string
	UserInfoEndpoint     string
	JWKSEndpoint         string
	SigningAlgorithm     string
	AccessTokenTTL       time.Duration
	RefreshTokenTTL      time.Duration
	AuthCodeTTL          time.Duration
	RequirePKCE          bool
	RequireTLS           bool
}

// NewServer creates a new OAuth server instance
func NewServer(db *gorm.DB, cache *redis.Client, logger *zap.Logger, supabaseAuth *auth.Service, config *Config) (*Server, error) {
	keyManager, err := NewKeyManager(db, cache, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize key manager: %w", err)
	}

	return &Server{
		db:           db,
		cache:        cache,
		logger:       logger,
		supabaseAuth: supabaseAuth,
		config:       config,
		keyManager:   keyManager,
	}, nil
}

// Authorize handles OAuth authorization requests
func (s *Server) Authorize(c *fiber.Ctx) error {
	// Enforce TLS if required
	if s.config.RequireTLS && !c.Secure() {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "https_required",
			"error_description": "HTTPS is required for authorization requests",
		})
	}

	var req models.AuthorizationRequest
	if err := c.QueryParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_request",
			"error_description": "Invalid authorization request",
		})
	}

	// Validate client
	var client models.OAuthClient
	if err := s.db.Where("client_id = ?", req.ClientID).First(&client).Error; err != nil {
		s.logAuthEvent(models.OAuthEventAuthDenied, nil, req.ClientID, nil, c, false, "Client not found")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_client",
			"error_description": "Client not found",
		})
	}

	// Validate redirect URI
	if !s.isValidRedirectURI(req.RedirectURI, client.RedirectURIs) {
		s.logAuthEvent(models.OAuthEventAuthDenied, nil, req.ClientID, nil, c, false, "Invalid redirect URI")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_request",
			"error_description": "Invalid redirect URI",
		})
	}

	// Validate response type
	if req.ResponseType != models.ResponseTypeCode {
		return c.Redirect(fmt.Sprintf("%s?error=unsupported_response_type&state=%s",
			req.RedirectURI, req.State))
	}

	// Validate PKCE if required
	if s.config.RequirePKCE && req.CodeChallenge == "" {
		return c.Redirect(fmt.Sprintf("%s?error=invalid_request&error_description=PKCE+required&state=%s",
			req.RedirectURI, req.State))
	}

	// Get authenticated user
	user := auth.GetUser(c)
	if user == nil {
		// Redirect to login
		loginURL := fmt.Sprintf("/login?redirect_uri=%s", c.OriginalURL())
		return c.Redirect(loginURL)
	}

	// Generate authorization code
	code := s.generateAuthorizationCode()
	
	// Store authorization code
	authCode := models.OAuthAuthorizationCode{
		Code:                code,
		ClientID:            req.ClientID,
		UserID:              uuid.MustParse(user.ID),
		Scopes:              strings.Split(req.Scope, " "),
		RedirectURI:         req.RedirectURI,
		CodeChallenge:       req.CodeChallenge,
		CodeChallengeMethod: req.CodeChallengeMethod,
		Nonce:              req.Nonce,
		State:              req.State,
		ExpiresAt:          time.Now().Add(s.config.AuthCodeTTL),
		CreatedAt:          time.Now(),
	}

	if err := s.db.Create(&authCode).Error; err != nil {
		s.logger.Error("Failed to store authorization code", zap.Error(err))
		return c.Redirect(fmt.Sprintf("%s?error=server_error&state=%s", req.RedirectURI, req.State))
	}

	// Log successful authorization
	s.logAuthEvent(models.OAuthEventCodeIssued, &user.ID, req.ClientID, authCode.Scopes, c, true, "")

	// Redirect with authorization code
	redirectURL := fmt.Sprintf("%s?code=%s&state=%s", req.RedirectURI, code, req.State)
	return c.Redirect(redirectURL)
}

// Token handles OAuth token requests
func (s *Server) Token(c *fiber.Ctx) error {
	var req models.TokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_request",
			"error_description": "Invalid token request",
		})
	}

	// Route based on grant type
	switch req.GrantType {
	case models.GrantTypeAuthorizationCode:
		return s.handleAuthorizationCodeGrant(c, &req)
	case models.GrantTypeRefreshToken:
		return s.handleRefreshTokenGrant(c, &req)
	case models.GrantTypeClientCredentials:
		return s.handleClientCredentialsGrant(c, &req)
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "unsupported_grant_type",
			"error_description": "Grant type not supported",
		})
	}
}

// handleAuthorizationCodeGrant exchanges an authorization code for tokens
func (s *Server) handleAuthorizationCodeGrant(c *fiber.Ctx, req *models.TokenRequest) error {
	// Validate client credentials
	client, err := s.validateClient(req.ClientID, req.ClientSecret)
	if err != nil {
		s.logAuthEvent(models.OAuthEventTokenDenied, nil, req.ClientID, nil, c, false, "Invalid client credentials")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "invalid_client",
			"error_description": "Invalid client credentials",
		})
	}

	// Retrieve authorization code
	var authCode models.OAuthAuthorizationCode
	if err := s.db.Where("code = ?", req.Code).First(&authCode).Error; err != nil {
		s.logAuthEvent(models.OAuthEventTokenDenied, nil, req.ClientID, nil, c, false, "Invalid authorization code")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_grant",
			"error_description": "Invalid authorization code",
		})
	}

	// Check if code is expired
	if time.Now().After(authCode.ExpiresAt) {
		s.logAuthEvent(models.OAuthEventTokenDenied, &authCode.UserID, req.ClientID, nil, c, false, "Authorization code expired")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_grant",
			"error_description": "Authorization code expired",
		})
	}

	// Check if code was already used
	if authCode.UsedAt != nil {
		s.logAuthEvent(models.OAuthEventTokenDenied, &authCode.UserID, req.ClientID, nil, c, false, "Authorization code already used")
		// Revoke all tokens issued with this code (security measure)
		s.revokeTokensForAuthCode(authCode.Code)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_grant",
			"error_description": "Authorization code already used",
		})
	}

	// Validate redirect URI
	if authCode.RedirectURI != req.RedirectURI {
		s.logAuthEvent(models.OAuthEventTokenDenied, &authCode.UserID, req.ClientID, nil, c, false, "Redirect URI mismatch")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid_grant",
			"error_description": "Redirect URI mismatch",
		})
	}

	// Validate PKCE if present
	if authCode.CodeChallenge != "" {
		if !s.validatePKCE(authCode.CodeChallenge, authCode.CodeChallengeMethod, req.CodeVerifier) {
			s.logAuthEvent(models.OAuthEventTokenDenied, &authCode.UserID, req.ClientID, nil, c, false, "PKCE verification failed")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid_grant",
				"error_description": "PKCE verification failed",
			})
		}
	}

	// Mark code as used
	now := time.Now()
	authCode.UsedAt = &now
	s.db.Save(&authCode)

	// Generate tokens
	accessToken, refreshToken, expiresIn, err := s.generateTokens(authCode.UserID.String(), client.ClientID, authCode.Scopes)
	if err != nil {
		s.logger.Error("Failed to generate tokens", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "server_error",
			"error_description": "Failed to generate tokens",
		})
	}

	// Log successful token issuance
	s.logAuthEvent(models.OAuthEventTokenIssued, &authCode.UserID, client.ClientID, authCode.Scopes, c, true, "")

	// Return token response
	return c.JSON(models.TokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		ExpiresIn:    expiresIn,
		RefreshToken: refreshToken,
		Scope:        strings.Join(authCode.Scopes, " "),
	})
}

// handleRefreshTokenGrant handles refresh token grant
func (s *Server) handleRefreshTokenGrant(c *fiber.Ctx, req *models.TokenRequest) error {
	// Implementation continues in next part...
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "not_implemented",
	})
}

// handleClientCredentialsGrant handles client credentials grant
func (s *Server) handleClientCredentialsGrant(c *fiber.Ctx, req *models.TokenRequest) error {
	// Implementation for service-to-service auth
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "not_implemented",
	})
}

// Helper methods

func (s *Server) generateAuthorizationCode() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(b)
}

func (s *Server) generateTokens(userID, clientID string, scopes []string) (accessToken, refreshToken string, expiresIn int, err error) {
	// Generate access token
	jti := uuid.New().String()
	claims := jwt.MapClaims{
		"iss":       s.config.Issuer,
		"sub":       userID,
		"aud":       clientID,
		"exp":       time.Now().Add(s.config.AccessTokenTTL).Unix(),
		"iat":       time.Now().Unix(),
		"jti":       jti,
		"scope":     strings.Join(scopes, " "),
		"client_id": clientID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	key, err := s.keyManager.GetActiveSigningKey()
	if err != nil {
		return "", "", 0, err
	}

	accessToken, err = token.SignedString([]byte(key.PrivateKeyEncrypted)) // In production, decrypt this
	if err != nil {
		return "", "", 0, err
	}

	// Store access token for revocation tracking
	s.db.Create(&models.OAuthAccessToken{
		JTI:       jti,
		UserID:    uuid.MustParse(userID),
		ClientID:  clientID,
		Scopes:    scopes,
		ExpiresAt: time.Now().Add(s.config.AccessTokenTTL),
		CreatedAt: time.Now(),
	})

	// Generate refresh token
	refreshTokenBytes := make([]byte, 32)
	rand.Read(refreshTokenBytes)
	refreshToken = base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(refreshTokenBytes)

	// Store refresh token
	hash := sha256.Sum256([]byte(refreshToken))
	s.db.Create(&models.OAuthRefreshToken{
		TokenHash: hex.EncodeToString(hash[:]),
		UserID:    uuid.MustParse(userID),
		ClientID:  clientID,
		Scopes:    scopes,
		ExpiresAt: time.Now().Add(s.config.RefreshTokenTTL),
		CreatedAt: time.Now(),
	})

	expiresIn = int(s.config.AccessTokenTTL.Seconds())
	return accessToken, refreshToken, expiresIn, nil
}

func (s *Server) validateClient(clientID, clientSecret string) (*models.OAuthClient, error) {
	var client models.OAuthClient
	if err := s.db.Where("client_id = ?", clientID).First(&client).Error; err != nil {
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(client.ClientSecretHash), []byte(clientSecret)); err != nil {
		return nil, fmt.Errorf("invalid client secret")
	}

	return &client, nil
}

func (s *Server) isValidRedirectURI(uri string, allowedURIs []string) bool {
	for _, allowed := range allowedURIs {
		if uri == allowed {
			return true
		}
	}
	return false
}

func (s *Server) validatePKCE(challenge, method, verifier string) bool {
	if method == "" || method == "plain" {
		return challenge == verifier
	}

	if method == "S256" {
		hash := sha256.Sum256([]byte(verifier))
		computedChallenge := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hash[:])
		return challenge == computedChallenge
	}

	return false
}

func (s *Server) revokeTokensForAuthCode(code string) {
	// Implementation to revoke all tokens issued with this authorization code
	// This is a security measure when a code is reused
}

func (s *Server) logAuthEvent(eventType string, userID *string, clientID string, scopes []string, c *fiber.Ctx, success bool, errorMsg string) {
	var uid *uuid.UUID
	if userID != nil {
		u := uuid.MustParse(*userID)
		uid = &u
	}

	event := models.OAuthAuditLog{
		EventType:    eventType,
		UserID:       uid,
		ClientID:     clientID,
		Scopes:       scopes,
		IPAddress:    c.IP(),
		UserAgent:    c.Get("User-Agent"),
		Success:      success,
		ErrorMessage: errorMsg,
		CreatedAt:    time.Now(),
	}

	if err := s.db.Create(&event).Error; err != nil {
		s.logger.Error("Failed to log OAuth event", zap.Error(err))
	}
}