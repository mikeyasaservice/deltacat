package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Service struct {
	db            *gorm.DB
	cache         *redis.Client
	logger        *zap.Logger
	supabaseURL   string
	supabaseKey   string
	jwtSecret     string
}

// SupabaseUser represents the user from Supabase JWT
type SupabaseUser struct {
	ID            string                 `json:"sub"`
	Email         string                 `json:"email"`
	Phone         string                 `json:"phone"`
	Role          string                 `json:"role"`
	UserMetadata  map[string]interface{} `json:"user_metadata"`
	AppMetadata   map[string]interface{} `json:"app_metadata"`
}

// NewService creates a new auth service that validates Supabase JWTs
func NewService(db *gorm.DB, cache *redis.Client, logger *zap.Logger, supabaseURL, supabaseKey, jwtSecret string) *Service {
	return &Service{
		db:          db,
		cache:       cache,
		logger:      logger,
		supabaseURL: supabaseURL,
		supabaseKey: supabaseKey,
		jwtSecret:   jwtSecret,
	}
}

// ValidateSupabaseToken validates a Supabase JWT token
func (s *Service) ValidateSupabaseToken(tokenString string) (*SupabaseUser, error) {
	// Parse the JWT with Supabase's secret
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(s.jwtSecret), nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	// Extract claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims")
	}

	// Build user from claims
	user := &SupabaseUser{
		ID:    claims["sub"].(string),
		Email: "",
		Role:  "authenticated",
	}

	if email, ok := claims["email"].(string); ok {
		user.Email = email
	}
	if phone, ok := claims["phone"].(string); ok {
		user.Phone = phone
	}
	if role, ok := claims["role"].(string); ok {
		user.Role = role
	}
	if userMeta, ok := claims["user_metadata"].(map[string]interface{}); ok {
		user.UserMetadata = userMeta
	}
	if appMeta, ok := claims["app_metadata"].(map[string]interface{}); ok {
		user.AppMetadata = appMeta
	}

	return user, nil
}

// SupabaseAuthMiddleware validates Supabase JWT tokens
func (s *Service) SupabaseAuthMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Get token from Authorization header
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Missing authorization header",
			})
		}

		// Extract Bearer token
		var tokenString string
		if _, err := fmt.Sscanf(authHeader, "Bearer %s", &tokenString); err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid authorization format",
			})
		}

		// Check cache for validated token
		cacheKey := fmt.Sprintf("auth:token:%s", tokenString[:16]) // Use first 16 chars as cache key
		cachedUser, err := s.cache.Get(context.Background(), cacheKey).Result()
		
		var user *SupabaseUser
		if err == nil && cachedUser != "" {
			// Found in cache
			if err := json.Unmarshal([]byte(cachedUser), &user); err == nil {
				// Set user in context
				c.Locals("user", user)
				c.Locals("user_id", user.ID)
				c.Locals("email", user.Email)
				c.Locals("role", user.Role)
				return c.Next()
			}
		}

		// Validate token
		user, err = s.ValidateSupabaseToken(tokenString)
		if err != nil {
			s.logger.Error("Token validation failed", zap.Error(err))
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Invalid token",
			})
		}

		// Cache the validated user for 5 minutes
		userData, _ := json.Marshal(user)
		s.cache.Set(context.Background(), cacheKey, userData, 5*time.Minute)

		// Set user in context
		c.Locals("user", user)
		c.Locals("user_id", user.ID)
		c.Locals("email", user.Email)
		c.Locals("role", user.Role)

		return c.Next()
	}
}

// RequireRole middleware checks if user has required role
func (s *Service) RequireRole(roles ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userRole, ok := c.Locals("role").(string)
		if !ok {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "No role found",
			})
		}

		// Check if user has required role
		for _, role := range roles {
			if userRole == role {
				return c.Next()
			}
		}

		// Check app_metadata for custom roles
		if user, ok := c.Locals("user").(*SupabaseUser); ok {
			if customRoles, exists := user.AppMetadata["roles"].([]interface{}); exists {
				for _, customRole := range customRoles {
					for _, requiredRole := range roles {
						if customRole.(string) == requiredRole {
							return c.Next()
						}
					}
				}
			}
		}

		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Insufficient permissions",
		})
	}
}

// GetUser gets the current user from context
func GetUser(c *fiber.Ctx) *SupabaseUser {
	if user, ok := c.Locals("user").(*SupabaseUser); ok {
		return user
	}
	return nil
}

// RegisterPublicRoutes registers public auth endpoints (health checks, etc)
func RegisterPublicRoutes(api fiber.Router, db *gorm.DB, cache *redis.Client, logger *zap.Logger) {
	// These routes don't require authentication
	api.Get("/auth/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "healthy",
			"service": "auth",
		})
	})
}

// RegisterProtectedRoutes registers protected auth endpoints
func RegisterProtectedRoutes(api fiber.Router, db *gorm.DB, cache *redis.Client, logger *zap.Logger) {
	auth := api.Group("/auth")
	
	// Get current user info
	auth.Get("/me", func(c *fiber.Ctx) error {
		user := GetUser(c)
		if user == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "User not found",
			})
		}
		return c.JSON(user)
	})

	// Invalidate cached tokens
	auth.Post("/logout", func(c *fiber.Ctx) error {
		// Clear cache for this user
		userID := c.Locals("user_id").(string)
		pattern := fmt.Sprintf("auth:*:%s:*", userID)
		
		ctx := context.Background()
		iter := cache.Scan(ctx, 0, pattern, 0).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			cache.Del(ctx, keys...)
		}

		return c.JSON(fiber.Map{
			"message": "Logged out successfully",
		})
	})
}