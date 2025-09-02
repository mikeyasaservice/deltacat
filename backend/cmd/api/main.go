package main

import (
	"context"
	"deltacat/backend/internal/auth"
	"deltacat/backend/internal/catalog"
	"deltacat/backend/internal/compute"
	"deltacat/backend/internal/storage"
	"deltacat/backend/pkg/config"
	"deltacat/backend/pkg/database"
	"deltacat/backend/pkg/middleware"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/gofiber/contrib/otelfiber"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// Load configuration
	cfg := config.Load()
	
	// Initialize database
	db, err := database.Initialize(cfg.DatabaseURL)
	if err != nil {
		sugar.Fatalf("Failed to initialize database: %v", err)
	}

	// Initialize Dragonfly cache
	cache, err := database.InitializeCache(cfg.CacheURL)
	if err != nil {
		sugar.Fatalf("Failed to initialize cache: %v", err)
	}

	// Create Fiber app with custom config
	app := fiber.New(fiber.Config{
		AppName:      "DeltaCAT API",
		ServerHeader: "DeltaCAT",
		ErrorHandler: middleware.CustomErrorHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	})

	// Global middleware
	app.Use(recover.New())
	app.Use(helmet.New())
	app.Use(requestid.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.AllowedOrigins,
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
		AllowMethods: "GET, POST, PUT, DELETE, PATCH, OPTIONS",
	}))

	// OpenTelemetry middleware
	if cfg.OTLPEndpoint != "" {
		app.Use(otelfiber.Middleware())
	}

	// Logging middleware with Zap
	app.Use(middleware.ZapLogger(logger))

	// Health check endpoints
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "healthy",
			"version": cfg.Version,
			"time":    time.Now().UTC(),
		})
	})

	app.Get("/ready", func(c *fiber.Ctx) error {
		// Check database connection
		if err := database.HealthCheck(db); err != nil {
			return c.Status(503).JSON(fiber.Map{
				"status": "not ready",
				"error":  err.Error(),
			})
		}
		
		// Check cache connection
		if err := cache.Ping(context.Background()).Err(); err != nil {
			return c.Status(503).JSON(fiber.Map{
				"status": "not ready",
				"error":  err.Error(),
			})
		}

		return c.JSON(fiber.Map{"status": "ready"})
	})

	// API routes
	api := app.Group("/api/v1")

	// Initialize auth service with Supabase
	authService := auth.NewService(db, cache, logger, 
		cfg.SupabaseURL, 
		cfg.SupabaseAnonKey,
		cfg.SupabaseJWTSecret,
	)

	// Public routes
	auth.RegisterPublicRoutes(api, db, cache, logger)

	// Protected routes (require Supabase JWT)
	api.Use(authService.SupabaseAuthMiddleware())
	
	// Register service routes
	auth.RegisterProtectedRoutes(api, db, cache, logger)
	catalog.RegisterRoutes(api, db, cache, logger)
	compute.RegisterRoutes(api, db, cache, logger)
	storage.RegisterRoutes(api, db, cache, logger)

	// Metrics endpoint (Prometheus)
	app.Get("/metrics", middleware.PrometheusHandler())

	// Start server
	go func() {
		port := cfg.Port
		if port == "" {
			port = "8080"
		}
		sugar.Infof("Starting DeltaCAT API on port %s", port)
		if err := app.Listen(":" + port); err != nil {
			sugar.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	sugar.Info("Shutting down server...")
	if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
		sugar.Errorf("Server forced to shutdown: %v", err)
	}

	sugar.Info("Server shutdown complete")
}