package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"go.uber.org/zap"
)

var (
	DB *gorm.DB
	healthCheckStop chan bool
	healthCheckWg sync.WaitGroup
)

func Initialize(databaseURL string) (*gorm.DB, error) {
	zapLogger, _ := zap.NewProduction()
	sugar := zapLogger.Sugar()

	config := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
		PrepareStmt:              true,
		DisableForeignKeyConstraintWhenMigrating: false,
	}

	db, err := gorm.Open(postgres.Open(databaseURL), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Connection pool settings with health-aware configuration
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Reduced from 1 hour to force rotation
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)  // Reduced from 10 minutes

	// Test connection
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Start health check goroutine
	healthCheckStop = make(chan bool)
	healthCheckWg.Add(1)
	go healthCheckLoop(db, sugar)

	sugar.Info("Database connection established with health checking")
	
	DB = db
	return db, nil
}

func healthCheckLoop(db *gorm.DB, sugar *zap.SugaredLogger) {
	defer healthCheckWg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-healthCheckStop:
			sugar.Info("Stopping database health check")
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := HealthCheckWithContext(ctx, db); err != nil {
				sugar.Warnf("Database health check failed: %v", err)
				// Connection pool will automatically handle reconnection
				// We just log the error for monitoring
			}
			cancel()
		}
	}
}

func HealthCheck(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

func HealthCheckWithContext(ctx context.Context, db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

func Close(db *gorm.DB) error {
	// Stop health check goroutine
	if healthCheckStop != nil {
		close(healthCheckStop)
		healthCheckWg.Wait()
	}
	
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}