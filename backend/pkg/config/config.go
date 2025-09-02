package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type Config struct {
	// Server
	Port           string `mapstructure:"PORT"`
	Environment    string `mapstructure:"ENV"`
	Version        string `mapstructure:"VERSION"`
	AllowedOrigins string `mapstructure:"ALLOWED_ORIGINS"`

	// Database
	DatabaseURL string `mapstructure:"DATABASE_URL"`
	
	// Cache (Dragonfly)
	CacheURL string `mapstructure:"CACHE_URL"`
	
	// Supabase Auth
	SupabaseURL       string `mapstructure:"SUPABASE_URL"`
	SupabaseAnonKey   string `mapstructure:"SUPABASE_ANON_KEY"`
	SupabaseServiceKey string `mapstructure:"SUPABASE_SERVICE_KEY"`
	SupabaseJWTSecret string `mapstructure:"SUPABASE_JWT_SECRET"`
	
	// Storage
	S3Endpoint        string `mapstructure:"S3_ENDPOINT"`
	S3AccessKey       string `mapstructure:"S3_ACCESS_KEY"`
	S3SecretKey       string `mapstructure:"S3_SECRET_KEY"`
	S3Bucket          string `mapstructure:"S3_BUCKET"`
	S3Region          string `mapstructure:"S3_REGION"`
	UseSSL            bool   `mapstructure:"S3_USE_SSL"`
	
	// Temporal
	TemporalHost      string `mapstructure:"TEMPORAL_HOST"`
	TemporalNamespace string `mapstructure:"TEMPORAL_NAMESPACE"`
	
	// Python DeltaCAT
	PythonAPIURL      string `mapstructure:"PYTHON_API_URL"`
	
	// Observability
	OTLPEndpoint      string `mapstructure:"OTLP_ENDPOINT"`
	LogLevel          string `mapstructure:"LOG_LEVEL"`
}

func Load() *Config {
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()

	viper.SetConfigFile(".env")
	viper.AutomaticEnv()
	
	// Default values
	viper.SetDefault("PORT", "8080")
	viper.SetDefault("ENV", "development")
	viper.SetDefault("VERSION", "2.0.0-rc1")
	viper.SetDefault("ALLOWED_ORIGINS", "*")
	viper.SetDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/deltacat?sslmode=disable")
	viper.SetDefault("CACHE_URL", "localhost:6379")
	viper.SetDefault("SUPABASE_URL", "http://localhost:54321")
	viper.SetDefault("SUPABASE_ANON_KEY", "")
	viper.SetDefault("SUPABASE_SERVICE_KEY", "")
	viper.SetDefault("S3_ENDPOINT", "localhost:9000")
	viper.SetDefault("S3_REGION", "us-east-1")
	viper.SetDefault("S3_BUCKET", "deltacat")
	viper.SetDefault("S3_USE_SSL", false)
	viper.SetDefault("TEMPORAL_HOST", "localhost:7233")
	viper.SetDefault("TEMPORAL_NAMESPACE", "default")
	viper.SetDefault("PYTHON_API_URL", "http://localhost:8000")
	viper.SetDefault("LOG_LEVEL", "info")
	
	// Try to read .env file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			sugar.Warnf("Error reading config file: %v", err)
		}
	}
	
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		sugar.Fatalf("Unable to decode config: %v", err)
	}
	
	// Validate required fields
	if config.SupabaseJWTSecret == "" && config.SupabaseServiceKey == "" {
		sugar.Warn("SUPABASE_JWT_SECRET not set, Supabase auth validation will fail!")
	}
	
	return &config
}

func generateRandomSecret() string {
	// This is just for development
	return "deltacat-dev-secret-change-in-production"
}

func (c *Config) IsDevelopment() bool {
	return strings.ToLower(c.Environment) == "development"
}

func (c *Config) IsProduction() bool {
	return strings.ToLower(c.Environment) == "production"
}