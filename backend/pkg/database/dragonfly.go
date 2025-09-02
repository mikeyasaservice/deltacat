package database

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var Cache *redis.Client

func InitializeCache(cacheURL string) (*redis.Client, error) {
	zapLogger, _ := zap.NewProduction()
	sugar := zapLogger.Sugar()

	// Parse Redis/Dragonfly URL
	opt, err := redis.ParseURL(fmt.Sprintf("redis://%s/0", cacheURL))
	if err != nil {
		// Fallback to simple connection
		opt = &redis.Options{
			Addr:         cacheURL,
			Password:     "",
			DB:           0,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     10,
			MinIdleConns: 5,
			MaxRetries:   3,
		}
	}

	client := redis.NewClient(opt)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to cache: %w", err)
	}

	sugar.Info("Cache connection established (Dragonfly/Redis)")
	
	Cache = client
	return client, nil
}

// CacheKey generates a consistent cache key
func CacheKey(prefix string, parts ...string) string {
	key := "deltacat:" + prefix
	for _, part := range parts {
		key += ":" + part
	}
	return key
}

// SetWithTTL sets a value with TTL
func SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return Cache.Set(ctx, key, value, ttl).Err()
}

// GetString gets a string value from cache
func GetString(ctx context.Context, key string) (string, error) {
	return Cache.Get(ctx, key).Result()
}

// Delete removes a key from cache
func Delete(ctx context.Context, keys ...string) error {
	return Cache.Del(ctx, keys...).Err()
}

// InvalidatePattern invalidates all keys matching a pattern
func InvalidatePattern(ctx context.Context, pattern string) error {
	const batchSize = 1000 // Process keys in batches to avoid memory spikes
	
	iter := Cache.Scan(ctx, 0, pattern, 0).Iterator()
	var batch []string
	deleted := 0
	
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		
		// Delete when batch is full
		if len(batch) >= batchSize {
			if err := Cache.Del(ctx, batch...).Err(); err != nil {
				return fmt.Errorf("failed to delete batch of %d keys: %w", len(batch), err)
			}
			deleted += len(batch)
			batch = batch[:0] // Reset batch slice
		}
	}
	
	if err := iter.Err(); err != nil {
		return fmt.Errorf("error scanning keys: %w", err)
	}
	
	// Delete remaining keys in final batch
	if len(batch) > 0 {
		if err := Cache.Del(ctx, batch...).Err(); err != nil {
			return fmt.Errorf("failed to delete final batch of %d keys: %w", len(batch), err)
		}
		deleted += len(batch)
	}
	
	if deleted > 0 {
		zapLogger, _ := zap.NewProduction()
		sugar := zapLogger.Sugar()
		sugar.Debugf("Invalidated %d keys matching pattern: %s", deleted, pattern)
	}
	
	return nil
}