package core

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Builder handles building and storing caches
type Builder struct {
	registry *Registry
	pgConn   *pgx.Conn
	redisClient *redis.Client
}

// NewBuilder creates a new cache builder
func NewBuilder(registry *Registry, pgConn *pgx.Conn, redisClient *redis.Client) *Builder {
	return &Builder{
		registry:    registry,
		pgConn:      pgConn,
		redisClient: redisClient,
	}
}

// BuildAndStore builds a cache by name and stores it in both Postgres and Redis
func (b *Builder) BuildAndStore(ctx context.Context, cacheName string, redisExpiration time.Duration) error {
	entry, ok := b.registry.Get(cacheName)
	if !ok {
		return fmt.Errorf("cache entry not found: %s", cacheName)
	}

	// Build cache from SQL
	data, err := entry.BuildCache(ctx, b.pgConn)
	if err != nil {
		return fmt.Errorf("failed to build cache: %w", err)
	}

	cache := NewCache(cacheName, data)

	// Save to Postgres
	if err := cache.SaveToPostgres(ctx, b.pgConn); err != nil {
		return fmt.Errorf("failed to save to postgres: %w", err)
	}

	// Save to Redis
	if err := cache.SaveToRedis(ctx, b.redisClient, redisExpiration); err != nil {
		return fmt.Errorf("failed to save to redis: %w", err)
	}

	return nil
}

// BuildAll builds and stores all registered caches
func (b *Builder) BuildAll(ctx context.Context, redisExpiration time.Duration) error {
	for name := range b.registry.All() {
		if err := b.BuildAndStore(ctx, name, redisExpiration); err != nil {
			return fmt.Errorf("failed to build cache %s: %w", name, err)
		}
	}
	return nil
}
