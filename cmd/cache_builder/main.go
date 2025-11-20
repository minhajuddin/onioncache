package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/minhajuddin/onioncache/pkg/core"
)

func main() {
	ctx := context.Background()

	// Get connection strings from environment
	pgURL := os.Getenv("DATABASE_URL")
	if pgURL == "" {
		pgURL = "postgres://localhost:5432/onioncache?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// Connect to Postgres
	pgConn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer pgConn.Close(ctx)

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	// Create registry and register cache entries
	registry := core.NewRegistry()

	// Example: Register a simple cache entry
	registry.Register("users", "SELECT id::text, name FROM users", func(rows pgx.Rows) (map[string]string, error) {
		cache := make(map[string]string)
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err != nil {
				return nil, err
			}
			cache[id] = name
		}
		return cache, rows.Err()
	})

	// Create builder
	builder := core.NewBuilder(registry, pgConn, rdb)

	// Build and store all caches
	log.Println("Building caches...")
	if err := builder.BuildAll(ctx, 1*time.Hour); err != nil {
		log.Fatalf("Failed to build caches: %v", err)
	}

	fmt.Println("✓ All caches built and stored successfully")
}
