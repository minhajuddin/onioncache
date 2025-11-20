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

	// Test loading cache in a loop
	cacheName := "users"
	iterations := 10
	interval := 2 * time.Second

	fmt.Printf("Testing cache '%s' for %d iterations (interval: %v)\n", cacheName, iterations, interval)
	fmt.Println("---")

	for i := 0; i < iterations; i++ {
		fmt.Printf("\n[Iteration %d] ", i+1)

		// Try loading from Redis first
		cache := core.NewCache(cacheName, nil)
		err := cache.LoadFromRedis(ctx, rdb)
		if err == nil {
			fmt.Printf("✓ Loaded from Redis (%d entries)\n", len(cache.Data))
			printSample(cache.Data, 3)
		} else {
			fmt.Printf("✗ Redis load failed: %v\n", err)

			// Fallback to Postgres
			err = cache.LoadFromPostgres(ctx, pgConn)
			if err == nil {
				fmt.Printf("✓ Loaded from Postgres (%d entries)\n", len(cache.Data))
				printSample(cache.Data, 3)
			} else {
				fmt.Printf("✗ Postgres load failed: %v\n", err)
			}
		}

		if i < iterations-1 {
			time.Sleep(interval)
		}
	}

	fmt.Println("\n✓ Test completed")
}

func printSample(data map[string]string, limit int) {
	count := 0
	for k, v := range data {
		if count >= limit {
			fmt.Println("  ...")
			break
		}
		fmt.Printf("  %s => %s\n", k, v)
		count++
	}
}
