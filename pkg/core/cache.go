package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Cache represents a string-to-string cache
type Cache struct {
	Name string
	Data map[string]string
}

// NewCache creates a new cache
func NewCache(name string, data map[string]string) *Cache {
	return &Cache{
		Name: name,
		Data: data,
	}
}

// SaveToRedis saves the cache to Redis
func (c *Cache) SaveToRedis(ctx context.Context, rdb *redis.Client, expiration time.Duration) error {
	key := fmt.Sprintf("cache:%s", c.Name)
	data, err := json.Marshal(c.Data)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, key, data, expiration).Err()
}

// LoadFromRedis loads the cache from Redis
func (c *Cache) LoadFromRedis(ctx context.Context, rdb *redis.Client) error {
	key := fmt.Sprintf("cache:%s", c.Name)
	data, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), &c.Data)
}

// SaveToPostgres saves the cache to Postgres
func (c *Cache) SaveToPostgres(ctx context.Context, conn *pgx.Conn) error {
	data, err := json.Marshal(c.Data)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, `
		INSERT INTO cache_storage (name, data, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE
		SET data = $2, updated_at = $3
	`, c.Name, data, time.Now())

	return err
}

// LoadFromPostgres loads the cache from Postgres
func (c *Cache) LoadFromPostgres(ctx context.Context, conn *pgx.Conn) error {
	var data []byte
	err := conn.QueryRow(ctx, `
		SELECT data FROM cache_storage WHERE name = $1
	`, c.Name).Scan(&data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &c.Data)
}
