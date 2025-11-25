package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// DefaultRefreshInterval is the default time between cache refresh cycles.
	DefaultRefreshInterval = 10 * time.Second
)

// SafeMap provides thread-safe access to a map.
// Consumers should use this to store their cache data to avoid race conditions.
type SafeMap[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewSafeMap creates a new thread-safe map.
func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		data: make(map[K]V),
		mu:   sync.RWMutex{},
	}
}

// Set stores a value in the map.
func (sm *SafeMap[K, V]) Set(key K, value V) {
	sm.mu.Lock()
	sm.data[key] = value
	sm.mu.Unlock()
}

// Get retrieves a value from the map.
func (sm *SafeMap[K, V]) Get(key K) (V, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	val, ok := sm.data[key]
	return val, ok
}

// SetAll replaces all data in the map with the provided map.
// This is useful for cache Reset operations.
func (sm *SafeMap[K, V]) SetAll(newData map[K]V) {
	sm.mu.Lock()
	sm.data = newData
	sm.mu.Unlock()
}

// GetAll returns a copy of all data in the map.
func (sm *SafeMap[K, V]) GetAll() map[K]V {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[K]V, len(sm.data))
	for k, v := range sm.data {
		result[k] = v
	}
	return result
}

// Len returns the number of items in the map.
func (sm *SafeMap[K, V]) Len() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.data)
}

// SQLCache is a generic, ready-to-use cache implementation that eliminates boilerplate.
// It automatically generates SQL queries and handles all the Cache interface requirements.
//
// Example usage:
//
//	type User struct {
//	    ID   int
//	    Name string
//	    Email string
//	}
//
//	// Create a cache with just the table name and key extractor
//	userCache := core.NewSQLCache("users", func(u User) int { return u.ID })
//
//	// That's it! No need to implement Cache interface manually.
//	registry.AddCache(userCache)
//
//	// Access data thread-safely
//	user, ok := userCache.Get(123)
type SQLCache[K comparable, V any] struct {
	tableName  string
	cacheID    string
	keyFunc    func(V) K
	data       *SafeMap[K, V]
	columns    []string
	versionSQL string
	rowSQL     string
}

// NewSQLCache creates a new generic SQL-backed cache.
// tableName: the database table name
// keyFunc: function to extract the key from a value (e.g., func(u User) int { return u.ID })
//
// The cache will automatically:
// - Generate version SQL using MD5 hashing
// - Generate row SQL by reflecting on the struct fields
// - Handle thread-safe updates
// - Implement the Cache interface
func NewSQLCache[K comparable, V any](tableName string, keyFunc func(V) K) *SQLCache[K, V] {
	cache := &SQLCache[K, V]{
		tableName: tableName,
		cacheID:   tableName + "_cache",
		keyFunc:   keyFunc,
		data:      NewSafeMap[K, V](),
	}

	// Generate the version SQL (MD5 hash of all rows)
	cache.versionSQL = fmt.Sprintf(
		`SELECT MD5(CAST((ARRAY_AGG(t.* ORDER BY t)) AS text)) AS version FROM %s t`,
		tableName,
	)

	// Generate row SQL by inspecting the struct type
	// We'll use SELECT * for simplicity and let pgx handle the mapping
	cache.rowSQL = fmt.Sprintf("SELECT * FROM %s", tableName)

	return cache
}

// WithColumns allows specifying which columns to select instead of SELECT *.
// This is useful for excluding large columns or selecting specific fields.
//
// Example:
//
//	cache := NewSQLCache("users", func(u User) int { return u.ID }).
//	    WithColumns("id", "name", "email")
func (c *SQLCache[K, V]) WithColumns(columns ...string) *SQLCache[K, V] {
	c.columns = columns
	if len(columns) > 0 {
		columnList := ""
		for i, col := range columns {
			if i > 0 {
				columnList += ", "
			}
			columnList += col
		}
		c.rowSQL = fmt.Sprintf("SELECT %s FROM %s", columnList, c.tableName)
	}
	return c
}

// WithID allows customizing the cache ID (defaults to tableName + "_cache").
func (c *SQLCache[K, V]) WithID(id string) *SQLCache[K, V] {
	c.cacheID = id
	return c
}

// ID implements the Cache interface.
func (c *SQLCache[K, V]) ID() string {
	return c.cacheID
}

// VersionSQL implements the Cache interface.
func (c *SQLCache[K, V]) VersionSQL() string {
	return c.versionSQL
}

// RowSQL implements the Cache interface.
func (c *SQLCache[K, V]) RowSQL() string {
	return c.rowSQL
}

// Reset implements the Cache interface.
func (c *SQLCache[K, V]) Reset(rows pgx.Rows) error {
	// Collect all rows into a slice
	values, err := pgx.CollectRows(rows, pgx.RowToStructByName[V])
	if err != nil {
		return fmt.Errorf("failed to collect rows: %w", err)
	}

	// Convert slice to map using the key function
	dataMap := make(map[K]V, len(values))
	for _, value := range values {
		key := c.keyFunc(value)
		dataMap[key] = value
	}

	// Update the cache atomically
	c.data.SetAll(dataMap)

	return nil
}

// Get retrieves a value from the cache by key (thread-safe).
func (c *SQLCache[K, V]) Get(key K) (V, bool) {
	return c.data.Get(key)
}

// GetAll returns a copy of all cached data (thread-safe).
func (c *SQLCache[K, V]) GetAll() map[K]V {
	return c.data.GetAll()
}

// Len returns the number of items in the cache (thread-safe).
func (c *SQLCache[K, V]) Len() int {
	return c.data.Len()
}

// Data returns the underlying SafeMap for advanced use cases.
func (c *SQLCache[K, V]) Data() *SafeMap[K, V] {
	return c.data
}

// NewRegistry creates a new cache registry.
// If logger is nil, a default logger will be used.
func NewRegistry(c *pgx.Conn, logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{
		cachedVersionContainers: make(map[string]CachedVersionContainer),
		caches:                  []Cache{},
		done:                    make(chan struct{}),
		conn:                    c,
		logger:                  logger,
	}
}

// Cache represents a cacheable data structure that can be automatically refreshed
// from PostgreSQL when the underlying data changes.
//
// Thread-safety: The Reset method will be called concurrently with reads from your
// cache data structure. You MUST use synchronization (e.g., sync.RWMutex or the
// provided SafeMap utility) to prevent race conditions.
//
// Example implementation:
//
//	type UserCache struct {
//	    users *core.SafeMap[int, User]
//	}
//
//	func (c *UserCache) ID() string {
//	    return "users"
//	}
//
//	func (c *UserCache) VersionSQL() string {
//	    // Must return a single row with a column named "version"
//	    return `SELECT MD5(CAST((ARRAY_AGG(t.* ORDER BY t)) AS text)) AS version
//	            FROM users t`
//	}
//
//	func (c *UserCache) RowSQL() string {
//	    // Column names must match struct field names (case-insensitive)
//	    return "SELECT id, name, email FROM users"
//	}
//
//	func (c *UserCache) Reset(rows pgx.Rows) error {
//	    users, err := pgx.CollectRows(rows, pgx.RowToStructByName[User])
//	    if err != nil {
//	        return err
//	    }
//	    // Convert to map and store atomically
//	    userMap := make(map[int]User)
//	    for _, user := range users {
//	        userMap[user.ID] = user
//	    }
//	    c.users.SetAll(userMap)
//	    return nil
//	}
type Cache interface {
	// ID returns a unique identifier for this cache.
	// This is used for logging and internal tracking.
	ID() string

	// VersionSQL returns a SQL query that produces a single row with a single column
	// named "version". The version value should change whenever the underlying data
	// changes. A common approach is to use MD5 hashing of all rows:
	//
	//   SELECT MD5(CAST((ARRAY_AGG(t.* ORDER BY t)) AS text)) AS version FROM table_name t
	//
	// The column MUST be named "version" (case-insensitive).
	VersionSQL() string

	// RowSQL returns a SQL query that fetches all rows to be cached.
	// Column names in the SELECT clause must match your struct field names
	// (case-insensitive) for pgx.RowToStructByName to work correctly.
	RowSQL() string

	// Reset is called when the cache needs to be rebuilt with fresh data.
	// This method MUST be thread-safe as it will be called concurrently with
	// reads from your cache data structure.
	//
	// The rows parameter contains the result of executing RowSQL().
	// You should:
	// 1. Collect the rows into your data structure
	// 2. Update your cache atomically (using mutexes or SafeMap)
	// 3. Return any errors that occur
	//
	// If Reset returns an error, the cache version will NOT be updated,
	// and the refresh will be retried on the next cycle.
	Reset(pgx.Rows) error
}

type CachedVersionContainer struct {
	Version string
	Cache   Cache
}

type Registry struct {
	mu                      sync.RWMutex
	cachedVersionContainers map[string]CachedVersionContainer
	caches                  []Cache
	done                    chan struct{}
	conn                    *pgx.Conn
	logger                  *slog.Logger
}

func (r *Registry) rebuildCache(ctx context.Context, cache Cache, version string) error {
	// fetch all rows
	rows, err := r.conn.Query(ctx, cache.RowSQL())
	if err != nil {
		r.logger.Error("error querying cache rows", "cache_id", cache.ID(), "error", err)
		return fmt.Errorf("error querying cache rows for cache ID %s: %w", cache.ID(), err)
	}
	defer rows.Close()

	err = cache.Reset(rows)
	if err != nil {
		r.logger.Error("error resetting cache", "cache_id", cache.ID(), "error", err)
		return fmt.Errorf("error resetting cache for cache ID %s: %w", cache.ID(), err)
	}

	// Update the cached version
	r.mu.Lock()
	r.cachedVersionContainers[cache.ID()] = CachedVersionContainer{
		Version: version,
		Cache:   cache,
	}
	r.mu.Unlock()

	return nil
}

func (r *Registry) refreshCache(ctx context.Context, cache Cache) error {
	var version string
	// get the single version row
	rows := r.conn.QueryRow(ctx, cache.VersionSQL())
	err := rows.Scan(&version)
	if err != nil {
		r.logger.Error("error scanning cache version", "cache_id", cache.ID(), "error", err)
		return fmt.Errorf("error scanning cache version for cache ID %s: %w", cache.ID(), err)
	}

	r.logger.Debug("cache version check", "cache_id", cache.ID(), "version", version)

	r.mu.RLock()
	currentCachedVersionContainer, exists := r.cachedVersionContainers[cache.ID()]
	r.mu.RUnlock()

	// check if we need to refresh
	if !exists || currentCachedVersionContainer.Version != version {
		r.logger.Info("cache is stale, refreshing", "cache_id", cache.ID(), "current_version", currentCachedVersionContainer.Version, "new_version", version)

		err := r.rebuildCache(ctx, cache, version)
		if err != nil {
			return err
		}

		r.logger.Info("cache refreshed successfully", "cache_id", cache.ID())
	} else {
		r.logger.Debug("cache is up-to-date", "cache_id", cache.ID())
	}

	return nil
}

// RefreshCache refreshes all registered caches by checking their versions
// and rebuilding them if they are stale.
func (r *Registry) RefreshCache(ctx context.Context) {
	cycleStart := time.Now()

	r.mu.RLock()
	caches := make([]Cache, len(r.caches))
	copy(caches, r.caches)
	r.mu.RUnlock()

	for _, cache := range caches {
		r.logger.Debug("refreshing cache", "cache_id", cache.ID())
		err := r.refreshCache(ctx, cache)
		if err != nil {
			r.logger.Error("error refreshing cache", "cache_id", cache.ID(), "error", err)
			continue
		}
		r.logger.Debug("cache refresh complete", "cache_id", cache.ID())
	}
	r.logger.Debug("refresh cycle complete", "duration", time.Since(cycleStart))
}

func (r *Registry) AddCache(cache Cache) *Registry {
	r.mu.Lock()
	r.caches = append(r.caches, cache)
	r.mu.Unlock()
	return r
}

func (r *Registry) StopLoopGoroutine() *Registry {
	r.done <- struct{}{}
	return r
}

func (r *Registry) StartLoopGoroutine(ctx context.Context) *Registry {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.Error("panic in registry loop, shutting down", "panic", rec)
				// After a panic, we exit the goroutine to fail-fast
				// rather than continuing in a potentially corrupted state
			}
		}()

		for {
			r.RefreshCache(ctx)
			r.logger.Debug("sleeping before next refresh cycle", "interval", DefaultRefreshInterval)
			select {
			case <-ctx.Done():
				r.logger.Info("stopping registry loop due to context cancellation")
				return
			case <-r.done:
				r.logger.Info("stopping registry loop as requested")
				return
			case <-time.After(DefaultRefreshInterval):
				continue
			}
		}
	}()

	return r
}
