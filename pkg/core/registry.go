package core

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// CacheConstructor builds a cache from SQL query results
type CacheConstructor func(rows pgx.Rows) (map[string]string, error)

// CacheEntry defines a SQL query and how to build a cache from it
type CacheEntry struct {
	Name        string
	SQL         string
	Constructor CacheConstructor
}

// Registry holds all cache entries
type Registry struct {
	entries map[string]*CacheEntry
}

// NewRegistry creates a new cache registry
func NewRegistry() *Registry {
	return &Registry{
		entries: make(map[string]*CacheEntry),
	}
}

// Register adds a cache entry to the registry
func (r *Registry) Register(name, sql string, constructor CacheConstructor) {
	r.entries[name] = &CacheEntry{
		Name:        name,
		SQL:         sql,
		Constructor: constructor,
	}
}

// Get retrieves a cache entry by name
func (r *Registry) Get(name string) (*CacheEntry, bool) {
	entry, ok := r.entries[name]
	return entry, ok
}

// All returns all registered cache entries
func (r *Registry) All() map[string]*CacheEntry {
	return r.entries
}

// BuildCache executes the SQL and builds the cache using the constructor
func (e *CacheEntry) BuildCache(ctx context.Context, conn *pgx.Conn) (map[string]string, error) {
	rows, err := conn.Query(ctx, e.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return e.Constructor(rows)
}
