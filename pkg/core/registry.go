package core

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func NewRegistry(c *pgx.Conn) *Registry {
	return &Registry{
		cachedVersionContainers: make(map[string]CachedVersionContainer),
		caches:                  []Cache{},
		done:                    make(chan struct{}),
		conn:                    c,
	}
}

// TODO: Extract this into a SQL cache interface
type Cache interface {
	ID() string
	VersionSQL() string
	RowSQL() string
}

type CachedVersionContainer struct {
	Version string
}

type Registry struct {
	cachedVersionContainers map[string]CachedVersionContainer
	caches                  []Cache
	done                    chan struct{}
	conn                    *pgx.Conn
}

type CacheVersion struct {
	Version string
}

func (r *Registry) refreshCache(cache Cache) {
	var version string
	// get the single version row
	rows := r.conn.QueryRow(context.Background(), cache.VersionSQL())
	err := rows.Scan(&version)
	if err != nil {
		fmt.Println("Error scanning cache version for cache ID:", cache.ID(), "error:", err)
		return
	}

	fmt.Println("Cache ID:", cache.ID(), "has version:", version)

	currentCachedVersionContainer, exists := r.cachedVersionContainers[cache.ID()]

	// check if we need to refresh
	if !exists || currentCachedVersionContainer.Version != version {
		fmt.Println("Cache ID:", cache.ID(), "is stale or missing, refreshing...", exists, "current version:", currentCachedVersionContainer.Version, "new version:", version)

		// fetch all rows
		rows, err := r.conn.Query(context.Background(), cache.RowSQL())
		if err != nil {
			fmt.Println("Error querying cache rows for cache ID:", cache.ID(), "error:", err)
			return
		}
		defer rows.Close()

		// Process rows as needed to refresh the cache
		// For demonstration, we'll just print the rows
		for rows.Next() {
			// Assuming the row has a single string column for simplicity
			var data string
			if err := rows.Scan(&data); err != nil {
				fmt.Println("Error scanning row for cache ID:", cache.ID(), "error:", err)
				return
			}
			fmt.Println("Cache ID:", cache.ID(), "row data:", data)
		}

		if err := rows.Err(); err != nil {
			fmt.Println("Error iterating rows for cache ID:", cache.ID(), "error:", err)
			return
		}

		// Update the cached version
		r.cachedVersionContainers[cache.ID()] = CachedVersionContainer{
			Version: version,
		}

		fmt.Println("Cache ID:", cache.ID(), "refreshed successfully.")
	} else {
		fmt.Println("Cache ID:", cache.ID(), "is up-to-date, no refresh needed.")
	}
}

// startTime is passed for metrics
func (r *Registry) RefreshCache(startTime time.Time) {
	timeSinceStart := time.Since(startTime)
	for _, cache := range r.caches {
		fmt.Println("Refreshing cache ID:", cache.ID())
		r.refreshCache(cache)
		fmt.Println("Refreshing cache ID COMPLETE:", cache.ID())
	}
	fmt.Println("Refreshing cache, time since start:", timeSinceStart)
}

func (r *Registry) AddCache(cache Cache) *Registry {
	// TODO: mutexes
	r.caches = append(r.caches, cache)
	return r
}

func (r *Registry) StopLoopGoRoutine() *Registry {
	r.done <- struct{}{}
	return r
}

func (r *Registry) StartLoopGoRoutine() *Registry {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				// TODO: Big error log and metric to monitor
				fmt.Printf("Recovered in Registry loop goroutine, we should have crashed at this point %#v\n", rec)
			}
		}()

		startTime := time.Now()

		for {
			r.RefreshCache(startTime)
			fmt.Println("sleeping for 1 minute in Registry loop goroutine...")
			select {
			case <-r.done:
				fmt.Println("stopping Registry loop goroutine as requested")
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}
	}()

	return r
}
