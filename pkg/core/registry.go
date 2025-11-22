package core

import (
	"fmt"
	"time"
)

func NewRegistry() *Registry {
	return &Registry{}
}

type Cache interface{}

type Registry struct {
	caches []Cache
	done   chan struct{}
}

// startTime is passed for metrics
func (r *Registry) RefreshCache(startTime time.Time) {
	timeSinceStart := time.Since(startTime)
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
