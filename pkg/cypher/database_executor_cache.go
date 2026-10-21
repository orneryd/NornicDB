package cypher

import (
	"sync"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// CachedDatabaseExecutor returns a valid database execution view, creating and
// publishing one under the database cache lock only after its selection is
// checked again. The discarded contender is never exposed to callers.
func CachedDatabaseExecutor[T any](mu *sync.RWMutex, cache *map[string]T, name string, valid func(T) bool, create func() (T, error)) (T, error) {
	mu.RLock()
	if executor, ok := (*cache)[name]; ok && valid(executor) {
		mu.RUnlock()
		return executor, nil
	}
	mu.RUnlock()

	executor, err := create()
	if err != nil {
		var zero T
		return zero, err
	}
	mu.Lock()
	defer mu.Unlock()
	if existing, ok := (*cache)[name]; ok && valid(existing) {
		return existing, nil
	}
	if !valid(executor) {
		var zero T
		return zero, multidb.ErrDatabaseNotFound
	}
	if *cache == nil {
		*cache = make(map[string]T)
	}
	(*cache)[name] = executor
	return executor, nil
}

// DatabaseExecutorCache binds one protocol's cache lifetime to the common
// publication algorithm. A protocol supplies only its view creation and a
// validity predicate; no second cache algorithm is needed.
type DatabaseExecutorCache[T any] struct {
	mu      *sync.RWMutex
	entries *map[string]T
	valid   func(T) bool
	create  func(string) (T, error)
	before  func()
}

// NewDatabaseExecutorCache binds an existing protocol cache to the common
// get-or-create owner while preserving its invalidation lock.
func NewDatabaseExecutorCache[T any](mu *sync.RWMutex, entries *map[string]T, valid func(T) bool, create func(string) (T, error), before func()) *DatabaseExecutorCache[T] {
	return &DatabaseExecutorCache[T]{mu: mu, entries: entries, valid: valid, create: create, before: before}
}

// Get returns the current valid database view, building one if needed.
func (c *DatabaseExecutorCache[T]) Get(name string) (T, error) {
	if c.before != nil {
		c.before()
	}
	return CachedDatabaseExecutor(c.mu, c.entries, name, c.valid, func() (T, error) { return c.create(name) })
}

// Delete invalidates one database after its catalog entry changes.
func (c *DatabaseExecutorCache[T]) Delete(name string) {
	c.mu.Lock()
	delete(*c.entries, name)
	c.mu.Unlock()
}

// Clear invalidates all cached views.
func (c *DatabaseExecutorCache[T]) Clear() {
	c.mu.Lock()
	*c.entries = nil
	c.mu.Unlock()
}

// EvictInvalid removes views whose catalog selections have been retired.
func (c *DatabaseExecutorCache[T]) EvictInvalid() {
	c.mu.Lock()
	for name, executor := range *c.entries {
		if !c.valid(executor) {
			delete(*c.entries, name)
		}
	}
	c.mu.Unlock()
}
