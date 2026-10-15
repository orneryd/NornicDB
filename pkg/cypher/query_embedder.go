package cypher

import (
	"sync"
	"sync/atomic"
	"weak"

	"github.com/orneryd/nornicdb/pkg/search"
)

// queryEmbedderBinding is shared by all scopes. Its mutex is used only when
// wiring a service or publishing a provider, never when executing a query.
type queryEmbedderBinding struct {
	current  atomic.Pointer[queryEmbedderState]
	mu       sync.Mutex
	services map[weak.Pointer[search.Service]]struct{}
}

// queryEmbedderState binds a provider and its vector-query cache to one
// publication. Replacing the provider also replaces the cache; requests still
// using the previous provider can finish without populating the new cache.
type queryEmbedderState struct {
	embedder QueryEmbedder
	mu       sync.Mutex
	cache    map[string][]float32
	inflight map[string]*vectorEmbedInflight
}

func (e *StorageExecutor) currentQueryEmbedder() *queryEmbedderState {
	if e.queryEmbedder == nil {
		return nil
	}
	return e.queryEmbedder.current.Load()
}

// ShareEmbedderFrom binds a scoped executor to its parent's current and future
// query embedder, including when the provider is still loading. Call it during
// executor construction, before serving requests. SetEmbedder publishes updates
// to all executors sharing this binding; embedding reads need no readiness lock.
func (e *StorageExecutor) ShareEmbedderFrom(parent *StorageExecutor) {
	if parent != nil && parent.queryEmbedder != nil {
		e.queryEmbedder = parent.queryEmbedder
		e.queryEmbedder.registerSearchService(e.searchService)
	}
}

func (b *queryEmbedderBinding) registerSearchService(svc *search.Service) {
	if b == nil || svc == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.services == nil {
		b.services = make(map[weak.Pointer[search.Service]]struct{})
	}
	b.services[weak.Make(svc)] = struct{}{}
}
