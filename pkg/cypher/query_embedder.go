package cypher

import (
	"context"
	"sync"
	"sync/atomic"
)

// QueryEmbedderBinding publishes one query provider to every protocol and scope.
// The zero value has no provider. A load takes one atomic snapshot; publication
// replaces both the provider's embedding cache and its result-cache generation.
// It retains no registry of services, executors, caches, or previous providers.
type QueryEmbedderBinding struct {
	current atomic.Pointer[queryEmbedderState]
	next    atomic.Uint64
	beginUse func(context.Context) (context.Context, func(), error)
}

// SetLifetime binds provider users to their database owner. Configure it during
// construction, before exposing the binding or sharing it with scoped executors.
func (b *QueryEmbedderBinding) SetLifetime(begin func(context.Context) (context.Context, func(), error)) {
	b.beginUse = begin
}

// BeginUse admits one provider-using operation and returns its cancellation
// context and release function. A standalone binding has no lifetime owner.
// Callers must release before returning, including on errors.
func (b *QueryEmbedderBinding) BeginUse(ctx context.Context) (context.Context, func(), error) {
	if b != nil && b.beginUse != nil {
		return b.beginUse(ctx)
	}
	return ctx, func() {}, nil
}

// Load returns the current provider, or nil while it is unavailable.
func (b *QueryEmbedderBinding) Load() QueryEmbedder {
	if state := b.snapshot(); state != nil {
		return state.embedder
	}
	return nil
}

func (b *QueryEmbedderBinding) snapshot() *queryEmbedderState {
	if b == nil {
		return nil
	}
	return b.current.Load()
}

// Store publishes a provider, including nil to make it unavailable. Old queries
// may finish using their captured snapshot, but their cached results cannot be
// served to a query that captured a different publication.
func (b *QueryEmbedderBinding) Store(embedder QueryEmbedder) {
	state := &queryEmbedderState{embedder: embedder, generation: b.next.Add(1)}
	if embedder != nil {
		state.cache = make(map[string][]float32, 512)
		state.inflight = make(map[string]*vectorEmbedInflight, 64)
	}
	b.current.Store(state)
}

type queryEmbedderState struct {
	embedder   QueryEmbedder
	generation uint64
	mu         sync.Mutex
	cache      map[string][]float32
	inflight   map[string]*vectorEmbedInflight
}

func (e *StorageExecutor) currentQueryEmbedder() *queryEmbedderState {
	return e.queryEmbedder.snapshot()
}

// QueryEmbedderBinding exposes this executor's publication owner for protocol
// adapters. Capture it at construction, rather than copying a loaded provider.
func (e *StorageExecutor) QueryEmbedderBinding() *QueryEmbedderBinding {
	return e.queryEmbedder
}

// ShareEmbedderFrom binds a scoped executor to its parent's current and future
// query embedder. Call it during construction, before serving requests.
func (e *StorageExecutor) ShareEmbedderFrom(parent *StorageExecutor) {
	if parent != nil && parent.queryEmbedder != nil {
		e.queryEmbedder = parent.queryEmbedder
	}
}

type queryEmbeddingStatementKey struct{}

// This replaces the existing statement-clock context, so capturing a provider
// adds no context allocation. Nested scopes reuse the outer snapshot, including
// its nil startup state. Embedding helpers never reload a published provider.
type queryEmbeddingStatementContext struct {
	context.Context
	instant any // Box the statement clock once, rather than on each Value call.
	binding *QueryEmbedderBinding
	state   *queryEmbedderState
}

func (c *queryEmbeddingStatementContext) Value(key any) any {
	switch key.(type) {
	case temporalStatementTimeKey:
		return c.instant
	case queryEmbeddingStatementKey:
		return c
	}
	return c.Context.Value(key)
}

func (e *StorageExecutor) queryEmbeddingState(ctx context.Context) *queryEmbedderState {
	if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*queryEmbeddingStatementContext); ok && statement.binding == e.queryEmbedder {
		return statement.state
	}
	return e.currentQueryEmbedder()
}

func (e *StorageExecutor) queryEmbedderForStatement(ctx context.Context) QueryEmbedder {
	if state := e.queryEmbeddingState(ctx); state != nil {
		return state.embedder
	}
	return nil
}
