package cypher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// QueryEmbedderBinding publishes one query provider to every protocol and scope.
// The zero value has no provider. A load takes one atomic snapshot; publication
// replaces both the provider's embedding cache and its result-cache generation.
// It retains no registry of services, executors, caches, or previous providers.
type QueryEmbedderBinding struct {
	current atomic.Pointer[queryEmbedderState]
	next    atomic.Uint64
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
	// Automatic embedding's pause control (db.embedding.*) comes with it.
	if parent != nil && parent.embeddingController != nil {
		e.embeddingController = parent.embeddingController
	}
}

type queryEmbeddingStatementKey struct{}

// This replaces the existing statement-clock context, so capturing a provider
// adds no context allocation. Nested scopes reuse the outer snapshot, including
// its nil startup state. Embedding helpers never reload a published provider.
type queryEmbeddingStatementContext struct {
	context.Context
	instant time.Time // The statement clock: Value hands out its address, so reading it allocates nothing.
	binding *QueryEmbedderBinding
	state   *queryEmbedderState
	outcome statementOutcome
}

// statementOutcome belongs to the semantic statement, not an Execute frame or
// a result object: a transparent continuation (PROFILE, USE and
// administration routing, through executeRouted) is marked continued and
// defers to the statement context it continues, so a commit its inner
// execution publishes is the outer statement's. A public Execute, including
// an independent reentrant call, starts a fresh one. Publication is monotonic
// and happens only where writes become durable (a storage commit, a direct
// effect); rolled-back, read-only and still-open explicit writes publish
// nothing. Kept in the statement context's own allocation.
type statementOutcome struct {
	flags atomic.Uint32
}

const (
	statementCommitted uint32 = 1 << iota
	statementContinued
)

// root is the statement context that owns c's outcome.
func (c *queryEmbeddingStatementContext) root() *queryEmbeddingStatementContext {
	for c.outcome.flags.Load()&statementContinued != 0 {
		outer, ok := c.Context.Value(queryEmbeddingStatementKey{}).(*queryEmbeddingStatementContext)
		if !ok {
			break
		}
		c = outer
	}
	return c
}

// committed reports that the statement's writes are durably committed.
func (c *queryEmbeddingStatementContext) committed() bool {
	return c.root().outcome.flags.Load()&statementCommitted != 0
}

// publishStatementCommit records that ctx's statement's writes are durably
// committed: from then on a cancellation or deadline doesn't fail it.
func publishStatementCommit(ctx context.Context) {
	if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*queryEmbeddingStatementContext); ok {
		statement.root().outcome.flags.Or(statementCommitted)
	}
}

// publishDirectStatementCommit publishes a statement that ran without a
// transaction (a legacy engine): a write or schema statement's completed
// effects are already applied. Reads publish nothing.
func publishDirectStatementCommit(ctx context.Context, query string) {
	if permissions := QueryPermissionRequirements(query); permissions.Write || permissions.Schema {
		publishStatementCommit(ctx)
	}
}

func (c *queryEmbeddingStatementContext) Value(key any) any {
	switch key.(type) {
	case temporalStatementTimeKey:
		return &c.instant
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
