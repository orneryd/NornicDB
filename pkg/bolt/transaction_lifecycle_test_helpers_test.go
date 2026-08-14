package bolt

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const transactionLifecycleControlledQuery = "RETURN 1 /* transaction lifecycle barrier */"

// startTransactionLifecycleServerWithStore runs the real database-manager Bolt
// adapter over store. Callers register store cleanup before invoking it so the
// server cleanup runs first under testing's LIFO order.
func startTransactionLifecycleServerWithStore(t *testing.T, store storage.Engine) int {
	t.Helper()
	mgr := &mockDBManager{
		stores:    map[string]storage.Engine{"nornic": store},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	return startBoltTestServer(t, server)
}

// countingTransactionEngine keeps the real MemoryEngine behavior while making
// storage transaction allocation observable to invalid-metadata tests.
type countingTransactionEngine struct {
	storage.Engine
	inner  *storage.MemoryEngine
	begins atomic.Int64
}

// BeginTransaction counts and delegates real MemoryEngine transaction
// allocation. The wrapper intentionally has no unwrapping method so the Cypher
// executor must exercise this boundary.
func (e *countingTransactionEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	e.begins.Add(1)
	return e.inner.BeginTransaction()
}

// controlledTransactionExecutor preserves production transaction behavior and
// adds test-only RUN and COMMIT ordering barriers at the QueryExecutor boundary.
type controlledTransactionExecutor struct {
	adapter *transactionalBoltQueryExecutorAdapter

	runStarted         chan struct{}
	runCanceled        chan error
	runStartOnce       sync.Once
	cancelOnce         sync.Once
	succeedAfterCancel bool
	runRelease         <-chan struct{}

	commitEntered chan struct{}
	commitRelease <-chan struct{}
	commitOnce    sync.Once
}

type fixedSessionExecutorFactory struct {
	QueryExecutor
}

func (f fixedSessionExecutorFactory) NewSessionExecutor() QueryExecutor {
	return f.QueryExecutor
}

// newControlledTransactionExecutor wraps the production transactional query
// adapter and intercepts only transactionLifecycleControlledQuery.
func newControlledTransactionExecutor(store storage.Engine) *controlledTransactionExecutor {
	return &controlledTransactionExecutor{
		adapter: &transactionalBoltQueryExecutorAdapter{
			boltQueryExecutorAdapter: boltQueryExecutorAdapter{
				executor: cypher.NewStorageExecutor(store),
			},
		},
		runStarted:    make(chan struct{}),
		runCanceled:   make(chan error, 1),
		commitEntered: make(chan struct{}),
	}
}

// Execute blocks the sentinel query inside the lifecycle-owned RUN operation.
// It returns only when the Session-provided context is canceled, allowing tests
// to prove deadline cancellation precedes the owned rollback.
func (e *controlledTransactionExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	if query != transactionLifecycleControlledQuery {
		return e.adapter.Execute(ctx, query, params)
	}

	e.runStartOnce.Do(func() { close(e.runStarted) })
	<-ctx.Done()
	e.cancelOnce.Do(func() { e.runCanceled <- ctx.Err() })
	if e.runRelease != nil {
		<-e.runRelease
	}
	if e.succeedAfterCancel {
		return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
	}
	return nil, ctx.Err()
}

// HasPendingTransactionWrites delegates to the production transaction adapter.
func (e *controlledTransactionExecutor) HasPendingTransactionWrites() bool {
	return e.adapter.HasPendingTransactionWrites()
}

// BeginTransaction delegates to the production transaction adapter.
func (e *controlledTransactionExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	return e.adapter.BeginTransaction(ctx, metadata)
}

// CommitTransaction exposes a barrier before delegating to the production
// adapter so tests can force COMMIT to win terminal-state arbitration.
func (e *controlledTransactionExecutor) CommitTransaction(ctx context.Context) error {
	e.commitOnce.Do(func() { close(e.commitEntered) })
	if e.commitRelease != nil {
		<-e.commitRelease
	}
	return e.adapter.CommitTransaction(ctx)
}

// RollbackTransaction delegates to the production transaction adapter.
func (e *controlledTransactionExecutor) RollbackTransaction(ctx context.Context) error {
	return e.adapter.RollbackTransaction(ctx)
}

// startControlledTransactionServer runs a server with one explicitly supplied
// executor, preserving the production Session and raw Bolt message path.
func startControlledTransactionServer(t *testing.T, executor QueryExecutor) int {
	t.Helper()
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	return startBoltTestServer(t, server)
}

// primeTestTransactionLifecycle initializes a hand-built Session through the
// same lifecycle entrypoint production BEGIN uses. Legacy handler unit tests
// set inTransaction directly; without this helper that state is intentionally
// rejected as inconsistent by the fail-closed terminal arbiter.
func primeTestTransactionLifecycle(t *testing.T, session *Session) {
	t.Helper()
	txExec, _ := session.executor.(TransactionalExecutor)
	if err := session.txLifecycle.begin(
		context.Background(),
		0,
		session.txDatabase,
		txExec,
		session.txMetadata,
		nil,
	); err != nil {
		t.Fatalf("prime transaction lifecycle: %v", err)
	}
}
