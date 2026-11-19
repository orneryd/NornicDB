package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type nor180BeginCleanupEngine struct {
	storage.Engine
	native       *storage.MemoryEngine
	wal          *storage.WAL
	tx           *storage.BadgerTransaction
	closeNative  bool
	closeOnBegin bool
	injected     bool
	cleanupErr   error
}

func (e *nor180BeginCleanupEngine) BeginTransaction(ctx context.Context, request storage.BeginRequest) (*storage.BadgerTransaction, error) {
	tx, err := e.native.BeginTransaction(ctx, request)
	e.tx = tx
	if err == nil && e.closeOnBegin {
		e.cleanupErr = tx.Rollback()
	}
	return tx, err
}

func TestNOR180BeginNamespaceAndConfigurationFailuresRetireHandle(t *testing.T) {
	for _, namespaced := range []bool{false, true} {
		for _, query := range []string{"BEGIN", "CREATE (:BeginCleanup)"} {
			name := "configuration/" + query
			if namespaced {
				name = "namespace/" + query
			}
			t.Run(name, func(t *testing.T) {
				_, base, _ := newWALOutcomeExecutor(t)
				engine := &nor180BeginCleanupEngine{Engine: base.MemoryEngine, native: base.MemoryEngine, wal: base.wal, closeOnBegin: true}
				var owner storage.Engine = engine
				if namespaced {
					owner = storage.NewNamespacedEngine(engine, "wal_outcome")
				}
				exec := NewStorageExecutor(owner)
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, engine.cleanupErr)
				require.NotNil(t, engine.tx)
				require.Nil(t, result)
				require.ErrorIs(t, err, storage.ErrTransactionClosed)
				require.NotErrorIs(t, err, storage.ErrWALClosed)
				require.False(t, engine.injected, "setup failed before WAL begin")
				require.False(t, exec.HasActiveTransaction())
			})
		}
	}
}

func (e *nor180BeginCleanupEngine) GetWAL() *storage.WAL {
	if e.tx != nil && !e.injected {
		e.injected = true
		if e.closeNative {
			e.cleanupErr = e.tx.Rollback()
		}
		if err := e.wal.Close(); err != nil {
			e.cleanupErr = err
		}
	}
	return e.wal
}

func TestNOR180BeginPreservesSetupAndCleanupFailures(t *testing.T) {
	for _, query := range []string{"BEGIN", "CREATE (:BeginCleanup)"} {
		for _, closed := range []bool{false, true} {
			name := "successful_cleanup"
			if closed {
				name = "already_closed_cleanup"
			}
			t.Run(query+"/"+name, func(t *testing.T) {
				_, base, _ := newWALOutcomeExecutor(t)
				engine := &nor180BeginCleanupEngine{Engine: base.MemoryEngine, native: base.MemoryEngine, wal: base.wal, closeNative: closed}
				exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "wal_outcome"))
				result, err := exec.Execute(t.Context(), query, nil)
				require.True(t, engine.injected, "inject at WAL begin after native setup")
				require.NoError(t, engine.cleanupErr)
				require.Nil(t, result)
				require.ErrorIs(t, err, storage.ErrWALClosed)
				if closed {
					require.ErrorIs(t, err, storage.ErrTransactionClosed, "BEGIN must preserve its distinct cleanup failure")
				}
				if !closed {
					require.NotErrorIs(t, err, storage.ErrTransactionClosed)
				}
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", code)
				require.False(t, exec.HasActiveTransaction())
				require.ErrorIs(t, engine.tx.Rollback(), storage.ErrTransactionClosed, "native transaction has retired")
			})
		}
	}
}
