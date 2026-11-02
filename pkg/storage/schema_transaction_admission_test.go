package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchemaTransactionAdmissionCancellationPrecedesSnapshot(t *testing.T) {
	engine := newTestMemoryEngine(t)
	schema := engine.GetSchemaForNamespace("admission")
	release := schema.LockSchemaMutation()
	defer func() {
		if release != nil {
			release()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waited := false
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &schema.schemaMutationMu {
			waited = true
			cancel()
		}
	})
	defer restore()
	began := false
	tx, err := schema.BeginSchemaTransaction(ctx, func(beginCtx context.Context) (*BadgerTransaction, error) {
		began = true
		return engine.BeginTransaction(beginCtx)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, tx)
	require.True(t, waited)
	require.False(t, began, "blocked admission must not open a stale physical snapshot")
	release()
	release = nil
	tx, err = schema.BeginSchemaTransaction(context.Background(), engine.BeginTransaction)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Nil(t, tx.schemaAdmission)
	require.True(t, schema.schemaMutationMu.TryLock())
	schema.schemaMutationMu.Unlock()
}

func TestSchemaTransactionAdmissionBeginFailureReleases(t *testing.T) {
	schema := NewSchemaManager()
	failure := errors.New("begin rejected")
	tx, err := schema.BeginSchemaTransaction(context.Background(), func(context.Context) (*BadgerTransaction, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	require.Nil(t, tx)
	require.True(t, schema.schemaMutationMu.TryLock())
	schema.schemaMutationMu.Unlock()
}

func TestSchemaTransactionAdmissionTerminalRelease(t *testing.T) {
	for _, terminal := range []string{"commit", "rollback", "conflict"} {
		t.Run(terminal, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			schema := engine.GetSchemaForNamespace("admission")
			tx, err := schema.BeginSchemaTransaction(context.Background(), engine.BeginTransaction)
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("admission"))
			view, err := tx.Schema(OrdinarySchemaChanges)
			require.NoError(t, err)
			require.NoError(t, view.AddPropertyIndex("admitted", "Item", []string{"id"}))
			require.NoError(t, tx.StageSchemaChanges())
			require.False(t, schema.schemaMutationMu.TryLock(), "lease ends only at native terminal cleanup")
			switch terminal {
			case "commit":
				require.NoError(t, tx.Commit())
			case "rollback":
				require.NoError(t, tx.Rollback())
			case "conflict":
				_, err = engine.CreateNode(t.Context(), &Node{ID: "admission:peer", Labels: []string{"Item"}})
				require.NoError(t, err)
				require.ErrorIs(t, tx.Commit(), ErrConflict)
			}
			require.Nil(t, tx.schemaAdmission)
			require.True(t, schema.schemaMutationMu.TryLock())
			schema.schemaMutationMu.Unlock()
			require.ErrorIs(t, tx.Rollback(), ErrTransactionClosed)
			require.True(t, schema.schemaMutationMu.TryLock(), "repeated close cannot release a peer's lease")
			schema.schemaMutationMu.Unlock()
			_, exists := schema.GetCompositeIndex("admitted")
			require.Equal(t, terminal == "commit", exists)
		})
	}
}

func TestSchemaTransactionAdmissionReleaseBeforeNativeCallbacks(t *testing.T) {
	for _, callback := range []string{"schema_hook", "listener"} {
		t.Run(callback, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			schema := engine.GetSchemaForNamespace("admission")
			called := false
			reenter := func() {
				called = true
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peer, err := schema.BeginSchemaTransaction(ctx, engine.BeginTransaction)
				require.NoError(t, err, "native synchronous callback must run after admission release")
				if peer != nil {
					require.NoError(t, peer.Rollback())
				}
			}
			if callback == "schema_hook" {
				schema.SetKnowledgePolicyChangedHook(reenter)
			} else {
				engine.OnNodeCreated(func(*Node) { reenter() })
			}
			tx, err := schema.BeginSchemaTransaction(context.Background(), engine.BeginTransaction)
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("admission"))
			if callback == "schema_hook" {
				view, err := tx.Schema(OrdinarySchemaChanges)
				require.NoError(t, err)
				require.NoError(t, view.AddPropertyIndex("admitted", "Item", []string{"id"}))
				require.NoError(t, tx.StageSchemaChanges())
			} else {
				_, err = tx.CreateNode(t.Context(), &Node{ID: "admission:listener"})
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			require.True(t, called)
		})
	}
}

// BenchmarkSchemaAdmissionBeginRollback compares the existing native opener
// with pre-snapshot schema admission on an otherwise identical empty statement.
// This isolates admission overhead; it does not measure index backfill work.
func BenchmarkSchemaAdmissionBeginRollback(b *testing.B) {
	engine := newTestMemoryEngine(b)
	const namespace = "admission_bench"
	if err := engine.EnsureNamespaceMVCC(namespace); err != nil {
		b.Fatal(err)
	}
	schema := engine.GetSchemaForNamespace(namespace)
	for _, admitted := range []bool{false, true} {
		name := "native"
		if admitted {
			name = "admitted"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var tx *BadgerTransaction
				var err error
				if admitted {
					tx, err = schema.BeginSchemaTransaction(context.Background(), engine.BeginTransaction)
				} else {
					tx, err = engine.BeginTransaction(b.Context())
				}
				if err != nil {
					b.Fatal(err)
				}
				if err = tx.SetNamespace(namespace); err != nil {
					b.Fatal(err)
				}
				if err = tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSchemaTransactionAdmissionNamespacesIndependent(t *testing.T) {
	engine := newTestMemoryEngine(t)
	first := engine.GetSchemaForNamespace("first")
	release := first.LockSchemaMutation()
	defer release()
	second := engine.GetSchemaForNamespace("second")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := second.BeginSchemaTransaction(ctx, engine.BeginTransaction)
	require.NoError(t, err, "another namespace must not share admission")
	require.NoError(t, tx.SetNamespace("second"))
	require.NoError(t, tx.Rollback())
}
