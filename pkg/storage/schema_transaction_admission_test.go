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
	tx, err := schema.BeginSchemaTransaction(ctx, func(beginCtx context.Context, begin BeginRequest) (*BadgerTransaction, error) {
		began = true
		return engine.BeginTransaction(beginCtx, begin)
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

// TestSchemaTransactionAdmissionPassesSchemaPurpose pins the one requester of
// an autocommit schema begin: the manager's opener receives the schema purpose,
// so the native begin admits the statement before its snapshot, and the
// statement's own transaction owns that exclusive lease.
func TestSchemaTransactionAdmissionPassesSchemaPurpose(t *testing.T) {
	engine := newTestMemoryEngine(t)
	schema := engine.GetSchemaForNamespace("admission")
	request := OrdinaryBegin
	tx, err := schema.BeginSchemaTransaction(context.Background(), func(ctx context.Context, begin BeginRequest) (*BadgerTransaction, error) {
		request = begin
		return engine.BeginTransaction(ctx, begin)
	})
	require.NoError(t, err)
	require.Equal(t, AutocommitSchemaBegin, request, "the autocommit opener must request a schema begin")
	if tx.commitLease == nil || !tx.commitLease.exclusive {
		t.Fatal("a schema begin must own the engine's exclusive lifecycle lease")
	}
	require.NoError(t, tx.Rollback())
	require.Nil(t, tx.commitLease)
	require.True(t, schema.schemaMutationMu.TryLock())
	schema.schemaMutationMu.Unlock()
	require.True(t, engine.writeBarrier.TryLock(), "rollback must release the begin's exclusive lease")
	engine.writeBarrier.Unlock()
}

func TestSchemaTransactionAdmissionBeginFailureReleases(t *testing.T) {
	schema := NewSchemaManager()
	failure := errors.New("begin rejected")
	tx, err := schema.BeginSchemaTransaction(context.Background(), func(context.Context, BeginRequest) (*BadgerTransaction, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	require.Nil(t, tx)
	require.True(t, schema.schemaMutationMu.TryLock())
	schema.schemaMutationMu.Unlock()
}

// TestSchemaTransactionAdmissionTerminalRelease pins the statement's single
// release owner for every terminal outcome: publication, rollback and a rejected
// commit each end its manager admission and its exclusive lifecycle lease once,
// and a repeated close cannot release either on a peer's behalf.
//
// The rejected commit waits for a peer schema publication's definition gate and
// is canceled there. A concurrent entity commit is not that terminal any more:
// an autocommit schema statement's admission excludes data commits for its whole
// window, so a peer write is admitted only after its publication
// (schema_ddl_data_writer_test.go).
func TestSchemaTransactionAdmissionTerminalRelease(t *testing.T) {
	for _, terminal := range []string{"commit", "rollback", "rejected_commit"} {
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
			case "rejected_commit":
				peer, err := schema.beginSchemaPublication(context.Background())
				require.NoError(t, err)
				commitCtx, cancelCommit := context.WithCancel(context.Background())
				defer cancelCommit()
				restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
					if owner == &schema.publicationGate {
						cancelCommit()
					}
				})
				defer restore()
				require.ErrorIs(t, tx.CommitContext(commitCtx), context.Canceled)
				peer.release()
			}
			require.Nil(t, tx.schemaAdmission)
			require.True(t, schema.schemaMutationMu.TryLock())
			schema.schemaMutationMu.Unlock()
			require.ErrorIs(t, tx.Rollback(), ErrTransactionClosed)
			require.True(t, schema.schemaMutationMu.TryLock(), "repeated close cannot release a peer's lease")
			schema.schemaMutationMu.Unlock()
			require.True(t, engine.writeBarrier.TryLock(), "a terminal outcome must release the begin's exclusive lease once")
			engine.writeBarrier.Unlock()
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
					tx, err = engine.BeginTransaction(b.Context(), OrdinaryBegin)
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
