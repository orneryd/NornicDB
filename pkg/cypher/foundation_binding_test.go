package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestFoundationReconstructedNativeMembership(t *testing.T) {
	base := enginePopulationBase(t)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	owner := NewStorageExecutor(tenant)
	_, err := owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	tx := owner.txContext.tx.(*storage.BadgerTransaction)
	view := owner.transactionStorageFor(tx)
	member := NewStorageExecutor(storage.NewTracedEngine(foundationWAL(t, view)))
	_, err = member.Execute(context.Background(), "CREATE (:FoundationAdded {v:$v})", map[string]interface{}{"v": int64(7)})
	require.NoError(t, err)
	visible, err := tenant.GetNodesByLabel("FoundationAdded")
	require.NoError(t, err)
	require.Empty(t, visible, "reconstruction must not independently commit")
	pending, err := view.GetNodesByLabel("FoundationAdded")
	require.NoError(t, err)
	require.Len(t, pending, 1)
	clone := owner.cloneWithStorage(view)
	_, err = clone.handleCommit(context.Background())
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	require.True(t, tx.IsActive(), "membership does not grant cloned controller authority")
	_, err = owner.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
	visible, err = tenant.GetNodesByLabel("FoundationAdded")
	require.NoError(t, err)
	require.Len(t, visible, 1)
	_, err = member.Execute(context.Background(), "RETURN 1", nil)
	require.ErrorIs(t, err, storage.ErrTransactionClosed, "ended boundary must beat a fresh-context fallback")
}

func TestFoundationCarriedEnforcementAndLease(t *testing.T) {
	base := enginePopulationBase(t)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	provider := &fakeLimitedEngine{Engine: tenant, checker: fakeQueryLimitChecker{limits: fakeMaxResults(2)}}
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	view := &transactionStorageWrapper{tx: tx, underlying: provider, namespace: "tenant", separator: ":"}
	member := NewStorageExecutor(storage.NewTracedEngine(foundationWAL(t, view)))
	for _, limit := range []int{2, 3} {
		provider.checker = fakeQueryLimitChecker{limits: fakeMaxResults(limit)}
		result, err := member.Execute(context.Background(), "UNWIND [1,2,3,4] AS x RETURN x", map[string]interface{}{"nonce": limit})
		require.NoError(t, err)
		require.Len(t, result.Rows, limit, "transaction view must retain the live original database limit provider")
	}
	// Borrow a real physical reveal lease with a fresh executor/context. If the
	// view loses the binding, this nested execution waits on its own owner's lock.
	ctx, release := storage.BeginQueryRevealScopeContext(context.Background(), tenant, false)
	releaseBinding := view.bindReadLease(ctx, func() {})
	defer releaseBinding()
	done := make(chan error, 1)
	go func() { _, err := member.Execute(context.Background(), "RETURN 9", nil); done <- err }()
	select {
	case err := <-done:
		release()
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		release()
		<-done
		t.Fatal("fresh-context execution reacquired its owner's reveal lock")
	}
}

func TestFoundationSchemaPublicationUsesRecoveredAdapter(t *testing.T) {
	base := enginePopulationBase(t)
	tenant := storage.NewNamespacedEngine(base, "tenant")
	owner := NewStorageExecutor(tenant)
	_, err := owner.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	tx := owner.txContext.tx.(*storage.BadgerTransaction)
	view := owner.transactionStorageFor(tx)
	member := NewStorageExecutor(storage.NewTracedEngine(view))
	_, err = member.Execute(context.Background(), "CREATE INDEX foundation_index FOR (n:Foundation) ON (n.v)", nil)
	require.NoError(t, err)
	require.NotNil(t, view.schema)
	require.NotSame(t, tenant.GetSchema(), view.schema, "schema must remain transaction-owned until owner completion")
	_, err = owner.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
	// The independent live schema cannot have acquired the rolled-back index.
	result, err := NewStorageExecutor(tenant).Execute(context.Background(), "SHOW INDEXES YIELD name WHERE name = 'foundation_index' RETURN name", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestFoundationModeBindingReadsLiveOwner(t *testing.T) {
	base := enginePopulationBase(t)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	mode := &resolvedStatementMode{}
	mode.active.Store(true)
	view := &transactionStorageWrapper{tx: tx, underlying: base, statementMode: mode}
	member := NewStorageExecutor(storage.NewTracedEngine(view))
	ctx, err := member.boundExecutionContext(context.Background())
	require.NoError(t, err)
	observed, ok := ctx.Value(resolvedStatementModeKey{}).(*resolvedStatementMode)
	require.True(t, ok)
	require.Same(t, mode, observed)
	mode.write.Store(true)
	require.True(t, observed.write.Load(), "bind the real observer, not a copied write flag")
	mode.active.Store(false)
	ctx, err = member.boundExecutionContext(context.Background())
	require.NoError(t, err)
	require.Nil(t, ctx.Value(resolvedStatementModeKey{}), "a subsequent statement cannot inherit an ended observer")
}
