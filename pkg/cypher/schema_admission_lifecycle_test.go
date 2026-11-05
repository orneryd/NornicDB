package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type ddlAdmissionCommitOwner struct {
	*storage.NamespacedEngine
	base    *storage.MemoryEngine
	opened  *storage.BadgerTransaction
	commits int
	reject  error
}

func (e *ddlAdmissionCommitOwner) BeginTransaction(ctx context.Context) (*storage.BadgerTransaction, error) {
	tx, err := e.base.BeginTransaction(ctx)
	e.opened = tx
	return tx, err
}
func (e *ddlAdmissionCommitOwner) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	e.commits++
	if e.reject != nil {
		return e.reject
	}
	return tx.CommitContext(ctx)
}

func TestSchemaAdmissionManagedRejectionRollsBack(t *testing.T) {
	base := newTestMemoryEngine(t)
	rejected := errors.New("managed commit rejected before native commit")
	engine := &ddlAdmissionCommitOwner{NamespacedEngine: storage.NewNamespacedEngine(base, "admission"), base: base, reject: rejected}
	exec := NewStorageExecutor(engine)
	_, err := exec.Execute(context.Background(), "CREATE INDEX admitted FOR (n:Item) ON (n.id)", nil)
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, engine.commits)
	require.Equal(t, storage.TxStatusRolledBack, engine.opened.Status)
	require.False(t, ddlPublishedCompositeIndex(engine.GetSchema(), "admitted"))
	engine.reject = nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = exec.Execute(ctx, "CREATE INDEX admitted FOR (n:Item) ON (n.id)", nil)
	require.NoError(t, err)
	require.Equal(t, 2, engine.commits, "retry still uses the custom managed owner")
	require.True(t, ddlPublishedCompositeIndex(engine.GetSchema(), "admitted"))
}

func TestSchemaAdmissionPrecommitCancellationRollsBack(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := &ddlAdmissionCommitOwner{NamespacedEngine: storage.NewNamespacedEngine(base, "admission"), base: base}
	exec := NewStorageExecutor(engine)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	query := "CREATE INDEX admitted FOR (n:Item) ON (n.id)"
	_, err := exec.executeWithImplicitTransactionCallback(ctx, query, upperASCII(query), func(txCtx context.Context, txExec *StorageExecutor) (*ExecuteResult, error) {
		result, err := txExec.executeWithoutTransaction(txCtx, query, upperASCII(query))
		cancel()
		return result, err
	})
	require.Error(t, err)
	require.Zero(t, engine.commits, "canceled request rejects before the custom/native commit")
	require.Equal(t, storage.TxStatusRolledBack, engine.opened.Status)
	require.False(t, ddlPublishedCompositeIndex(engine.GetSchema(), "admitted"))
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	_, err = exec.Execute(retryCtx, query, nil)
	require.NoError(t, err)
	require.Equal(t, 1, engine.commits)
}

func TestSchemaAdmissionSchemaHookReentersPublicDDL(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "admission")
	exec := NewStorageExecutor(store)
	reentered := false
	store.GetSchema().SetKnowledgePolicyChangedHook(func() {
		if reentered {
			return
		}
		reentered = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := exec.Execute(ctx, "CREATE INDEX nested FOR (n:Nested) ON (n.id)", nil)
		require.NoError(t, err)
	})
	_, err := exec.Execute(context.Background(), "CREATE INDEX outer_index FOR (n:Outer) ON (n.id)", nil)
	require.NoError(t, err)
	require.True(t, reentered)
	require.True(t, ddlPublishedCompositeIndex(store.GetSchema(), "outer_index"))
	require.True(t, ddlPublishedCompositeIndex(store.GetSchema(), "nested"))
}
