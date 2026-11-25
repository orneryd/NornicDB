package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNamespaceMVCCRecoveredBeforeFirstTransactionAfterReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	for _, namespace := range []string{"alpha", "beta"} {
		_, err := engine.CreateNode(context.Background(), &Node{ID: NodeID(namespace + ":node"), Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	defer reopened.Close()
	for _, namespace := range []string{"alpha", "beta"} {
		reopened.mvccByNamespaceMu.RLock()
		state := reopened.mvccByNamespace[namespace]
		reopened.mvccByNamespaceMu.RUnlock()
		require.NotNil(t, state, "existing namespace must be ready before first transaction")
		require.Positive(t, state.seq.Load())
		require.Positive(t, state.highWaterNanos.Load())

		tx, err := reopened.BeginTransaction(context.Background(), OrdinaryBegin)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace(namespace))
		_, err = tx.GetNode(context.Background(), NodeID(namespace+":node"))
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
	}
}

func TestNamespaceMVCCConcurrentColdCallersShareState(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer engine.Close()
	const callers = 24
	states := make([]*namespaceMVCCState, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range states {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			states[i], errs[i] = engine.namespaceMVCC("cold")
		}()
	}
	close(start)
	wg.Wait()
	for i := range states {
		require.NoError(t, errs[i])
		require.Same(t, states[0], states[i])
	}
}

func TestNamespaceMVCCColdLoadFailureCanRetry(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer engine.Close()
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(mvccNamespaceSequenceKey("cold"), []byte{1})
	}))
	_, err = engine.namespaceMVCC("cold")
	require.ErrorContains(t, err, "invalid mvcc sequence length")

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(mvccNamespaceSequenceKey("cold"))
	}))
	state, err := engine.namespaceMVCC("cold")
	require.NoError(t, err)
	require.NotNil(t, state)
}
