package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// heldNativeIncidentLister is a test-only read boundary. It delegates every
// storage call to the real transaction wrapper and parks its first incidence
// read in read admission: it waits for the context the DELETE hands to storage,
// or for the test to release the boundary. A guard that read incidence without
// the request's context cannot end that read here.
type heldNativeIncidentLister struct {
	storage.Engine // the real transactionStorageWrapper
	entered        chan struct{}
	release        chan struct{}
	reads          atomic.Int32
}

func (e *heldNativeIncidentLister) IncidentEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	if e.reads.Add(1) == 1 {
		close(e.entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-e.release:
		}
	}
	native, ok := e.Engine.(storage.IncidentEdgeLister)
	if !ok {
		return nil, fmt.Errorf("the held incidence boundary does not wrap a storage.IncidentEdgeLister")
	}
	return native.IncidentEdges(ctx, nodeID)
}

func onceCloser(ch chan struct{}) func() {
	var closed atomic.Bool
	return func() {
		if closed.CompareAndSwap(false, true) {
			close(ch)
		}
	}
}

// probeStatementDelete runs one statement-body DELETE (pipelineApplyDelete, the
// body every parsed DELETE reaches) and holds its incidence read: it waits until
// the body entered the boundary or finished, cancels the caller, releases the
// boundary and joins the body. A single cleanup owns cancel, release and join,
// so an entry timeout leaves no goroutine behind and the probe never sleeps.
func probeStatementDelete(t *testing.T, exec *StorageExecutor, clause string, rows []pipelineRow, scope map[string]struct{}, entered <-chan struct{}, release func(), reads *atomic.Int32) (stoppedOnCancel bool, reachedBoundary bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_, _, err := exec.pipelineApplyDelete(ctx, rows, scope, clause)
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			t.Error("the canceled statement DELETE probe did not join after its incidence boundary was released")
		}
	})

	var err error
	select {
	case <-entered:
		reachedBoundary = true
	case err = <-done:
		// Finished without reading incidence through the boundary under test.
		require.NoError(t, err, "the statement DELETE finished before reading incidence (%d boundary reads)", reads.Load())
		return false, false
	}
	cancel()
	select {
	case err = <-done:
		stoppedOnCancel = true
	case <-time.After(2 * time.Second):
	}
	release()
	if !stoppedOnCancel {
		select {
		case err = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the statement DELETE did not return after its held incidence read was released")
		}
	}
	<-joined
	require.ErrorIs(t, err, context.Canceled)
	return stoppedOnCancel, reachedBoundary
}

// TestStatementDeleteStagesNothingOnCanceledNativeIncidentRead pins the two
// reads the DELETE guard owns, at a held native incidence boundary: the
// non-DETACH guard stages nothing when the caller cancels inside it, and a
// DETACH DELETE that already staged its relationship deletes rolls them back
// when the caller cancels inside its incidence count. Both continue to work for
// a normal caller through the real native implementation.
func TestStatementDeleteStagesNothingOnCanceledNativeIncidentRead(t *testing.T) {
	const namespace = "delete_statement_context"

	counts := func(t *testing.T, store storage.Engine) (nodes, relationships int64) {
		t.Helper()
		exec := NewStorageExecutor(store)
		matched, err := exec.Execute(t.Context(), "MATCH (n) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Len(t, matched.Rows, 1)
		rels, err := exec.Execute(t.Context(), "MATCH ()-[r]->() RETURN count(r)", nil)
		require.NoError(t, err)
		require.Len(t, rels.Rows, 1)
		return matched.Rows[0][0].(int64), rels.Rows[0][0].(int64)
	}
	newFixture := func(t *testing.T) (base *storage.MemoryEngine, store storage.Engine, rows []pipelineRow) {
		t.Helper()
		base = newTestMemoryEngine(t)
		store = storage.NewNamespacedEngine(base, namespace)
		exec := NewStorageExecutor(store)
		_, err := exec.Execute(t.Context(), "CREATE (:Guarded {id: 'a'})-[:R]->(:Leaf {id: 'b'})", nil)
		require.NoError(t, err)
		matched, err := exec.Execute(t.Context(), "MATCH (a:Guarded {id: 'a'})-[r:R]->(b:Leaf {id: 'b'}) RETURN a, r, b", nil)
		require.NoError(t, err)
		require.Len(t, matched.Rows, 1)
		return base, store, []pipelineRow{{"a": matched.Rows[0][0], "r": matched.Rows[0][1]}}
	}
	scope := map[string]struct{}{"a": {}, "r": {}}
	begin := func(t *testing.T, base *storage.MemoryEngine, store storage.Engine) (*storage.BadgerTransaction, *transactionStorageWrapper, *heldNativeIncidentLister, *StorageExecutor) {
		t.Helper()
		require.NoError(t, base.EnsureNamespaceMVCC(namespace))
		tx, err := base.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace(namespace))
		txStore := &transactionStorageWrapper{
			tx:             tx,
			underlying:     store,
			namespace:      namespace,
			separator:      ":",
			mutatedNodeIDs: make(map[string]struct{}),
		}
		require.Implements(t, (*storage.IncidentEdgeLister)(nil), txStore, "the wrapped target is the real native incidence reader")
		held := &heldNativeIncidentLister{Engine: txStore, entered: make(chan struct{}), release: make(chan struct{})}
		return tx, txStore, held, NewStorageExecutor(held)
	}

	t.Run("guard", func(t *testing.T) {
		base, store, rows := newFixture(t)
		tx, txStore, held, exec := begin(t, base, store)
		stopped, reached := probeStatementDelete(t, exec, "DELETE a, r", rows, scope, held.entered, onceCloser(held.release), &held.reads)
		require.True(t, reached, "the guard must read incidence through the caller-context interface")
		require.True(t, stopped, "the guard's incidence read ignored the caller's cancellation")
		require.Zero(t, tx.OperationCount(), "a canceled guard stages nothing")
		nodes, relationships := counts(t, store)
		require.Equal(t, int64(2), nodes, "a canceled DELETE deletes nothing")
		require.Equal(t, int64(1), relationships)

		// The same statement body and transaction stay healthy for a caller.
		stats, _, err := NewStorageExecutor(txStore).pipelineApplyDelete(context.Background(), rows, scope, "DELETE a, r")
		require.NoError(t, err)
		require.Equal(t, 1, stats.NodesDeleted)
		require.Equal(t, 1, stats.RelationshipsDeleted)
		require.NoError(t, tx.Commit())
		nodes, relationships = counts(t, store)
		require.Equal(t, int64(1), nodes, "the healthy DELETE removed its node")
		require.Zero(t, relationships)
	})

	t.Run("detach-count", func(t *testing.T) {
		base, store, rows := newFixture(t)
		tx, _, held, exec := begin(t, base, store)
		stopped, reached := probeStatementDelete(t, exec, "DETACH DELETE a, r", rows, scope, held.entered, onceCloser(held.release), &held.reads)
		require.True(t, reached, "the DETACH DELETE count must read incidence through the caller-context interface")
		require.True(t, stopped, "the DETACH DELETE incidence count ignored the caller's cancellation")
		require.Positive(t, tx.OperationCount(), "the statement had already staged the relationship delete")
		// All-or-none: native rollback discards the staged deletes.
		require.NoError(t, tx.Rollback())
		nodes, relationships := counts(t, store)
		require.Equal(t, int64(2), nodes, "a canceled DETACH DELETE deletes nothing")
		require.Equal(t, int64(1), relationships, "a canceled DETACH DELETE keeps every relationship")

		// The same statement body stays healthy on a fresh transaction.
		tx2, txStore2, _, _ := begin(t, base, store)
		stats, _, err := NewStorageExecutor(txStore2).pipelineApplyDelete(context.Background(), rows, scope, "DETACH DELETE a, r")
		require.NoError(t, err)
		require.Equal(t, 1, stats.NodesDeleted)
		require.Equal(t, 1, stats.RelationshipsDeleted)
		require.NoError(t, tx2.Commit())
		nodes, relationships = counts(t, store)
		require.Equal(t, int64(1), nodes)
		require.Zero(t, relationships)
	})
}
