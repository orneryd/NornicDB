package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Transaction-scoped count-store reads, consumption half (B10). A capability is
// issued for one read at one cut of one transaction, and the native counter
// kernel composes its answer: the counter at that cut plus the staged
// contribution the transaction holds when the capability is consumed. These
// cases pin that composition (including a reader that stages a write and then
// forwards the request it was handed), the decline for a contribution the count
// cannot express, the stale-cut refusal, and the transaction synchronization the
// answer depends on.

// transactionCountScopedLabelRequest is one node-label count in the tests'
// namespace.
func transactionCountScopedLabelRequest() CountStoreReadRequest {
	return CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L"}
}

// TestTransactionCountScopeComposesStagedContributionAtConsumption is the
// reentrant staging contract: a reader may stage a write into the transaction
// and then forward the very request it was handed, unchanged. The answer is the
// counter at the pinned snapshot plus the contribution the transaction holds
// when the native kernel consumes the capability, so the count still equals the
// rows the transaction reads.
func TestTransactionCountScopeComposesStagedContributionAtConsumption(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	request := transactionCountScopedLabelRequest()
	scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described)
	require.EqualValues(t, 2, transactionCountScopedCounter(t, engine, request),
		"the seeded nodes are the counter at the transaction's cut")

	// The reader stages a row of the counted label after planning and forwards
	// the request as it received it.
	_, err = tx.CreateNode(ctx, &Node{ID: "test:late", Labels: []string{"L"}})
	require.NoError(t, err)

	answer, err := engine.ReadCountStore(ctx, scoped)
	require.NoError(t, err)
	require.True(t, answer.DescribesRows)
	require.True(t, scoped.Scope.DescribesSnapshot(),
		"the native counter kernel consumed the transaction's capability")
	require.EqualValues(t, 3, answer.Count,
		"the two committed nodes and the row the forwarding reader staged")

	nodes, err := tx.GetNodesByLabel(ctx, "L")
	require.NoError(t, err)
	require.Len(t, nodes, 3, "the count and the rows this transaction reads agree")
}

// TestTransactionCountScopeDeclinesAStagedContributionItCannotExpress is the
// negative half of the same composition: a staged change that can move a
// positional (label, type) count is derived only at commit, so the capability
// declines and the caller counts the rows it reads.
func TestTransactionCountScopeDeclinesAStagedContributionItCannotExpress(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	request := CountStoreReadRequest{
		Kind: CountStoreRelationshipsByStartLabel, Namespace: "test", Name: "L", Types: []string{"REL"},
	}
	scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described, "the cut holds no unpublished version, so the read can be planned")

	require.NoError(t, tx.CreateEdge(ctx, &Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))

	answer, err := engine.ReadCountStore(ctx, scoped)
	require.NoError(t, err)
	require.False(t, answer.DescribesRows, "a staged positional change leaves the count to the rows")
	require.False(t, scoped.Scope.DescribesSnapshot())

	edges, err := tx.GetEdgesByType(ctx, "REL")
	require.NoError(t, err)
	require.Len(t, edges, 1, "the rows the transaction reads hold the staged relationship")
}

// TestTransactionCountScopeDeclinesACutItsTransactionLeft is the stale-cut
// contract: a capability names one snapshot, and a transaction that continues
// onto a newer one must not certify the capability's read from it. A capability
// planned after the continuation answers from the snapshot the transaction now
// reads.
func TestTransactionCountScopeDeclinesACutItsTransactionLeft(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))

	request := transactionCountScopedLabelRequest()
	stale, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described)

	// A peer publishes a node of the counted label, and the implicit caller
	// continues onto a snapshot that holds it.
	peer, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	_, err = peer.CreateNode(ctx, &Node{ID: "test:peer", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, peer.Commit())
	require.NoError(t, tx.ResumeAfterIndependentCommit(ctx))

	answer, err := engine.ReadCountStore(ctx, stale)
	require.NoError(t, err, "a capability whose cut is gone declines; it reads no other snapshot")
	require.False(t, answer.DescribesRows, "the newer physical snapshot does not certify the older cut")
	require.False(t, stale.Scope.DescribesSnapshot())

	fresh, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described)
	answer, err = engine.ReadCountStore(ctx, fresh)
	require.NoError(t, err)
	require.True(t, answer.DescribesRows)
	require.True(t, fresh.Scope.DescribesSnapshot())
	require.EqualValues(t, 3, answer.Count, "the two seeded nodes and the peer's")
	nodes, err := tx.GetNodesByLabel(ctx, "L")
	require.NoError(t, err)
	require.Len(t, nodes, 3)
}

// TestTransactionCountScopeConcurrentConsumptionIsRaceFree exercises the
// transaction synchronization consumption depends on: the same transaction is
// read from several goroutines while another goroutine stages writes into it.
// Every consumed answer is one instant's counter plus the contribution held
// then, so each counts at least the snapshot's rows and at most every staged
// row, and the read planned once the staging is done counts them all.
func TestTransactionCountScopeConcurrentConsumptionIsRaceFree(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := context.Background()
	createEdgeEndpoints(t, engine)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	request := transactionCountScopedLabelRequest()
	const (
		readers = 8
		staged  = 4
	)
	scopes := make([]CountStoreReadRequest, readers)
	for index := range scopes {
		scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
		require.NoError(t, err)
		require.True(t, described)
		scopes[index] = scoped
	}

	answers := make([]CountStoreRead, readers)
	errs := make([]error, readers)
	stagedErr := make(chan error, 1)
	var wait sync.WaitGroup
	start := make(chan struct{})
	for index := range scopes {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			answers[index], errs[index] = engine.ReadCountStore(ctx, scopes[index])
		}(index)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for index := 0; index < staged; index++ {
			if _, err := tx.CreateNode(ctx, &Node{
				ID: NodeID(fmt.Sprintf("test:staged-%d", index)), Labels: []string{"L"},
			}); err != nil {
				stagedErr <- err
				return
			}
		}
		stagedErr <- nil
	}()
	close(start)
	wait.Wait()
	require.NoError(t, <-stagedErr, "the transaction stages its writes")

	for index, answer := range answers {
		require.NoError(t, errs[index])
		require.True(t, answer.DescribesRows, "a scoped read of a label count is expressible at every instant")
		require.True(t, scopes[index].Scope.DescribesSnapshot())
		require.GreaterOrEqual(t, answer.Count, int64(2),
			"the snapshot's two committed nodes are in every answer")
		require.LessOrEqual(t, answer.Count, int64(2+staged),
			"no answer counts a row the transaction had not staged")
	}

	// Staging is idle now, so the capability counts every row the transaction
	// reads, and the listing agrees.
	last, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described)
	answer, err := engine.ReadCountStore(ctx, last)
	require.NoError(t, err)
	require.True(t, answer.DescribesRows)
	require.EqualValues(t, 2+staged, answer.Count)
	nodes, err := tx.GetNodesByLabel(ctx, "L")
	require.NoError(t, err)
	require.Len(t, nodes, 2+staged)
}

// transactionCountScopedCounter reads request outside any transaction, which is
// the committed counter the tests use as their sensitivity control.
func transactionCountScopedCounter(t *testing.T, engine *BadgerEngine, request CountStoreReadRequest) int64 {
	t.Helper()
	answer, err := engine.ReadCountStore(t.Context(), request)
	require.NoError(t, err)
	require.True(t, answer.DescribesRows)
	return answer.Count
}
