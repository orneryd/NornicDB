package storage

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Transaction-scoped count-store reads (B10). An explicit transaction reads its
// rows at its own begin boundary, so after a peer commits into its namespace the
// current counters no longer describe them. The counters are persisted per
// snapshot, and these cases pin the contract that lets such a transaction read
// them: the capability answers at its own snapshot, only for the read and engine
// it was issued for, not after the transaction ends, and it declines rather than
// borrowing a newer view.

// transactionCountScopedRequest is one relationship-type count in the tests'
// namespace.
func transactionCountScopedRequest() CountStoreReadRequest {
	return CountStoreReadRequest{Kind: CountStoreRelationshipsByType, Namespace: "test", Types: []string{"REL"}}
}

// TestTransactionCountScopeAnswersAtTheTransactionsSnapshot is the correction
// itself: after a peer's commit moved the namespace past the transaction's
// boundary, the snapshot counter still counts the transaction's rows, and the
// capability proves the native kernel answered from it.
func TestTransactionCountScopeAnswersAtTheTransactionsSnapshot(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	request := transactionCountScopedRequest()
	// Sensitivity control: the committed counter moved with the peer's commit,
	// so the current counters no longer count this transaction's rows.
	live, err := engine.ReadCountStore(ctx, request)
	require.NoError(t, err)
	require.True(t, live.DescribesRows)
	require.EqualValues(t, 2, live.Count)

	scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described, "a pinned transaction plans its own snapshot read")
	require.Nil(t, request.Scope, "the caller's request is not mutated")
	require.NotNil(t, scoped.Scope)

	answer, err := engine.ReadCountStore(ctx, scoped)
	require.NoError(t, err)
	require.True(t, answer.DescribesRows, "the snapshot counter describes the rows this transaction reads")
	require.EqualValues(t, 1, answer.Count, "the counter as of the transaction's begin boundary")
	require.True(t, scoped.Scope.DescribesSnapshot(),
		"the native counter kernel consumed the transaction's capability")
}

// TestTransactionCountScopeDeclinesAnotherReadOrNamespace keeps the capability
// bound to the one read and engine it was issued for.
func TestTransactionCountScopeDeclinesAnotherReadOrNamespace(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	label := CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L"}
	for _, request := range []CountStoreReadRequest{
		{Kind: CountStoreNodesByLabel, Namespace: "other", Name: "L"},
		{Kind: CountStoreNodesByLabel, Namespace: "", Name: "L"},
		{Kind: CountStoreNodesByLabel, Name: "L"},
	} {
		_, described, err := tx.ScopeCountStoreRead(ctx, request)
		require.NoError(t, err)
		require.False(t, described, "only the transaction's own scope is answerable at its snapshot")
	}

	// A retained copy whose read was changed is not the read the capability was
	// issued for.
	changedPlan, described, err := tx.ScopeCountStoreRead(ctx, label)
	require.NoError(t, err)
	require.True(t, described)
	changed := changedPlan
	changed.Name = "Other"
	answer, err := engine.ReadCountStore(ctx, changed)
	require.NoError(t, err)
	require.False(t, answer.DescribesRows, "a changed read is not covered by the transaction's snapshot")
	require.False(t, changed.Scope.DescribesSnapshot())

	// Nor is a read answered by another engine.
	otherEnginePlan, described, err := tx.ScopeCountStoreRead(ctx, label)
	require.NoError(t, err)
	require.True(t, described)
	other := newIsolatedBadgerEngine(t)
	answer, err = other.ReadCountStore(ctx, otherEnginePlan)
	require.NoError(t, err)
	require.False(t, answer.DescribesRows, "a capability is valid only for its originating engine")
	require.False(t, otherEnginePlan.Scope.DescribesSnapshot())
}

// TestTransactionCountScopeDoesNotOutliveItsTransaction is the retention
// contract: a request retained past the end of its transaction declines instead
// of reaching the transaction's disposed view.
func TestTransactionCountScopeDoesNotOutliveItsTransaction(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)

	for _, ending := range []struct {
		name string
		end  func(*BadgerTransaction) error
	}{
		{name: "rollback", end: func(tx *BadgerTransaction) error { return tx.Rollback() }},
		{name: "commit", end: func(tx *BadgerTransaction) error { return tx.Commit() }},
	} {
		t.Run(ending.name, func(t *testing.T) {
			tx := beginPinnedTransaction(t, engine)
			scoped, described, err := tx.ScopeCountStoreRead(ctx, CountStoreReadRequest{
				Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L",
			})
			require.NoError(t, err)
			require.True(t, described)
			require.NoError(t, ending.end(tx))

			answer, err := engine.ReadCountStore(ctx, scoped)
			require.NoError(t, err, "a retained capability declines; it does not read a disposed view")
			require.False(t, answer.DescribesRows)
			require.False(t, scoped.Scope.DescribesSnapshot())
		})
	}
}

// TestTransactionCountScopeDeclinesAMalformedSnapshotCounterWithoutRepair keeps
// the error contract: a malformed persisted count has no repair at a pinned
// snapshot, so the read declines instead of rebuilding the family from a newer
// view, and the persisted value it could not decode stays as it is.
func TestTransactionCountScopeDeclinesAMalformedSnapshotCounterWithoutRepair(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	malformed := make([]byte, 4)
	binary.BigEndian.PutUint32(malformed, 99)
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(labelCountKey("test", "L"), malformed)
	}))

	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	request := CountStoreReadRequest{Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L"}
	scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described)

	answer, err := engine.ReadCountStore(ctx, scoped)
	require.NoError(t, err, "a malformed pinned counter declines rather than borrowing a newer view")
	require.False(t, answer.DescribesRows)
	require.False(t, scoped.Scope.DescribesSnapshot())
	require.Equal(t, malformed, transactionCountRawDerivedCount(t, engine, labelCountKey("test", "L")),
		"the pinned read must not repair the family from the current state")

	// The current-view read keeps its own contract: it repairs the family from
	// the current state and answers. (That repair rewrites the base counters and
	// leaves the family's unfolded deltas in place, so the count it returns is
	// outside this case's contract; the pinned read must simply not take it.)
	live, err := engine.ReadCountStore(ctx, request)
	require.NoError(t, err)
	require.True(t, live.DescribesRows)
	require.NotZero(t, live.Count)
}

// transactionCountRawDerivedCount reads one persisted derived-count value as it
// is stored.
func transactionCountRawDerivedCount(t *testing.T, engine *BadgerEngine, key []byte) []byte {
	t.Helper()
	var value []byte
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(stored []byte) error {
			value = append([]byte(nil), stored...)
			return nil
		})
	}))
	return value
}

// TestTransactionCountScopeKeepsScalarCountsWhileAPeerPublicationIsInFlight is
// the positional cut proof. A commit writes its scalar label and edge-type
// deltas atomically with its rows but publishes its positional (label, type)
// deltas afterwards, so a transaction that begins inside that window reads rows
// whose positional counters its snapshot does not hold. The scalar counters
// describe such a snapshot; the positional ones decline.
func TestTransactionCountScopeKeepsScalarCountsWhileAPeerPublicationIsInFlight(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)

	publishing := make(chan struct{})
	resume := make(chan struct{})
	var release sync.Once
	releasePublication := func() { release.Do(func() { close(resume) }) }
	defer releasePublication()
	var once sync.Once
	restore := setCommitTailHook(func() {
		first := false
		once.Do(func() { first = true })
		if first {
			close(publishing)
			<-resume
		}
	})
	defer restore()

	// The peer's commit lands its rows, blocks before its positional counters,
	// and keeps its version in the ledger.
	peer, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, peer.SetNamespace("test"))
	_, err = peer.CreateNode(ctx, &Node{ID: "test:peer", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, peer.CreateEdge(ctx, &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
	peerCommitted := make(chan error, 1)
	go func() { peerCommitted <- peer.Commit() }()
	<-publishing

	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err, "a cut is taken while the peer's publication is in flight")
	require.NoError(t, tx.SetNamespace("test"))
	require.NotNil(t, tx.inflightAtBegin, "the cut names the version the peer has not retired")

	// The scalar label counter was published with the peer's rows, so it
	// describes this snapshot.
	labelPlan, described, err := tx.ScopeCountStoreRead(ctx, CountStoreReadRequest{
		Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L", HiddenLabelPrefix: "_",
	})
	require.NoError(t, err)
	require.True(t, described, "a scalar counter needs no positional publication")
	labelAnswer, err := engine.ReadCountStore(ctx, labelPlan)
	require.NoError(t, err)
	require.True(t, labelAnswer.DescribesRows)
	require.EqualValues(t, 3, labelAnswer.Count, "the two seeded nodes and the peer's own")
	nodes, err := tx.GetNodesByLabel(ctx, "L")
	require.NoError(t, err)
	require.Len(t, nodes, 3, "the snapshot reads the rows its scalar counter counted")

	// A positional counter is published after its rows, so a cut that holds an
	// unpublished version cannot prove the snapshot's positional counters.
	for _, request := range []CountStoreReadRequest{
		{Kind: CountStoreRelationshipsByStartLabel, Namespace: "test", Name: "L", Types: []string{"REL"}, HiddenLabelPrefix: "_"},
		{Kind: CountStoreRelationshipsByEndLabel, Namespace: "test", Name: "L", Types: []string{"REL"}, HiddenLabelPrefix: "_"},
		{Kind: CountStoreRelationshipsByType, Namespace: "test", Types: []string{"REL"}, HiddenLabelPrefix: "_"},
	} {
		_, described, err := tx.ScopeCountStoreRead(ctx, request)
		require.NoError(t, err)
		require.False(t, described,
			"%d reads positional counters, which this cut cannot prove published", request.Kind)
	}

	releasePublication()
	require.NoError(t, <-peerCommitted, "the peer's commit completes")
	require.NoError(t, tx.Rollback())
}

// TestTransactionCountScopeDeclinesWriteBehindReads keeps the write-behind
// contract: those reads are read-committed, so the live counters describe them
// and a pinned snapshot does not.
func TestTransactionCountScopeDeclinesWriteBehindReads(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ctx := t.Context()
	createEdgeEndpoints(t, engine)

	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, described, err := tx.ScopeCountStoreRead(ctx, CountStoreReadRequest{
		Kind: CountStoreNodesByLabel, Namespace: "test", Name: "L",
	})
	require.NoError(t, err)
	require.False(t, described, "a write-behind transaction reads the buffer's read-committed view")
}
