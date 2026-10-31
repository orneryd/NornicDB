package storage

import (
	"expvar"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestPendingCandidatesVerifiedEmptyAvoidsRepeatedScans(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	metric := expvar.Get("badger_iterator_num_user").(*expvar.Int)
	before := metric.Value()
	for i := 0; i < 32; i++ {
		require.Nil(t, e.FindNodeNeedingEmbedding())
	}
	require.Zero(t, metric.Value()-before, "a verified empty published snapshot needs no repeated index scan")

	// A non-marker commit also invalidates the empty snapshot. Reconcile it
	// once, then reuse the new verified snapshot without retaining storage.
	require.NoError(t, e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set([]byte("empty-snapshot-probe"), []byte("published"))
	}))
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.EqualValues(t, 1, metric.Value()-before)
	for i := 0; i < 32; i++ {
		require.Nil(t, e.FindNodeNeedingEmbedding())
	}
	require.EqualValues(t, 1, metric.Value()-before)
	require.Zero(t, cap(e.pendingCandidates.ids))
	require.Nil(t, e.pendingCandidates.members)
}

// Fault injection: lose the volatile hint, retaining the exact
// committed pending marker. A pure cache must not make that marker invisible.
func TestPendingCandidatesReviewLostHint(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	e.MarkNodeEmbedded(pendingCandidateNode(0).ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.NoError(t, e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		if err := e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(0).ID); err != nil {
			return err
		}
		e.pendingEmbeddingAdds.Delete(txn) // lose only the volatile hint
		return nil
	}))
	require.Equal(t, 1, e.PendingEmbeddingsCount())
	misses := 0
	for i := 0; i < 32; i++ {
		if e.FindNodeNeedingEmbedding() == nil {
			misses++
		}
	}
	t.Logf("REVIEW_LOST_HINT durable=1 consecutive_misses=%d loaded=%t heap=%d", misses, e.pendingCandidates.loaded, len(e.pendingCandidates.ids))
	if misses == 32 {
		t.Error("durable pending node remains invisible after a lost hint")
	}
	e.InvalidatePendingEmbeddingsIndex()
	require.NotNil(t, e.FindNodeNeedingEmbedding(), "explicit invalidation alone recovers the marker")
}

func TestPendingCandidatesLostHintAfterDrain(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	for i := 0; i < 3; i++ {
		_, err := e.CreateNode(pendingCandidateNode(i))
		require.NoError(t, err)
	}
	e.MarkNodeEmbedded(pendingCandidateNode(2).ID)
	require.Equal(t, pendingCandidateNode(0).ID, e.FindNodeNeedingEmbedding().ID)
	require.NoError(t, e.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		if err := e.setPendingEmbeddingInTxn(txn, pendingCandidateNode(2).ID); err != nil {
			return err
		}
		e.pendingEmbeddingAdds.Delete(txn)
		return nil
	}))
	for i := 0; i < 3; i++ {
		node := e.FindNodeNeedingEmbedding()
		require.NotNil(t, node, "exhausting stale hints must reconcile before reporting an empty queue")
		require.Equal(t, pendingCandidateNode(i).ID, node.ID)
		e.MarkNodeEmbedded(node.ID)
	}
	require.Nil(t, e.FindNodeNeedingEmbedding())
}

func TestPendingCandidatesDropPreservesOtherNamespace(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	other := &Node{ID: "other:document", Labels: []string{"Doc"}, Properties: map[string]any{"content": "other namespace"}}
	_, err = e.CreateNode(other)
	require.NoError(t, err)
	require.NotNil(t, e.FindNodeNeedingEmbedding())
	generation := e.pendingCandidatesGeneration.Load()
	_, _, err = e.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.Greater(t, e.pendingCandidatesGeneration.Load(), generation)
	require.Equal(t, 1, e.PendingEmbeddingsCount())
	node := e.FindNodeNeedingEmbedding()
	require.NotNil(t, node)
	require.Equal(t, other.ID, node.ID)
	e.MarkNodeEmbedded(node.ID)
	require.Nil(t, e.FindNodeNeedingEmbedding())
}

func TestPendingCandidatesReviewDrainReleasesStorage(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	const n = 4096
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = pendingCandidateNode(i)
	}
	require.NoError(t, e.BulkCreateNodes(nodes))
	for i := 0; i < n; i++ {
		node := e.FindNodeNeedingEmbedding()
		require.NotNil(t, node)
		e.MarkNodeEmbedded(node.ID)
	}
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.Zero(t, e.PendingEmbeddingsCount())
	p := &e.pendingCandidates
	p.mu.Lock()
	retained, membersRetained := cap(p.ids), p.members != nil
	t.Logf("REVIEW_DRAIN durable=0 heap_len=%d heap_cap=%d heap_slot_bytes=%d members_len=%d members_retained=%t", len(p.ids), retained, retained*16, len(p.members), membersRetained)
	p.mu.Unlock()
	if retained != 0 || membersRetained {
		t.Error("drained candidate cache retains peak heap and map storage")
	}
}

func TestPendingCandidatesReviewDropInvalidates(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	const n = 1024
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = pendingCandidateNode(i)
	}
	require.NoError(t, e.BulkCreateNodes(nodes))
	require.NotNil(t, e.FindNodeNeedingEmbedding())
	generation := e.pendingCandidatesGeneration.Load()
	deleted, _, err := e.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(n), deleted)
	require.Zero(t, e.PendingEmbeddingsCount())
	p := &e.pendingCandidates
	p.mu.Lock()
	unchanged := p.loaded && p.generation == e.pendingCandidatesGeneration.Load() && len(p.ids) > 0
	t.Logf("REVIEW_DROP durable=0 heap_len=%d members=%d generation_before=%d generation_after=%d", len(p.ids), len(p.members), generation, e.pendingCandidatesGeneration.Load())
	p.mu.Unlock()
	if unchanged {
		t.Error("DROP DATABASE keeps the deleted namespace's candidate cache valid and populated")
	}
	require.Nil(t, e.FindNodeNeedingEmbedding(), "deleted nodes must never be returned")
}

func TestPendingCandidatesReviewDuplicateAndDelayedHints(t *testing.T) {
	e := newTestBadgerEngineForPending(t)
	require.Nil(t, e.FindNodeNeedingEmbedding())
	_, err := e.CreateNode(pendingCandidateNode(0))
	require.NoError(t, err)
	cw := &commitWriter{engine: e, pendingEmbeddingIDs: map[NodeID]struct{}{pendingCandidateNode(0).ID: {}}}
	for i := 0; i < 32; i++ {
		cw.pendingEmbeddingIDs = map[NodeID]struct{}{pendingCandidateNode(0).ID: {}}
		cw.publishPendingEmbeddingAdds()
	}
	require.Len(t, e.pendingCandidates.ids, 1)
	require.NotNil(t, e.FindNodeNeedingEmbedding())
	require.NoError(t, e.DeleteNode(pendingCandidateNode(0).ID))
	cw.pendingEmbeddingIDs = map[NodeID]struct{}{pendingCandidateNode(0).ID: {}}
	cw.publishPendingEmbeddingAdds()
	require.Nil(t, e.FindNodeNeedingEmbedding())
	require.Zero(t, e.PendingEmbeddingsCount())
}
