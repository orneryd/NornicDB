package storage

import (
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestPreparedVectorImagesStayDetachedFromCallers(t *testing.T) {
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)

	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:owned", Properties: map[string]any{"text": "same"}, ChunkEmbeddings: [][]float32{{1, 2}}, NamedEmbeddings: map[string][]float32{"title": {3, 4}}})
	require.NoError(t, err)
	input, err := engine.GetNode(t.Context(), "test:owned")
	require.NoError(t, err)
	input.Properties["flag"] = true
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: input, Policy: ClientNodeEdits}))
	prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "owned-images"})
	require.NoError(t, err)
	defer prepared.Abort()
	before, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	input.Properties["flag"] = false
	input.ChunkEmbeddings[0][0] = 999
	input.NamedEmbeddings["title"][0] = 999
	diagnostic := prepared.Operations()
	require.Len(t, diagnostic, 1)
	for _, node := range []*Node{diagnostic[0].Node, diagnostic[0].OldNode} {
		require.NotNil(t, node)
		require.Equal(t, [][]float32{{1, 2}}, node.ChunkEmbeddings)
		require.Equal(t, []float32{3, 4}, node.NamedEmbeddings["title"])
		node.ChunkEmbeddings[0][0] = 777
		node.NamedEmbeddings["title"][0] = 777
		node.Properties["flag"] = "diagnostic"
	}
	after, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	beforeEnvelope, beforeOps, err := DecodeEntityTransaction(before)
	require.NoError(t, err)
	afterEnvelope, afterOps, err := DecodeEntityTransaction(after)
	require.NoError(t, err)
	require.Equal(t, beforeEnvelope, afterEnvelope)
	require.Equal(t, beforeOps, afterOps, "external mutation must not change the accepted images; map iteration order is not part of this contract")
	// A parked frame cannot acquire new client writes that mutate retained images.
	require.Error(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: input, Policy: ClientNodeEdits}))
}

func TestPreparedDiagnosticDeletionFactsStayDetached(t *testing.T) {
	original := AffectedFact{EdgeID: "test:edge", StartNode: "test:a", EndNode: "test:b", Type: "LINKS", Deleted: true}
	prepared := &PreparedEntityCommit{operations: []Operation{{Type: OpDeleteNode, NodeID: "test:a", DeletedEdgeIDs: []EdgeID{"test:edge"}, DeletedEdgeFacts: []AffectedFact{original}}}}
	diagnostic := prepared.Operations()
	diagnostic[0].DeletedEdgeIDs[0] = "test:other"
	diagnostic[0].DeletedEdgeFacts[0].Type = "CORRUPTED"
	again := prepared.Operations()
	require.Equal(t, []EdgeID{"test:edge"}, again[0].DeletedEdgeIDs)
	require.Equal(t, []AffectedFact{original}, again[0].DeletedEdgeFacts)
}

func TestPreparedImagesStayDetachedThroughPublication(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true, synchronous: !buffered})
			engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			_, err := engine.CreateNode(t.Context(), &Node{ID: "test:published", Properties: map[string]any{"text": "same"}, ChunkEmbeddings: [][]float32{{1, 2}}})
			require.NoError(t, err)
			require.NoError(t, engine.FlushWriteBehind(t.Context()))
			delivered := make(chan struct{}, 1)
			engine.OnNodeUpdated(func(node *Node) {
				node.Properties["flag"] = "listener"
				node.ChunkEmbeddings[0][0] = 999
				delivered <- struct{}{}
			})
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: &Node{ID: "test:published", Properties: map[string]any{"text": "same", "flag": true}}, Policy: ClientNodeEdits}))
			prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "publication-owned-images"})
			require.NoError(t, err)
			defer prepared.Abort()
			payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
			require.NoError(t, err)
			envelope, operations, err := DecodeEntityTransaction(payload)
			require.NoError(t, err)
			require.NoError(t, prepared.Commit(nil))
			select {
			case <-delivered:
			case <-time.After(5 * time.Second):
				t.Fatal("listener did not receive committed update")
			}
			require.NoError(t, engine.FlushWriteBehind(t.Context()))
			after, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
			require.NoError(t, err)
			gotEnvelope, gotOperations, err := DecodeEntityTransaction(after)
			require.NoError(t, err)
			require.Equal(t, envelope, gotEnvelope)
			require.Equal(t, operations, gotOperations, "physical publication and listener mutation must not change accepted images")
			stored, err := engine.GetNode(t.Context(), "test:published")
			require.NoError(t, err)
			require.Equal(t, true, stored.Properties["flag"])
			require.Equal(t, [][]float32{{1, 2}}, stored.ChunkEmbeddings)
		})
	}
}
