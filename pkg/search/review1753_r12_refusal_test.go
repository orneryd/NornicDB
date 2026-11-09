package search

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review round 12 of #1753 (reviewer probes of r4508564855 and r4508564886):
// a refresh admits a node's changed metadata against the full usage whether
// or not its vectors changed, and a relationship whose stored state a budget
// refuses serves neither its replaced vectors nor its old type.

func TestReview1753R12NodeMetadataBudget(t *testing.T) {
	ctx := context.Background()
	store := newNamespacedMemoryEngine(t)
	_, err := store.CreateNode(ctx, &storage.Node{ID: "a", Labels: []string{"D"}, Properties: map[string]any{"text": "admittedterm", "emb": []float64{1, 0, 0}}})
	require.NoError(t, err)
	svc := NewService(store, 3, &ServiceOptions{MetadataMemoryMaxBytes: 100})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	old, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	updated := storage.CopyNode(old)
	updated.Labels = []string{strings.Repeat("L", 100)}
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: updated}))
	refreshErr := svc.RefreshNode(ctx, "a")
	t.Logf("refresh=%v vectorMetadata=%d limit=%d", refreshErr, svc.vectorMetadataBytes, svc.metadataMemoryMaxBytes)
	require.ErrorIs(t, refreshErr, ErrIndexMemoryBudgetExceeded, "unchanged vectors do not exempt changed label metadata")
	require.LessOrEqual(t, svc.vectorMetadataBytes, svc.metadataMemoryMaxBytes)
}

func TestReview1753R12UnchangedVectorMetadataRefusal(t *testing.T) {
	ctx := context.Background()
	store := newNamespacedMemoryEngine(t)
	_, err := store.CreateNode(ctx, &storage.Node{ID: "a", Labels: []string{"D"}, Properties: map[string]any{"emb": []float64{1, 0, 0}}})
	require.NoError(t, err)
	svc := NewService(store, 3, &ServiceOptions{MetadataMemoryMaxBytes: 100})
	svc.SetIndexFlags(false, true)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	old, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	updated := storage.CopyNode(old)
	updated.Labels = []string{strings.Repeat("L", 100)}
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: updated}))
	refreshErr := svc.RefreshNode(ctx, "a")
	t.Logf("refresh=%v vectorMetadata=%d limit=%d", refreshErr, svc.vectorMetadataBytes, svc.metadataMemoryMaxBytes)
	require.ErrorIs(t, refreshErr, ErrIndexMemoryBudgetExceeded, "changed label metadata must be admitted even with unchanged vectors and BM25 disabled")
}

func TestReview1753R12RelationshipRetainedMetadataMustWithdrawStale(t *testing.T) {
	ctx := context.Background()
	store := newNamespacedMemoryEngine(t)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(ctx, &storage.Node{ID: id})
		require.NoError(t, err)
	}
	old := &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"emb": []float64{1, 0, 0}, "emb2": []float64{0, 1, 0}}}
	require.NoError(t, store.CreateEdge(ctx, old))
	svc := NewService(store, 3, &ServiceOptions{MetadataMemoryMaxBytes: 100})
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	updated := storage.CopyEdge(old)
	updated.Type = strings.Repeat("S", 30)
	updated.Properties["emb"] = []float64{0.6, 0.8, 0}
	require.NoError(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{After: updated}))
	require.ErrorIs(t, svc.RefreshEdge(ctx, "r"), ErrIndexMemoryBudgetExceeded)
	svc.nodeMutationMu.Lock()
	pending := len(svc.edgeMutations.pending)
	svc.nodeMutationMu.Unlock()
	hits, err := svc.VectorQueryRelationships(ctx, []float32{1, 0, 0}, RelationshipVectorQuerySpec{Type: "R", Property: "emb", Similarity: "cosine", Limit: 10})
	require.NoError(t, err)
	stored, err := store.GetEdge(ctx, "r")
	require.NoError(t, err)
	t.Logf("storedType=%s pending=%d servedOldType=%+v", stored.Type, pending, hits)
	require.Zero(t, pending)
	require.Empty(t, hits, "completed refused refresh must withdraw replaced vectors and the old type")
}
