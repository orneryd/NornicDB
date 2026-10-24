package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchResultMaterializedNode(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	source := &storage.Node{ID: "tenant:source", Labels: []string{"RetrievalSource"}, Properties: map[string]any{"id": "source", "ordinary": int64(42), "embedding": []float64{1, 0}}, CreatedAt: created, UpdatedAt: created.Add(time.Minute), EmbedMeta: map[string]any{"has_embedding": true}}
	result := searchResultFromNode("display-id", source, newResultPropertyProjection(nil, nil))
	node := result.MaterializedNode()
	require.NotSame(t, source, node)
	require.Equal(t, source, node)
	node.CreatedAt = time.Time{}
	require.Equal(t, created, source.CreatedAt, "output body must not modify its source")
	fallback := (SearchResult{ID: "legacy", Labels: source.Labels, Properties: source.Properties}).MaterializedNode()
	require.Equal(t, storage.NodeID("legacy"), fallback.ID)
	require.Equal(t, source.Properties, fallback.Properties)
}

func TestSearchResultMaterializationDoesNotRetainManagedVectorsOrExcludedProperties(t *testing.T) {
	for _, dimensions := range []int{2, 2048, 65536} {
		t.Run(fmt.Sprint(dimensions), func(t *testing.T) {
			vector := make([]float32, dimensions)
			vector[0] = 1
			source := &storage.Node{ID: "tenant:source", Labels: []string{"RetrievalSource"}, Properties: map[string]any{"id": "source", "opaque": make([]byte, dimensions)}, ChunkEmbeddings: [][]float32{vector}, NamedEmbeddings: map[string][]float32{"body": vector}, CreatedAt: time.Now(), EmbedMeta: map[string]any{"has_embedding": true}}
			result := searchResultFromNode(string(source.ID), source, newResultPropertyProjection([]string{"id"}, nil))
			require.Nil(t, result.sourceNode.Properties, "metadata must not retain excluded payloads")
			require.Nil(t, result.sourceNode.ChunkEmbeddings)
			require.Nil(t, result.sourceNode.NamedEmbeddings)
			output := result.MaterializedNode()
			require.Equal(t, map[string]any{"id": "source"}, output.Properties)
			require.Equal(t, source.CreatedAt, output.CreatedAt)
			require.Equal(t, source.EmbedMeta, output.EmbedMeta)
			require.Nil(t, output.ChunkEmbeddings)
			require.Nil(t, output.NamedEmbeddings)
			require.Len(t, source.ChunkEmbeddings[0], dimensions, "materialization must not mutate storage values")
			compact := compactContinuationResult(result)
			require.Nil(t, compact.sourceNode, "retained continuation rows must contain no node body")
			require.Nil(t, compact.Properties)
			hydrated := hydrateContinuationResult(compact, result)
			require.Equal(t, output, hydrated.MaterializedNode())
		})
	}
}

func TestSearchResultMetadataCountsTowardCacheBudget(t *testing.T) {
	source := &storage.Node{ID: "source", EmbedMeta: map[string]any{"chunk_texts": "retained metadata"}}
	result := searchResultFromNode("source", source, newResultPropertyProjection(nil, nil))
	with := estimateSearchResponseBytes(&SearchResponse{Results: []SearchResult{result}})
	bytes := result.nodeMetadataBytes()
	require.Positive(t, bytes)
	result.sourceNode = nil
	without := estimateSearchResponseBytes(&SearchResponse{Results: []SearchResult{result}})
	require.Equal(t, bytes, with-without)
}

func TestSearchContinuationUsesSharedNodeMaterialization(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "tenant")
	created := time.Now().Add(-time.Hour)
	_, err = store.CreateNode(&storage.Node{ID: "source", Labels: []string{"RetrievalSource"}, Properties: map[string]any{"id": "source", "title": "retrieval", "content": "catalogue"}, CreatedAt: created, UpdatedAt: created})
	require.NoError(t, err)
	node, err := store.GetNode("source")
	require.NoError(t, err)
	ordinary := searchResultFromNode("source", node, newResultPropertyProjection(nil, nil))
	continuation := searchResultFromContinuationNode(node, newResultPropertyProjection(nil, nil))
	assertMaterializedSearchNode(t, ordinary.MaterializedNode(), continuation.MaterializedNode())
	hydrated, err := hydrateContinuationResultsContext(context.Background(), store, []SearchResult{compactContinuationResult(ordinary)}, nil, newResultPropertyProjection(nil, nil))
	require.NoError(t, err)
	require.Len(t, hydrated, 1)
	assertMaterializedSearchNode(t, ordinary.MaterializedNode(), hydrated[0].MaterializedNode())
}

func assertMaterializedSearchNode(t *testing.T, want, got *storage.Node) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.Labels, got.Labels)
	require.Equal(t, want.Properties, got.Properties)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "creation timestamp changed")
	require.True(t, want.UpdatedAt.Equal(got.UpdatedAt), "update timestamp changed")
	require.Equal(t, want.EmbedMeta, got.EmbedMeta)
	require.Equal(t, want.VisibilitySuppressed, got.VisibilitySuppressed)
	require.Empty(t, got.ChunkEmbeddings)
	require.Empty(t, got.NamedEmbeddings)
}

var materializedSearchNodeSink *storage.Node

func BenchmarkSearchResultMaterialization(b *testing.B) {
	for _, dimensions := range []int{2, 2048, 65536} {
		b.Run(fmt.Sprint(dimensions), func(b *testing.B) {
			vector := make([]float32, dimensions)
			source := &storage.Node{ID: "source", Labels: []string{"RetrievalSource"}, Properties: map[string]any{"id": "source"}, ChunkEmbeddings: [][]float32{vector}, CreatedAt: time.Now()}
			projection := newResultPropertyProjection(nil, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := searchResultFromNode("source", source, projection)
				materializedSearchNodeSink = result.MaterializedNode()
			}
		})
	}
}
