// NOR-160 lane (Claude), 30 October 2026: retained search hits are caller-owned.
package search

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func nor160HitNode() *storage.Node {
	return &storage.Node{
		ID:     "n1",
		Labels: []string{"Held"},
		Properties: map[string]any{
			"title":   "t",
			"payload": map[string]any{"value": "original"},
			"list":    []any{"a", []any{"b"}},
		},
		EmbedMeta: map[string]any{"model": map[string]any{"name": "m"}},
	}
}

// A retained hit materializes a new caller-owned node every time.
func TestNOR160RetentionMaterializedHit(t *testing.T) {
	hit := searchResultFromNode("n1", nor160HitNode(), resultPropertyProjection{})
	want := readownership.Capture(searchResultFromNode("n1", nor160HitNode(), resultPropertyProjection{}).MaterializedNode())
	for pass := 0; pass < 3; pass++ {
		node := hit.MaterializedNode()
		require.Equal(t, want, readownership.Capture(node), "pass %d", pass)
		readownership.Mutate(node)
	}
}

// The search result cache admits and hands out caller-owned hits.
func TestNOR160RetentionSearchResultCache(t *testing.T) {
	cache := newSearchResultCache(8, time.Hour)
	input := &SearchResponse{Results: []SearchResult{searchResultFromNode("n1", nor160HitNode(), resultPropertyProjection{})}}
	cache.Put("q", input)
	want := readownership.Capture((&SearchResponse{Results: []SearchResult{searchResultFromNode("n1", nor160HitNode(), resultPropertyProjection{})}}).Results)
	readownership.Mutate(input.Results)
	for pass := 0; pass < 3; pass++ {
		got := cache.Get("q")
		require.NotNil(t, got)
		require.Equal(t, want, readownership.Capture(got.Results), "pass %d", pass)
		readownership.Mutate(got.Results)
	}
}

// A replayable continuation keeps its metadata; every page owns its values.
func TestNOR160RetentionContinuationMetadata(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	provider, ok := storage.Engine(engine).(graphMutationVersionProvider)
	require.True(t, ok)
	version, supported := provider.GraphMutationVersion()
	require.True(t, supported)
	policy := &atomic.Uint64{}
	policy.Store(1)
	metadata := func() map[string]any { return map[string]any{"facet": map[string]any{"values": []any{"x"}}} }
	stream := &catalogContinuationStream{engine: engine, version: version, policy: policy, policyID: 1, metadata: metadata()}
	want := readownership.Capture(metadata()["facet"])
	ctx := context.Background()
	for pass := 0; pass < 3; pass++ {
		page, err := stream.Pull(ctx, 0, 1)
		require.NoError(t, err)
		require.Equal(t, want, readownership.Capture(page.Metadata["facet"]), "pass %d", pass)
		readownership.Mutate(page.Metadata)
	}
}
