// NOR-160 lane (Claude), 30 October 2026: a search hit's caller copy is complete.
package search

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func nor160RelationshipEdge(id storage.EdgeID) *storage.Edge {
	return &storage.Edge{ID: id, Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{
		"payload": map[string]any{"value": "original"},
		"list":    []any{"a", []any{"b"}},
	}}
}

// nor160RelationshipHit populates every exported reference field of a hit,
// including a passage that carries its own relationship.
func nor160RelationshipHit() SearchResult {
	passage := SearchResult{ID: "test:p", Labels: []string{"Chunk"},
		Properties:   map[string]any{"text": map[string]any{"value": "passage"}},
		Matches:      []SearchMatch{{Kind: SearchMatchTerm, Term: "x"}},
		Relationship: nor160RelationshipEdge("test:p")}
	return SearchResult{ID: "test:e", Labels: []string{"Held"},
		Properties:   map[string]any{"payload": map[string]any{"value": "original"}},
		Matches:      []SearchMatch{{Kind: SearchMatchTerm, Term: "x"}},
		Passages:     []SearchPassage{passage},
		Relationship: nor160RelationshipEdge("test:e")}
}

// The hit's Copier must leave no exported reference shared, for every field
// the canonical walk would otherwise copy.
func TestNOR160CopierCompleteSearchResult(t *testing.T) {
	hit := nor160RelationshipHit()
	require.Empty(t, readownership.Unpopulated(hit), "the sample must populate every reference field")
	require.Empty(t, readownership.SharedReferences(hit, hit.CallerCopy()))
}

// Planted control: the partial copier this head replaced (no Relationship)
// must turn the completeness check red, at the hit and inside its passage.
func TestNOR160CopierControlPartialSearchResult(t *testing.T) {
	var partial func(SearchResult) SearchResult
	partial = func(result SearchResult) SearchResult {
		result.Labels = append([]string(nil), result.Labels...)
		result.Properties = readownershipFreeMap(result.Properties)
		result.Matches = append([]SearchMatch(nil), result.Matches...)
		if result.Passages != nil {
			passages := make([]SearchPassage, len(result.Passages))
			for i, passage := range result.Passages {
				passages[i] = partial(passage)
			}
			result.Passages = passages
		}
		return result
	}
	hit := nor160RelationshipHit()
	shared := readownership.SharedReferences(hit, partial(hit))
	require.Contains(t, shared, ".Relationship")
	require.Contains(t, shared, ".Passages[0].Relationship")
}

// readownershipFreeMap deep-copies a property map without the product owner,
// so the planted control differs from the real copier only by the omission.
func readownershipFreeMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if nested, ok := v.(map[string]any); ok {
			v = readownershipFreeMap(nested)
		}
		out[k] = v
	}
	return out
}

// A replayable stream of relationship hits: changing a delivered hit's
// relationship, or its passage's, never reaches another page.
func TestNOR160RetentionRelationshipHitReplay(t *testing.T) {
	stream, err := resultstream.NewProgressive([][]any{{nor160RelationshipHit()}}, true, 1, nil)
	require.NoError(t, err)
	ctx := context.Background()
	want := readownership.Capture([][]any{{nor160RelationshipHit()}})
	var earlier *resultstream.Page
	for pass := 0; pass < 3; pass++ {
		page, err := stream.Pull(ctx, 0, 1)
		require.NoError(t, err)
		require.Equal(t, want, readownership.Capture(page.Rows), "pass %d", pass)
		if earlier != nil {
			// Another caller's later page is not this caller's page.
			require.Equal(t, want, readownership.Capture(earlier.Rows), "a later pull changed an earlier page, pass %d", pass)
		}
		earlier = page
		next, err := stream.Pull(ctx, 0, 1)
		require.NoError(t, err)
		hit := next.Rows[0][0].(SearchResult)
		hit.Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
		hit.Passages[0].Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
		readownership.Mutate(next.Rows)
	}
}

// A replayable catalog continuation keeps its ranked hits: a page's hit,
// its matches included, is the caller's own.
func TestNOR160RetentionContinuationHits(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&storage.Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"title": "t"}})
	require.NoError(t, err)
	provider, ok := storage.Engine(engine).(graphMutationVersionProvider)
	require.True(t, ok)
	version, supported := provider.GraphMutationVersion()
	require.True(t, supported)
	policy := &atomic.Uint64{}
	policy.Store(1)
	ranked := SearchResult{ID: "test:n1", NodeID: "test:n1", Matches: []SearchMatch{{Kind: SearchMatchTerm, Term: "x", Property: "title"}}}
	stream := &catalogContinuationStream{engine: engine, version: version, policy: policy, policyID: 1, results: []SearchResult{ranked}}
	ctx := context.Background()
	for pass := 0; pass < 3; pass++ {
		page, err := stream.Pull(ctx, 0, 1)
		require.NoError(t, err)
		require.Len(t, page.Rows, 1)
		hit := page.Rows[0][0].(SearchResult)
		require.Equal(t, "x", hit.Matches[0].Term, "pass %d", pass)
		hit.Matches[0].Term = "caller"
	}
}

// The search result cache admits and hands out responses whose metrics are
// the caller's own.
func TestNOR160RetentionSearchResultCacheMetrics(t *testing.T) {
	cache := newSearchResultCache(8, time.Hour)
	input := &SearchResponse{Results: []SearchResult{nor160RelationshipHit()}, Metrics: &SearchMetrics{TotalTimeMs: 7}}
	cache.Put("q", input)
	input.Metrics.TotalTimeMs = -1
	for pass := 0; pass < 3; pass++ {
		got := cache.Get("q")
		require.NotNil(t, got)
		require.Equal(t, 7, got.Metrics.TotalTimeMs, "pass %d", pass)
		got.Metrics.TotalTimeMs = -1
	}
}
