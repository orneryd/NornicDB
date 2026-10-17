package search

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SearchVisible's edges: a nil visible is SearchAllowed; no limit, an empty
// index and a query with no terms find nothing; a removed document neither
// counts in the statistics nor appears; the result is cut to the limit.
func TestSearchVisibleEdges(t *testing.T) {
	index := NewFulltextIndexV2()
	for id, text := range map[string]string{"a": "alpha beta", "b": "alpha gamma", "c": "alpha delta", "gone": "alpha"} {
		index.Index(id, text)
	}
	index.Remove("gone")
	all := func(string) bool { return true }

	require.Equal(t, index.SearchAllowed("alpha", 10, nil), index.SearchVisible("alpha", 10, nil, nil))
	require.Empty(t, index.SearchVisible("alpha", 0, all, nil))
	require.Empty(t, NewFulltextIndexV2().SearchVisible("alpha", 10, all, nil))
	require.Empty(t, index.SearchVisible("   ", 10, all, nil))

	results := index.SearchVisible("alpha", 10, all, nil)
	require.Len(t, results, 3)
	for _, result := range results {
		require.NotEqual(t, "gone", result.ID)
	}
	require.Len(t, index.SearchVisible("alpha", 2, all, nil), 2)
}

// Vector index queries take a restricted reader's visibility as a candidate
// filter (#935): a node or relationship it rejects is never ranked, on the
// exact node path and on relationship queries, so its top-K are visible.
func TestVectorQueriesRankOnlyVisibleCandidates(t *testing.T) {
	ctx := context.Background()
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	for _, node := range []*storage.Node{
		{ID: "hidden", Labels: []string{"Doc"}, Properties: map[string]any{"v": []any{1.0, 0.0, 0.0}}},
		{ID: "near", Labels: []string{"Doc"}, Properties: map[string]any{"v": []any{0.8, 0.2, 0.0}}},
		{ID: "far", Labels: []string{"Doc"}, Properties: map[string]any{"v": []any{0.1, 0.9, 0.0}}},
	} {
		require.NoError(t, svc.IndexNode(node))
	}
	for _, edge := range []*storage.Edge{
		{ID: "hidden-rel", Type: "REL", StartNode: "hidden", EndNode: "near", Properties: map[string]any{"embedding": []float32{1, 0, 0}}},
		{ID: "near-rel", Type: "REL", StartNode: "near", EndNode: "far", Properties: map[string]any{"embedding": []float32{0.8, 0.2, 0}}},
	} {
		require.NoError(t, svc.IndexEdge(edge))
	}
	query := []float32{1, 0, 0}

	nodes, err := svc.VectorQueryNodes(ctx, query, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 1,
		Restricted: &RestrictedReader{Node: func(id string) bool { return id != "hidden" }}})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "near", nodes[0].ID)

	relationships, err := svc.VectorQueryRelationships(ctx, query, RelationshipVectorQuerySpec{Type: "REL", Property: "embedding", Limit: 1})
	require.NoError(t, err)
	require.Equal(t, "hidden-rel", relationships[0].ID, "unfiltered, the hidden relationship ranks first")
	relationships, err = svc.VectorQueryRelationships(ctx, query, RelationshipVectorQuerySpec{Type: "REL", Property: "embedding", Limit: 1,
		Restricted: &RestrictedReader{Relationship: func(id string) bool { return id != "hidden-rel" }}})
	require.NoError(t, err)
	require.Len(t, relationships, 1)
	require.Equal(t, "near-rel", relationships[0].ID)
	relationships, err = svc.VectorQueryRelationships(ctx, query, RelationshipVectorQuerySpec{Type: "REL", Property: "embedding", Limit: 1,
		Restricted: &RestrictedReader{}})
	require.NoError(t, err)
	require.Empty(t, relationships)
}

// bm25Engine is the part of both BM25 engines the restricted-reader
// contract is stated over.
type bm25Engine interface {
	Index(id string, text string)
	Search(query string, limit int) []indexResult
	SearchVisible(query string, limit int, visible, allowed func(docID string) bool) []indexResult
}

// rankedForComparison orders results by score, then ID, so engines that
// order equal scores differently compare equal.
func rankedForComparison(results []indexResult) []indexResult {
	out := append([]indexResult(nil), results...)
	sort.Slice(out, func(i, j int) bool {
		if math.Abs(out[i].Score-out[j].Score) > 1e-9 {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// A restricted reader's BM25 search is the search of an index holding only
// the documents it can see: the same documents, scores and order, however
// many hidden documents share its terms, on every engine and in prefix mode
// (where a term only hidden documents hold, sorting ahead of a visible one,
// takes no expansion). Hidden documents never appear; an allowed filter
// narrows what is scored without changing the statistics.
func TestSearchVisibleEqualsVisibleOnlyCorpus(t *testing.T) {
	documents := map[string]string{
		"a":  "household budget electricity bill alpha alpha alpha",
		"b":  "household insurance renewal beta",
		"c":  "medical results household doctor",
		"d":  "medical prescription doctor doctor",
		"e":  "electricity meter reading prefixzzz",
		"h1": "alpha prefixaaa",
		"h2": "alpha alpha prefixaab",
		"h3": "alpha household",
		"h4": "alpha prefixaac",
	}
	visible := map[string]bool{"a": true, "b": true, "e": true}
	queries := []string{"household", "electricity", "doctor", "household electricity", "medical", "alpha beta", "prefix", "prefixz"}
	engines := map[string]struct {
		prefixExpansions string
		build            func() bm25Engine
	}{
		"v2":        {"0", func() bm25Engine { return NewFulltextIndexV2() }},
		"v2 prefix": {"1", func() bm25Engine { return NewFulltextIndexV2() }},
		"v1":        {"0", func() bm25Engine { return NewFulltextIndex() }},
	}
	for name, engine := range engines {
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", engine.prefixExpansions)
			t.Setenv("NORNICDB_BM25_PREFIX_MIN_LEN", "3")
			full, onlyVisible := engine.build(), engine.build()
			for id, text := range documents {
				full.Index(id, text)
				if visible[id] {
					onlyVisible.Index(id, text)
				}
			}
			isVisible := func(id string) bool { return visible[id] }
			for _, query := range queries {
				want := rankedForComparison(onlyVisible.Search(query, 10))
				got := rankedForComparison(full.SearchVisible(query, 10, isVisible, nil))
				require.Equal(t, len(want), len(got), "%s: %v vs %v", query, want, got)
				for i := range want {
					require.Equal(t, want[i].ID, got[i].ID, query)
					require.InDelta(t, want[i].Score, got[i].Score, 1e-9, "%s: %s", query, got[i].ID)
				}
				for _, result := range got {
					require.True(t, visible[result.ID], query)
				}
				if len(want) > 0 {
					top := full.SearchVisible(query, 1, isVisible, nil)
					require.Len(t, top, 1, query)
					require.Equal(t, want[0].ID, top[0].ID, "%s: the top result is the visible corpus's", query)
				}
			}
			if name == "v2 prefix" {
				require.NotEmpty(t, full.SearchVisible("prefix", 10, isVisible, nil), "the visible expansion is taken although hidden ones sort first")
			}

			narrowed := full.SearchVisible("household", 10, isVisible, func(id string) bool { return id == "b" })
			require.Len(t, narrowed, 1)
			for _, result := range onlyVisible.Search("household", 10) {
				if result.ID == "b" {
					require.InDelta(t, result.Score, narrowed[0].Score, 1e-9)
				}
			}
			require.Empty(t, full.SearchVisible("household", 10, func(string) bool { return false }, nil))
		})
	}
}

// A restricted search decides visibility (which reads storage) before any
// index lock is taken: a visibility callback that writes the index, as a
// commit landing at that moment does, completes instead of waiting on the
// search's own lock. On both engines.
func TestRestrictedSearchDecidesVisibilityOutsideIndexLocks(t *testing.T) {
	for _, engine := range []string{"v1", "v2"} {
		t.Run(engine, func(t *testing.T) {
			t.Setenv(EnvSearchBM25Engine, engine)
			engineStore := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
			svc := NewServiceWithDimensions(engineStore, 3)
			t.Cleanup(func() { _ = svc.Close() })
			for i, text := range []string{"alpha visible", "alpha hidden", "alpha visible too"} {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": text}}
				_, err := engineStore.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(node))
			}
			ctx := context.Background()
			type outcome struct {
				response *SearchResponse
				err      error
			}
			done := make(chan outcome, 1)
			go func() {
				opts := DefaultSearchOptions()
				opts.Limit = 10
				opts.MinSimilarity = nil
				commits := 0
				opts.Restricted = &RestrictedReader{Node: func(id string) bool {
					commits++
					// A commit indexing a node while visibility is decided.
					late := &storage.Node{ID: storage.NodeID(fmt.Sprintf("late-%d", commits)), Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha late"}}
					_, _ = engineStore.CreateNode(late)
					_ = svc.IndexNode(late)
					return id != "n1"
				}}
				response, err := svc.Search(ctx, "alpha", nil, opts)
				done <- outcome{response, err}
			}()
			select {
			case result := <-done:
				require.NoError(t, result.err)
				require.NotEmpty(t, result.response.Results)
				for _, hit := range result.response.Results {
					require.NotEqual(t, "n1", hit.ID)
					require.False(t, strings.HasPrefix(hit.ID, "late-"), "a document indexed after visibility was decided isn't visible")
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the restricted search waited on its own index lock")
			}
		})
	}
}
