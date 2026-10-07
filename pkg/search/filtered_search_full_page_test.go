package search

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// filteredSearchFixture indexes 1,200 "Other" nodes that match the query
// strongly (text "alpha alpha alpha", vector (1, 0)) and 50 "Doc" nodes that
// match it weakly (text "alpha" among filler words, vector (0.6, 0.8)), so a
// filter selecting the 50 must look past every stronger match (#938). Ten Doc
// nodes carry their vector as a named embedding and ten as a vector property,
// the other kinds of node vector a search ranks.
func filteredSearchFixture(t *testing.T) *Service {
	t.Helper()
	store := storage.NewNamespacedEngine(newNamespacedEngine(t), "test")
	svc := NewServiceWithDimensions(store, 2)
	add := func(node *storage.Node) {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	for i := 0; i < 1200; i++ {
		add(&storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("other-%d", i)),
			Labels:          []string{"Other"},
			Properties:      map[string]any{"content": "alpha alpha alpha", "collection": "theirs"},
			ChunkEmbeddings: [][]float32{{1, 0}},
		})
	}
	filler := strings.Repeat(" filler", 20)
	for i := 0; i < 50; i++ {
		node := &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("doc-%d", i)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "alpha" + filler, "collection": "mine"},
		}
		switch {
		case i < 10:
			node.NamedEmbeddings = map[string][]float32{"content": {0.6, 0.8}}
		case i < 20:
			node.Properties["embedding"] = []float32{0.6, 0.8}
		default:
			node.ChunkEmbeddings = [][]float32{{0.6, 0.8}}
		}
		add(node)
	}
	return svc
}

func requireAllPrefixed(t *testing.T, response *SearchResponse, prefix string, want int) {
	t.Helper()
	require.Len(t, response.Results, want)
	for _, result := range response.Results {
		require.True(t, strings.HasPrefix(result.ID, prefix), result.ID)
	}
}

func TestFilteredSearchFillsThePage(t *testing.T) {
	svc := filteredSearchFixture(t)
	ctx := context.Background()
	options := func(mutate func(*SearchOptions)) *SearchOptions {
		opts := DefaultSearchOptions()
		opts.Limit = 50
		opts.MinSimilarity = nil
		mutate(opts)
		return opts
	}
	for name, run := range map[string]func() (*SearchResponse, error){
		"bm25 types": func() (*SearchResponse, error) {
			return svc.Search(ctx, "alpha", nil, options(func(o *SearchOptions) { o.Types = []string{"Doc"} }))
		},
		"bm25 filters": func() (*SearchResponse, error) {
			return svc.Search(ctx, "alpha", nil, options(func(o *SearchOptions) { o.Filters = map[string][]string{"collection": {"mine"}} }))
		},
		"vector types": func() (*SearchResponse, error) {
			return svc.Search(ctx, "", []float32{1, 0}, options(func(o *SearchOptions) { o.Types = []string{"Doc"} }))
		},
		"vector filters": func() (*SearchResponse, error) {
			return svc.Search(ctx, "", []float32{1, 0}, options(func(o *SearchOptions) { o.Filters = map[string][]string{"collection": {"mine"}} }))
		},
		"hybrid filters": func() (*SearchResponse, error) {
			return svc.Search(ctx, "alpha", []float32{1, 0}, options(func(o *SearchOptions) { o.Filters = map[string][]string{"collection": {"mine"}} }))
		},
	} {
		t.Run(name, func(t *testing.T) {
			response, err := run()
			require.NoError(t, err)
			requireAllPrefixed(t, response, "doc-", 50)
		})
	}

	t.Run("an explicit candidate cap is respected", func(t *testing.T) {
		response, err := svc.Search(ctx, "alpha", nil, options(func(o *SearchOptions) {
			o.Filters = map[string][]string{"collection": {"mine"}}
			o.MaxCandidateLimit = 100
		}))
		require.NoError(t, err)
		require.Empty(t, response.Results, "the 50 matches rank below the first 100 candidates")
	})
	t.Run("a page is cut to its limit", func(t *testing.T) {
		for name, embedding := range map[string][]float32{"bm25": nil, "vector": {1, 0}} {
			query := "alpha"
			if embedding != nil {
				query = ""
			}
			response, err := svc.Search(ctx, query, embedding, options(func(o *SearchOptions) {
				o.Limit = 13
				o.Filters = map[string][]string{"collection": {"mine"}}
			}))
			require.NoError(t, err, name)
			requireAllPrefixed(t, response, "doc-", 13)
		}
	})
	t.Run("a type no node has matches nothing", func(t *testing.T) {
		response, err := svc.Search(ctx, "", []float32{1, 0}, options(func(o *SearchOptions) { o.Types = []string{"Missing"} }))
		require.NoError(t, err)
		require.Empty(t, response.Results)
	})
	t.Run("an empty filter list matches nothing", func(t *testing.T) {
		response, err := svc.Search(ctx, "alpha", []float32{1, 0}, options(func(o *SearchOptions) {
			o.Filters = map[string][]string{"collection": {}}
		}))
		require.NoError(t, err)
		require.Empty(t, response.Results)
	})
}

// TestBM25SearchAllowedNeverScoresRejectedDocuments: both BM25 engines ask
// allowed once per document and return only allowed ones, the best allowed
// first, however many better documents they reject. The rejected documents
// match both query terms, one only by prefix ("alphabet"), so every scoring
// loop meets rejected documents, and a removed document is never asked about;
// the v2 index scores into a dense array below
// 100,000 documents and a sparse map above.
func TestBM25SearchAllowedNeverScoresRejectedDocuments(t *testing.T) {
	check := func(t *testing.T, index bm25Index) {
		for i := 0; i < 300; i++ {
			index.Index(fmt.Sprintf("other-%d", i), "alpha alpha alpha beta")
		}
		index.Index("other-prefix", "alphabet beta")
		index.Index("doc-strong", "alpha alpha beta")
		index.Index("doc-weak", "alpha beta gamma delta epsilon")
		index.Index("doc-removed", "alpha alpha alpha alpha beta")
		index.Remove("doc-removed")
		asked := map[string]int{}
		results := index.SearchAllowed("alpha beta", 2, func(id string) bool {
			asked[id]++
			return strings.HasPrefix(id, "doc-")
		})
		require.Len(t, results, 2)
		require.Equal(t, "doc-strong", results[0].ID)
		require.Equal(t, "doc-weak", results[1].ID)
		for id, count := range asked {
			require.Equal(t, 1, count, id)
		}
		require.Len(t, asked, 303)
		require.Empty(t, index.SearchAllowed("alpha beta", 2, func(string) bool { return false }))
		require.Len(t, index.Search("alpha beta", 2), 2, "Search allows every document")
	}
	for _, engine := range []string{BM25EngineV1, BM25EngineV2} {
		t.Run(engine, func(t *testing.T) {
			index, _ := newBM25Index(engine)
			check(t, index)
		})
	}
	t.Run("v2 sparse scores", func(t *testing.T) {
		index := NewFulltextIndexV2()
		entries := make([]FulltextBatchEntry, bm25SparseMinDocSlots)
		for i := range entries {
			entries[i] = FulltextBatchEntry{ID: fmt.Sprintf("filler-%d", i), Text: "filler"}
		}
		index.IndexBatch(entries)
		check(t, index)
	})
}

// TestCompletingAPageStopsWhenCancelled: the passes that fill a filtered page
// return the context's error when it is cancelled between batches or before
// scoring.
func TestCompletingAPageStopsWhenCancelled(t *testing.T) {
	svc := filteredSearchFixture(t)
	opts := DefaultSearchOptions()
	opts.MinSimilarity = nil

	ctx, cancel := context.WithCancel(context.Background())
	cancelling := func([]indexResult) []indexResult { cancel(); return nil }
	_, _, err := svc.completeVectorSearch(ctx, []float32{1, 0}, opts, 50, cancelling, vectorOverfetchStats{})
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = svc.completeVectorSearch(ctx, []float32{1, 0}, opts, 50, cancelling, vectorOverfetchStats{})
	require.ErrorIs(t, err, context.Canceled, "cancelled before scoring")

	ctx, cancel = context.WithCancel(context.Background())
	cancelling = func([]indexResult) []indexResult { cancel(); return nil }
	_, _, err = svc.completeBM25Search(ctx, svc.fulltext().Search, "alpha", 1250, 50, 50, cancelling, vectorOverfetchStats{})
	require.ErrorIs(t, err, context.Canceled)
}
