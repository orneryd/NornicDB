package search

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkFilteredSearch times a 10-result search over 2,000 strongly
// matching "Other" nodes and 50 weakly matching "Doc" nodes, with and without
// a Types or Filters selection of the Doc nodes (#938). results/op is how many
// results the search returned. The result cache is off, so every iteration
// searches.
func BenchmarkFilteredSearch(b *testing.B) {
	store := storage.NewNamespacedEngine(newNamespacedEngine(b), "test")
	svc := NewServiceWithDimensions(store, 2)
	svc.SetSearchResultCachePolicy(0, 0)
	add := func(id, label, collection, content string, vector []float32) {
		node := &storage.Node{
			ID:              storage.NodeID(id),
			Labels:          []string{label},
			Properties:      map[string]any{"content": content, "collection": collection},
			ChunkEmbeddings: [][]float32{vector},
		}
		if _, err := store.CreateNode(node); err != nil {
			b.Fatal(err)
		}
		if err := svc.IndexNode(node); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 2000; i++ {
		add(fmt.Sprintf("other-%d", i), "Other", "theirs", "alpha alpha alpha", []float32{1, 0})
	}
	filler := strings.Repeat(" filler", 20)
	for i := 0; i < 50; i++ {
		add(fmt.Sprintf("doc-%d", i), "Doc", "mine", "alpha"+filler, []float32{0.6, 0.8})
	}

	ctx := context.Background()
	// The first vector search builds the vector index; build it untimed.
	if _, err := svc.Search(ctx, "", []float32{1, 0}, DefaultSearchOptions()); err != nil {
		b.Fatal(err)
	}
	types := func(o *SearchOptions) { o.Types = []string{"Doc"} }
	filters := func(o *SearchOptions) { o.Filters = map[string][]string{"collection": {"mine"}} }
	for _, c := range []struct {
		name      string
		query     string
		embedding []float32
		narrow    func(*SearchOptions)
	}{
		{"bm25/unfiltered", "alpha", nil, nil},
		{"bm25/types", "alpha", nil, types},
		{"bm25/filters", "alpha", nil, filters},
		{"vector/unfiltered", "", []float32{1, 0}, nil},
		{"vector/types", "", []float32{1, 0}, types},
		{"vector/filters", "", []float32{1, 0}, filters},
		{"hybrid/unfiltered", "alpha", []float32{1, 0}, nil},
		{"hybrid/filters", "alpha", []float32{1, 0}, filters},
	} {
		b.Run(c.name, func(b *testing.B) {
			opts := DefaultSearchOptions()
			opts.Limit = 10
			opts.MinSimilarity = nil
			if c.narrow != nil {
				c.narrow(opts)
			}
			results := 0
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				response, err := svc.Search(ctx, c.query, c.embedding, opts)
				if err != nil {
					b.Fatal(err)
				}
				results = len(response.Results)
			}
			b.ReportMetric(float64(results), "results/op")
		})
	}
}
