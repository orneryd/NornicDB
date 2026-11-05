package search

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLiveIndexAnalyzesNewDocumentOnce(t *testing.T) {
	t.Run("fulltext", func(t *testing.T) {
		var calls atomic.Int64
		options := DefaultServiceOptions("test")
		options.BM25Stemmer = &stemmer.Registration{ID: "test.count", StemTokens: func(tokens []string) []string {
			calls.Add(1)
			return tokens
		}}
		service := NewService(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 3, &options)
		t.Cleanup(func() { _ = service.Close() })
		node := &storage.Node{ID: "test:n1", Labels: []string{"Item"}, Properties: map[string]any{"name": "alpha beta alpha"}}
		require.NoError(t, service.IndexNode(node))
		require.EqualValues(t, 1, calls.Load(), "capacity accounting and indexing must share analysis")
		require.EqualValues(t, len(node.ID)+16+(len("item")+8)+(len("alpha")+8)+(len("beta")+8), service.bm25MetadataBytes)
		require.NoError(t, service.IndexNode(node))
		require.EqualValues(t, 1, calls.Load(), "unchanged documents need no additional analysis")
		results := service.fulltext().Search("alpha", 10)
		require.Len(t, results, 1)
		require.Equal(t, string(node.ID), results[0].ID)
		node.Properties["name"] = "gamma"
		require.NoError(t, service.IndexNode(node))
		require.Empty(t, service.fulltext().Search("alpha", 10))
		require.Len(t, service.fulltext().Search("gamma", 10), 1)
	})
}

// Prepared writes and ordinary public writes must produce the same index,
// including replacements, tokenless text and the empty-ID behavior.
func TestPreparedFulltextMatchesPublicWrites(t *testing.T) {
	t.Run("fulltext", func(t *testing.T) {
		plain := NewFulltextIndexV2WithAnalyzer(DefaultTextAnalyzer())
		prepared := NewFulltextIndexV2WithAnalyzer(DefaultTextAnalyzer())
		for _, entry := range []FulltextBatchEntry{
			{ID: "one", Text: "Café café 42"},
			{ID: "two", Text: "alpha beta"},
			{ID: "one", Text: "beta beta"},
			{ID: "two", Text: "---"},
			{ID: "", Text: "empty identifier"},
		} {
			terms := DefaultTextAnalyzer().Analyze(entry.Text)
			frequency := make(map[string]int, len(terms))
			for _, term := range terms {
				frequency[term]++
			}
			plain.Index(entry.ID, entry.Text, nil, nil, false)
			prepared.Index(entry.ID, entry.Text, terms, frequency, true)
			require.Equal(t, plain.Count(), prepared.Count())
			for _, id := range []string{"one", "two", ""} {
				plainText, plainOK := plain.GetDocument(id)
				preparedText, preparedOK := prepared.GetDocument(id)
				require.Equal(t, plainOK, preparedOK)
				require.Equal(t, plainText, preparedText)
			}
			for _, query := range []string{"café", "alpha", "beta", "identifier"} {
				require.Equal(t, plain.Search(query, 10), prepared.Search(query, 10))
			}
		}
	})
}

func BenchmarkLiveIndexNewDocuments(b *testing.B) {
	b.Run("fulltext", func(b *testing.B) {
		service := NewService(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 3, nil)
		b.Cleanup(func() { _ = service.Close() })
		nodes := make([]storage.Node, b.N)
		for i := range nodes {
			nodes[i] = storage.Node{ID: storage.NodeID("test:" + strconv.Itoa(i)), Labels: []string{"Product"}, Properties: map[string]any{"name": "fresh milk", "category": "dairy goods"}}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range nodes {
			if err := service.IndexNode(&nodes[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
}
