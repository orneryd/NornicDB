package search

import (
	"fmt"
	"strings"
	"testing"
)

const bm25QualityBenchmarkDocuments = 20000

func BenchmarkBM25V2QualityPaths(b *testing.B) {
	tests := []struct {
		name             string
		prefixExpansions string
		query            string
		coldPlan         bool
	}{
		{name: "ExactWarmCommon", prefixExpansions: "0", query: "shared retrieval"},
		{name: "ExactColdCommon", prefixExpansions: "0", query: "shared retrieval", coldPlan: true},
		{name: "ExactWarmRare", prefixExpansions: "0", query: "rare19999"},
		{name: "Prefix32WarmCommon", prefixExpansions: "32", query: "shared retrieval"},
		{name: "Prefix32ColdCommon", prefixExpansions: "32", query: "shared retrieval", coldPlan: true},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", test.prefixExpansions)
			index := newBM25QualityBenchmarkIndex(bm25QualityBenchmarkDocuments)
			if !test.coldPlan {
				_ = index.Search(test.query, 100)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if test.coldPlan {
					index.queryPlanCache.Clear()
				}
				_ = index.Search(test.query, 100)
			}
		})
	}
}

func newBM25QualityBenchmarkIndex(documentCount int) *FulltextIndexV2 {
	index := NewFulltextIndexV2()
	entries := make([]FulltextBatchEntry, documentCount)
	for document := 0; document < documentCount; document++ {
		terms := []string{
			"shared",
			"retrieval",
			fmt.Sprintf("sharedvariant%d", document%100),
			fmt.Sprintf("rare%d", document),
			fmt.Sprintf("topic%d", document%100),
		}
		if document%3 == 0 {
			terms = append(terms, "shared", "ranking")
		}
		if document%7 == 0 {
			terms = append(terms, "retrieval", "quality", "benchmark")
		}
		entries[document] = FulltextBatchEntry{ID: fmt.Sprintf("doc-%05d", document), Text: strings.Join(terms, " ")}
	}
	index.IndexBatch(entries)
	return index
}
