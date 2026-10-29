package search

import (
	"fmt"
	"testing"
)

// buildFulltextBatchFixture creates the same fresh index used by the batch
// correctness test and the ingest benchmark.
func buildFulltextBatchFixture(entries []FulltextBatchEntry) *FulltextIndex {
	index := NewFulltextIndex(nil)
	index.IndexBatch(entries)
	return index
}

// BenchmarkFulltextBatchIngest exercises the 250-document batch size used by
// the index builder, with a fresh index for each batch.
func BenchmarkFulltextBatchIngest(b *testing.B) {
	entries := make([]FulltextBatchEntry, fulltextIndexChunkSize)
	for i := range entries {
		entries[i] = FulltextBatchEntry{ID: fmt.Sprintf("node-%d", i), Text: fmt.Sprintf("northwind product %d supplier stock order quantity", i)}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		index := buildFulltextBatchFixture(entries)
		if index.Count() != len(entries) {
			b.Fatal("missing indexed documents")
		}
	}
}
