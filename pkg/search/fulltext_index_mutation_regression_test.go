package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltextIndexScoresUseCurrentDocumentFrequency(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.IndexBatch([]FulltextBatchEntry{
		{ID: "rare", Text: "needle unique"},
		{ID: "common-a", Text: "needle common"},
		{ID: "common-b", Text: "common"},
	})

	before := idx.Search("unique", 10)
	require.Len(t, before, 1)

	// Persisted IDF is retained for wire-format compatibility, but query
	// scoring must derive it from the current corpus and posting list.
	idx.termIndex["unique"].IDF = 0
	idx.queryPlanCache.Clear()
	after := idx.Search("unique", 10)
	require.Equal(t, before, after)

	idx.Remove("common-b")
	require.Equal(t, "rare", idx.Search("unique", 1)[0].ID)

	idx.Index("rare", "replacement only")
	require.Empty(t, idx.Search("unique", 10))
	require.Equal(t, "rare", idx.Search("replacement", 1)[0].ID)

	path := t.TempDir() + "/fulltext.gob"
	require.NoError(t, idx.Save(path))
	reloaded := NewFulltextIndexV2()
	require.NoError(t, reloaded.Load(path))
	require.Equal(t, idx.Search("replacement", 10), reloaded.Search("replacement", 10))
}

func BenchmarkFulltextIndexGrowingVocabularyMutation(b *testing.B) {
	for range b.N {
		idx := NewFulltextIndexV2()
		for i := 0; i < 1_000; i++ {
			idx.Index(fmt.Sprintf("doc-%04d", i), fmt.Sprintf("shared unique-%04d", i))
		}
	}
}
