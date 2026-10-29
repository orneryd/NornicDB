package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type metadataOnlySeedIndex struct {
	bm25Index
	searchCalls int
}

func (i *metadataOnlySeedIndex) Search(string, int) []indexResult {
	i.searchCalls++
	return nil
}

func (i *metadataOnlySeedIndex) LexicalSeedHints(int, int) []LexicalSeedHint {
	return []LexicalSeedHint{{ID: "document", Rank: 0, Signature: 1}}
}

func TestLexicalSeedHintsPreserveRankAndTopicSignature(t *testing.T) {
	index := NewFulltextIndexV2WithAnalyzer(nil)
	index.Index("alpha-1", "alpha alpha shared", nil, nil, false)
	index.Index("alpha-2", "alpha shared", nil, nil, false)
	index.Index("beta-1", "beta beta shared", nil, nil, false)
	index.Index("beta-2", "beta shared", nil, nil, false)

	hints := index.LexicalSeedHints(8, 2)
	require.NotEmpty(t, hints)
	for i := range hints {
		require.Equal(t, uint32(i), hints[i].Rank)
		require.NotZero(t, hints[i].Signature)
	}
	byID := make(map[string]LexicalSeedHint, len(hints))
	for _, hint := range hints {
		byID[hint.ID] = hint
	}
	require.Equal(t, byID["alpha-1"].Signature, byID["alpha-2"].Signature)
	require.Equal(t, byID["beta-1"].Signature, byID["beta-2"].Signature)
	require.NotEqual(t, byID["alpha-1"].Signature, byID["beta-1"].Signature)
	require.Equal(t, hints, index.LexicalSeedHints(8, 2), "lexical metadata must be deterministic")
}

func TestLexicalSeedHintsSingleAndBatchParity(t *testing.T) {
	single := NewFulltextIndexV2WithAnalyzer(nil)
	batch := NewFulltextIndexV2WithAnalyzer(nil)
	var entries []FulltextBatchEntry
	for _, document := range []struct{ id, content string }{
		{"alpha-1", "alpha alpha shared"},
		{"alpha-2", "alpha shared"},
		{"beta-1", "beta beta shared"},
		{"beta-2", "beta shared"},
	} {
		single.Index(document.id, document.content, nil, nil, false)
		entries = append(entries, FulltextBatchEntry{ID: document.id, Text: document.content})
	}
	batch.IndexBatch(entries)
	for _, limits := range []struct{ terms, docs int }{{8, 2}, {1, 1}, {0, 2}, {8, 0}} {
		require.Equal(t, single.LexicalSeedHints(limits.terms, limits.docs), batch.LexicalSeedHints(limits.terms, limits.docs), "limits=%+v", limits)
	}
}

func TestHNSWBuildMetadataDoesNotExecuteBM25Queries(t *testing.T) {
	service := NewService(newTestMemoryEngine(t), 2, nil)
	index := &metadataOnlySeedIndex{bm25Index: NewFulltextIndexV2WithAnalyzer(nil)}

	hints := service.hnswLexicalSeedHints(index, 16)
	require.Contains(t, hints, vectorNodeID("document"))
	require.Zero(t, index.searchCalls)
}

func TestHNSWBuildIteratorPrioritizesRankedDocumentsAndAnnotatesTheirChunks(t *testing.T) {
	ids := []string{"other", nodeChunkVectorKey("doc-a", 1), "doc-a", "doc-b", nodeChunkVectorKey("doc-b", 1)}
	vectors := [][]float32{{1}, {2}, {3}, {4}, {5}}
	iterate := func(_ int, fn func([]string, [][]float32) error) error {
		return fn(ids, vectors)
	}
	hints := nodeLexicalHints{
		"doc-a": {ID: "doc-a", Rank: 1, Signature: 1},
		"doc-b": {ID: "doc-b", Rank: 0, Signature: 2},
	}
	var built []hnswBuildPair
	err := hnswVectorChunkIterator(iterate, hints)(2, func(batch []hnswBuildPair) error {
		built = append(built, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, built, len(ids))
	require.Equal(t, []string{"doc-b", "doc-a"}, []string{built[0].id, built[1].id})
	require.True(t, built[0].hasHint)
	require.True(t, built[1].hasHint)

	seen := make(map[string]hnswBuildPair, len(built))
	for _, pair := range built {
		seen[pair.id] = pair
	}
	require.True(t, seen[nodeChunkVectorKey("doc-a", 1)].hasHint)
	require.Equal(t, hints["doc-a"], seen[nodeChunkVectorKey("doc-a", 1)].hint)
	require.True(t, seen[nodeChunkVectorKey("doc-b", 1)].hasHint)
	require.False(t, seen["other"].hasHint)
}

func TestHNSWInMemoryBuildOrderUsesLexicalRank(t *testing.T) {
	index := NewVectorIndex(1)
	require.NoError(t, index.Add("doc-a", []float32{1}))
	require.NoError(t, index.Add("doc-b", []float32{1}))
	require.NoError(t, index.Add("other", []float32{1}))
	pairs := hnswOrderedVectorIndexPairs(index, nodeLexicalHints{
		"doc-a": {ID: "doc-a", Rank: 1},
		"doc-b": {ID: "doc-b", Rank: 0},
	})

	require.Equal(t, []string{"doc-b", "doc-a"}, []string{pairs[0].id, pairs[1].id})
	require.True(t, pairs[0].hasHint)
	require.True(t, pairs[1].hasHint)
}
