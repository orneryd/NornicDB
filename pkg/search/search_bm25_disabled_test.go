package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBM25EngineNormalizationAndFactory(t *testing.T) {
	require.Equal(t, BM25EngineV1, normalizeBM25Engine("v1"))
	require.Equal(t, BM25EngineV2, normalizeBM25Engine(" V2 "))
	require.Equal(t, BM25EngineV2, normalizeBM25Engine("unknown"))

	idx, engine := newBM25Index("v1")
	require.Equal(t, BM25EngineV1, engine)
	require.NotNil(t, idx)

	idx, engine = newBM25Index("garbage")
	require.Equal(t, BM25EngineV2, engine)
	require.NotNil(t, idx)
}

func TestDisabledBM25Index_NoOpContract(t *testing.T) {
	var idx disabledBM25Index
	var iface bm25Index = idx

	idx.Index("doc-1", "content")
	idx.Remove("doc-1")
	idx.Clear()
	idx.IndexBatch([]FulltextBatchEntry{{ID: "doc-2", Text: "hello"}})
	// Exercise the same no-op mutators via interface dispatch too.
	iface.Index("doc-3", "content")
	iface.Remove("doc-3")
	iface.Clear()
	iface.IndexBatch([]FulltextBatchEntry{{ID: "doc-4", Text: "world"}})

	require.Nil(t, idx.Search("hello", 10))
	require.Nil(t, idx.PhraseSearch("hello world", 10))
	doc, ok := idx.GetDocument("doc-1")
	require.False(t, ok)
	require.Equal(t, "", doc)
	require.Nil(t, idx.LexicalSeedDocIDs(5, 3))
	require.Equal(t, 0, idx.Count())
	require.NoError(t, idx.Save(""))
	require.NoError(t, idx.Load(""))
	require.False(t, idx.IsDirty())
}
