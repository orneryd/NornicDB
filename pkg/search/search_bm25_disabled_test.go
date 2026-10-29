package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisabledBM25Index_NoOpContract(t *testing.T) {
	var idx disabledBM25Index
	var iface bm25Index = idx

	idx.Index("doc-1", "content", nil, nil, false)
	idx.Remove("doc-1")
	idx.Clear()
	idx.IndexBatch([]FulltextBatchEntry{{ID: "doc-2", Text: "hello"}})
	// Exercise the same no-op mutators via interface dispatch too.
	iface.Index("doc-3", "content", nil, nil, false)
	iface.Remove("doc-3")
	iface.Clear()
	iface.IndexBatch([]FulltextBatchEntry{{ID: "doc-4", Text: "world"}})
	idx.Index("doc-1", "hello world", nil, nil, false)
	idx.Remove("doc-1")
	idx.Clear()
	idx.IndexBatch([]FulltextBatchEntry{{ID: "doc-2", Text: "batch text"}})

	require.Nil(t, idx.Search("hello", 10))
	require.Nil(t, idx.Search("hello", 5))
	require.Nil(t, idx.SearchAllowed("hello", 5, nil))
	require.Nil(t, idx.PhraseSearch("hello world", 10))
	require.Nil(t, idx.PhraseSearch("hello world", 5))
	doc, ok := idx.GetDocument("doc-1")
	require.False(t, ok)
	require.Equal(t, "", doc)
	require.Nil(t, idx.LexicalSeedDocIDs(5, 3))
	require.Nil(t, idx.LexicalSeedDocIDs(3, 2))
	require.Equal(t, 0, idx.Count())
	require.NoError(t, idx.Save(""))
	require.NoError(t, idx.SaveNoCopy(""))
	require.NoError(t, idx.Load(""))
	require.NoError(t, idx.Save("ignored"))
	require.NoError(t, idx.SaveNoCopy("ignored"))
	require.NoError(t, idx.Load("ignored"))
	require.False(t, idx.IsDirty())
}
