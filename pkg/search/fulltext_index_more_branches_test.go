package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltextIndex_IndexAndBatchEdgeBranches(t *testing.T) {
	idx := NewFulltextIndex()

	// Index with no Unicode terms should not create a document.
	idx.Index("doc-empty", "! -- ...")
	require.Equal(t, 0, idx.Count())

	// Seed one doc, then batch-update through mixed edge cases.
	idx.Index("doc-1", "hello world")
	idx.IndexBatch(nil) // no-op branch
	idx.IndexBatch([]FulltextBatchEntry{
		{ID: "doc-1", Text: "! -- ..."}, // remove existing, then skip empty tokens
		{ID: "", Text: "ignored empty id"},
		{ID: "doc-2", Text: "alpha beta beta"},
	})

	require.Equal(t, 1, idx.Count())
	_, ok := idx.GetDocument("doc-1")
	require.False(t, ok)
	text, ok := idx.GetDocument("doc-2")
	require.True(t, ok)
	require.Equal(t, "alpha beta beta", text)
}

func TestFulltextIndex_CalculateIDFAndPhraseLimitBranches(t *testing.T) {
	idx := NewFulltextIndex()

	// Force BM25 IDF floor path (df > N makes raw expression negative).
	idx.docCount = 1
	idx.invertedIndex["term"] = map[string]int{
		"d1": 1, "d2": 1, "d3": 1, "d4": 1,
	}
	require.Equal(t, 0.0, idx.calculateIDF("term"))

	// PhraseSearch scoring + limit branch.
	idx.Index("p1", "quick brown fox")
	idx.Index("p2", "x quick brown fox")
	idx.Index("p3", "y y quick brown fox")
	got := idx.PhraseSearch("quick brown", 2)
	require.Len(t, got, 2)
	require.GreaterOrEqual(t, got[0].Score, got[1].Score)
}

func TestFulltextIndex_SaveNoCopyErrorBranch(t *testing.T) {
	idx := NewFulltextIndex()
	idx.Index("doc-1", "hello world")

	parentFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

	err := idx.SaveNoCopy(filepath.Join(parentFile, "bm25"))
	require.Error(t, err)
}

func TestFulltextIndex_SearchPrefixAndGuardBranches(t *testing.T) {
	idx := NewFulltextIndex()

	// Empty index and empty-token query guards.
	require.Nil(t, idx.Search("anything", 10))
	idx.Index("doc-empty", "! -- ...")
	require.Nil(t, idx.Search("! --", 10))

	// Prefix-match and result limiting path.
	idx.Index("doc-1", "searchablealice content")
	idx.Index("doc-2", "searchablebob content")
	idx.Index("doc-3", "searchablecharlie content")

	out := idx.Search("search", 2)
	require.Len(t, out, 2)
	require.GreaterOrEqual(t, out[0].Score, out[1].Score)
}
