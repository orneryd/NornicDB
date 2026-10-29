package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFulltextIndex_CalculateIDFAndPhraseLimitBranches(t *testing.T) {
	idx := NewFulltextIndex(nil)

	// A document frequency above the document count has no IDF.
	idx.docCount = 1
	require.Equal(t, 0.0, idx.calculateIDFLocked(4))
	require.Greater(t, idx.calculateIDFLocked(1), 0.0)

	// PhraseSearch scoring + limit branch.
	idx.Index("p1", "quick brown fox", nil, nil, false)
	idx.Index("p2", "x quick brown fox", nil, nil, false)
	idx.Index("p3", "y y quick brown fox", nil, nil, false)
	got := idx.PhraseSearch("quick brown", 2)
	require.Len(t, got, 2)
	require.GreaterOrEqual(t, got[0].Score, got[1].Score)
}

func TestFulltextIndex_SaveNoCopyErrorBranch(t *testing.T) {
	idx := NewFulltextIndex(nil)
	idx.Index("doc-1", "hello world", nil, nil, false)

	parentFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(parentFile, []byte("x"), 0o644))

	err := idx.SaveNoCopy(filepath.Join(parentFile, "bm25"))
	require.Error(t, err)
}

func TestFulltextIndex_SearchPrefixAndGuardBranches(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "3")
	idx := NewFulltextIndex(nil)

	// Empty index and empty-token query guards.
	require.Nil(t, idx.Search("anything", 10))
	idx.Index("doc-empty", "! -- ...", nil, nil, false)
	require.Nil(t, idx.Search("! --", 10))

	// Prefix-match and result limiting path.
	idx.Index("doc-1", "searchablealice content", nil, nil, false)
	idx.Index("doc-2", "searchablebob content", nil, nil, false)
	idx.Index("doc-3", "searchablecharlie content", nil, nil, false)

	out := idx.Search("search", 2)
	require.Len(t, out, 2)
	require.GreaterOrEqual(t, out[0].Score, out[1].Score)
}
