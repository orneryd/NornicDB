package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Removing documents that share a term counts their postings dead instead
// of copying the term's list per removal; results, document frequencies and
// snapshots see live documents only (#826).
func TestFulltextIndexV2RemovalKeepsResultsAndCompacts(t *testing.T) {
	index := NewFulltextIndexV2()
	const n = 1000
	for i := 0; i < n; i++ {
		index.Index(fmt.Sprintf("doc-%04d", i), fmt.Sprintf("shared token%d", i))
	}
	for i := 0; i < n; i += 2 {
		index.Remove(fmt.Sprintf("doc-%04d", i))
	}
	require.Equal(t, n/2, index.Count())

	index.mu.RLock()
	shared := index.termIndex["shared"]
	require.Equal(t, n/2, shared.liveDocumentFrequency())
	require.LessOrEqual(t, len(shared.Postings), 2*shared.liveDocumentFrequency()+1, "the list is compacted once half of it is dead")
	_, removedTerm := index.termIndex["token0"]
	require.False(t, removedTerm, "a term with no live document is dropped")
	index.mu.RUnlock()

	results := index.Search("shared", n)
	require.Len(t, results, n/2)
	for _, result := range results {
		var number int
		_, err := fmt.Sscanf(result.ID, "doc-%04d", &number)
		require.NoError(t, err)
		require.Equal(t, 1, number%2, "only live documents are returned")
	}
	require.Len(t, index.Search("token1", 10), 1)
	require.Empty(t, index.Search("token0", 10))

	// Re-indexing a document replaces its postings.
	index.Index("doc-0001", "replaced")
	require.Empty(t, index.Search("token1", 10))
	require.Len(t, index.Search("replaced", 10), 1)

	// Both save paths write, and load keeps, live postings only.
	for _, save := range []func(string) error{index.Save, index.SaveNoCopy} {
		path := t.TempDir() + "/bm25"
		require.NoError(t, save(path))
		loaded := NewFulltextIndexV2()
		require.NoError(t, loaded.Load(path))
		require.Equal(t, index.Count(), loaded.Count())
		loaded.mu.RLock()
		require.Equal(t, n/2-1, len(loaded.termIndex["shared"].Postings))
		require.Zero(t, loaded.termIndex["shared"].dead)
		loaded.mu.RUnlock()
		require.Len(t, loaded.Search("shared", n), n/2-1)
	}
}

func BenchmarkFulltextIndexV2RemoveSharedTerm(b *testing.B) {
	for _, n := range []int{10_000, 40_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				index := NewFulltextIndexV2()
				for doc := 0; doc < n; doc++ {
					index.Index(fmt.Sprintf("doc-%d", doc), "shared value")
				}
				b.StartTimer()
				for doc := 0; doc < n; doc++ {
					index.Remove(fmt.Sprintf("doc-%d", doc))
				}
			}
		})
	}
}

func TestFulltextIndexV2SnapshotDropsRemovedPostings(t *testing.T) {
	index := NewFulltextIndexV2()
	index.applyV2Snapshot(bm25V2Snapshot{
		Documents:  map[string]string{"live": "kept"},
		DocIDToNum: map[string]uint32{"live": 1},
		DocNumToID: []string{"", "live"},
		DocLengths: []uint32{0, 1},
		TermIndex: map[string]*bm25TermState{
			"kept":  {Postings: []bm25Posting{{DocNum: 0, TF: 1}, {DocNum: 1, TF: 1}}},
			"gone":  {Postings: []bm25Posting{{DocNum: 0, TF: 1}}},
			"stray": {Postings: []bm25Posting{{DocNum: 9, TF: 1}}},
			"empty": nil,
		},
		DocCount: 1,
	})
	index.mu.RLock()
	defer index.mu.RUnlock()
	require.Len(t, index.termIndex, 1)
	require.Equal(t, []bm25Posting{{DocNum: 1, TF: 1}}, index.termIndex["kept"].Postings)
}
