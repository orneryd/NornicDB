package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFulltextIndexV2_BasicLifecycle(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("doc1", "hello world distributed systems")
	idx.Index("doc2", "hello nornicdb graph database")

	results := idx.Search("hello graph", 10)
	require.NotEmpty(t, results)
	require.Equal(t, 2, idx.Count())

	idx.Remove("doc1")
	require.Equal(t, 1, idx.Count())
}

func TestFulltextIndexV2_PrefixBounded(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "2")
	t.Setenv("NORNICDB_BM25_PREFIX_MIN_LEN", "3")

	idx := NewFulltextIndexV2()
	idx.Index("a", "prescription refill reminder")
	idx.Index("b", "prescribe dosage reminder")
	idx.Index("c", "prescribed medicine")
	idx.Index("d", "pressure issue")

	results := idx.Search("pres", 10)
	require.NotEmpty(t, results)
}

func TestFulltextIndexV2_StableTopK(t *testing.T) {
	v2 := NewFulltextIndexV2()

	docs := []FulltextBatchEntry{
		{ID: "d1", Text: "graph database nornicdb embeddings"},
		{ID: "d2", Text: "prescription refill status and dosage"},
		{ID: "d3", Text: "where are my prescriptions and refill history"},
		{ID: "d4", Text: "medical translation memory and dictionary"},
		{ID: "d5", Text: "hybrid search bm25 and vector fusion"},
	}
	v2.IndexBatch(docs)

	query := "prescriptions refill"
	r1 := v2.Search(query, 3)
	r2 := v2.Search(query, 3)
	require.NotEmpty(t, r1)
	require.NotEmpty(t, r2)
	require.Equal(t, len(r1), len(r2))
	for i := range r1 {
		require.Equal(t, r1[i].ID, r2[i].ID)
	}
}

func TestFulltextIndexV2_SaveLoadAndMigrateV1(t *testing.T) {
	path := t.TempDir() + "/bm25"

	v2 := NewFulltextIndexV2()
	v2.Index("doc1", "hello world")
	v2.Index("doc2", "hello graph world")
	require.NoError(t, v2.Save(path))

	loaded := NewFulltextIndexV2()
	require.NoError(t, loaded.Load(path))
	require.Equal(t, 2, loaded.Count())
	require.NotEmpty(t, loaded.Search("hello", 10))

	legacyPath := t.TempDir() + "/bm25_legacy"
	legacySnap := bm25V1Snapshot{
		Version:   "1.0.0",
		Documents: map[string]string{"docA": "legacy migration path", "docB": "legacy bm25 test"},
		InvertedIndex: map[string]map[string]int{
			"legacy":    {"docA": 1, "docB": 1},
			"migration": {"docA": 1},
			"path":      {"docA": 1},
			"bm25":      {"docB": 1},
			"test":      {"docB": 1},
		},
		DocLengths:   map[string]int{"docA": 3, "docB": 3},
		AvgDocLength: 3,
		DocCount:     2,
	}
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(f).Encode(&legacySnap))
	require.NoError(t, f.Close())

	fromLegacy := NewFulltextIndexV2()
	require.NoError(t, fromLegacy.Load(legacyPath))
	require.Equal(t, 2, fromLegacy.Count())
	require.NotEmpty(t, fromLegacy.Search("legacy", 10))
}

func TestFulltextIndexV2_DirtySaveNoCopyPhraseClear(t *testing.T) {
	idx := NewFulltextIndexV2()
	require.False(t, idx.IsDirty())

	idx.Index("doc1", "the quick brown fox jumps")
	idx.Index("doc2", "quick brown is common phrase")
	require.True(t, idx.IsDirty())

	phrase := idx.PhraseSearch("quick brown", 10)
	require.Len(t, phrase, 2)

	path := filepath.Join(t.TempDir(), "bm25v2")
	require.NoError(t, idx.SaveNoCopy(path))
	require.False(t, idx.IsDirty())

	idx.Clear()
	require.Equal(t, 0, idx.Count())
	require.True(t, idx.IsDirty())

	// Empty clear path should still be safe.
	idx.Clear()
	require.Equal(t, 0, idx.Count())
}

func TestFulltextIndexV2_LoadDecodeFailureClears(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bm25v2_corrupt")
	require.NoError(t, os.WriteFile(path, []byte("not-msgpack"), 0644))

	idx := NewFulltextIndexV2()
	idx.Index("doc1", "hello world")
	require.NoError(t, idx.Load(path))
	require.Equal(t, 0, idx.Count())
}

func TestFulltextIndexV2_PersistenceErrorAndMigrationBranches(t *testing.T) {
	idx := NewFulltextIndexV2()
	require.NoError(t, idx.Load(filepath.Join(t.TempDir(), "missing")))

	badParent := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(badParent, []byte("x"), 0644))
	require.Error(t, idx.Load(filepath.Join(badParent, "child")))
	require.Error(t, idx.Save(filepath.Join(badParent, "child")))

	legacyPath := filepath.Join(t.TempDir(), "legacy_bad_version")
	file, err := os.Create(legacyPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(file).Encode(&bm25V1Snapshot{Version: "9.0.0"}))
	require.NoError(t, file.Close())
	idx.Index("doc", "content")
	require.NoError(t, idx.Load(legacyPath))
	require.Equal(t, 0, idx.Count())

	nilMapsPath := filepath.Join(t.TempDir(), "legacy_nil_maps")
	file, err = os.Create(nilMapsPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(file).Encode(&bm25V1Snapshot{Version: "1.0.0"}))
	require.NoError(t, file.Close())
	require.NoError(t, idx.Load(nilMapsPath))
	require.Equal(t, 0, idx.Count())

	legacyEdgePath := filepath.Join(t.TempDir(), "legacy_edges")
	file, err = os.Create(legacyEdgePath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(file).Encode(&bm25V1Snapshot{
		Version:   "1.0.0",
		Documents: map[string]string{"good": "alpha", "neg": "beta"},
		InvertedIndex: map[string]map[string]int{
			"empty": {},
			"bad":   {"missing": 1, "good": 0},
			"good":  {"good": 70000},
		},
		DocLengths: map[string]int{"good": 1, "neg": -3},
	}))
	require.NoError(t, file.Close())
	require.NoError(t, idx.Load(legacyEdgePath))
	require.Equal(t, 2, idx.Count())
	require.NotEmpty(t, idx.Search("good", 10))

	idx.applyV2Snapshot(bm25V2Snapshot{Version: bm25V2FormatVersion, DocLengths: []uint32{2, 3}})
	require.Equal(t, int64(5), idx.totalDocLength)
}

func TestFulltextIndexV2_MinIntHelper(t *testing.T) {
	require.Equal(t, 1, minInt(1, 2))
	require.Equal(t, -5, minInt(-5, 3))
	require.Equal(t, 7, minInt(7, 7))
}

// ---------------------------------------------------------------------------
// BM25 IDF calculation – Okapi BM25 formula: log(1 + (N - df + 0.5)/(df + 0.5))
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_CalculateIDFLocked(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Zero df or zero docCount → 0
	require.Equal(t, float64(0), idx.calculateIDFLocked(0))
	require.Equal(t, float64(0), idx.calculateIDFLocked(-1))

	idx.docCount = 0
	require.Equal(t, float64(0), idx.calculateIDFLocked(1))

	// Standard BM25 IDF: 10 docs, term appears in 3 → log(1 + (10-3+0.5)/(3+0.5))
	idx.docCount = 10
	idf := idx.calculateIDFLocked(3)
	require.Greater(t, idf, float64(0))
	// With N=10, df=3: log(1 + 7.5/3.5) ≈ log(1 + 2.1428) ≈ log(3.1428) ≈ 1.145
	require.InDelta(t, 1.145, idf, 0.01)

	// Rare term (df=1) should have higher IDF than common term (df=9)
	rare := idx.calculateIDFLocked(1)
	common := idx.calculateIDFLocked(9)
	require.Greater(t, rare, common, "rare terms must have higher IDF per BM25")
}

// ---------------------------------------------------------------------------
// Lexicon insert/remove – sorted slice maintenance
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_LexiconInsertRemove(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Insert in non-sorted order — lexicon must stay sorted
	idx.insertLexiconTermLocked("banana")
	idx.insertLexiconTermLocked("apple")
	idx.insertLexiconTermLocked("cherry")

	require.Equal(t, []string{"apple", "banana", "cherry"}, idx.lexicon)

	// Duplicate insert should be a no-op
	idx.insertLexiconTermLocked("banana")
	require.Equal(t, []string{"apple", "banana", "cherry"}, idx.lexicon)

	// Remove middle element
	idx.removeLexiconTermLocked("banana")
	require.Equal(t, []string{"apple", "cherry"}, idx.lexicon)

	// Remove non-existent term should be a no-op
	idx.removeLexiconTermLocked("banana")
	require.Equal(t, []string{"apple", "cherry"}, idx.lexicon)

	// Remove from edges
	idx.removeLexiconTermLocked("apple")
	require.Equal(t, []string{"cherry"}, idx.lexicon)

	idx.removeLexiconTermLocked("cherry")
	require.Empty(t, idx.lexicon)

	// Remove from empty lexicon should not panic
	idx.removeLexiconTermLocked("nothing")
	require.Empty(t, idx.lexicon)
}

// ---------------------------------------------------------------------------
// topKMinScore – min-heap threshold for top-K scoring
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_TopKMinScore(t *testing.T) {
	// k <= 0 → 0
	require.Equal(t, float64(0), topKMinScore(map[uint32]float64{1: 5.0}, 0))

	// Fewer items than k → 0
	require.Equal(t, float64(0), topKMinScore(map[uint32]float64{1: 5.0}, 2))

	// Exactly k items → returns the minimum score
	scores := map[uint32]float64{1: 3.0, 2: 1.0, 3: 5.0}
	minScore := topKMinScore(scores, 3)
	require.InDelta(t, 1.0, minScore, 0.001)

	// More items than k → returns the k-th highest score threshold
	scores = map[uint32]float64{1: 10.0, 2: 20.0, 3: 30.0, 4: 5.0, 5: 15.0}
	threshold := topKMinScore(scores, 3)
	// Top-3 are 30, 20, 15 → min of top-3 is 15
	require.InDelta(t, 15.0, threshold, 0.001)
}

// ---------------------------------------------------------------------------
// topKFromScores – deterministic top-K extraction
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_TopKFromScores(t *testing.T) {
	// k <= 0 → nil
	require.Nil(t, topKFromScores(map[uint32]float64{1: 5.0}, 0))

	// Empty scores → nil
	require.Nil(t, topKFromScores(map[uint32]float64{}, 5))

	// Normal case: extract top 2 from 4 items
	scores := map[uint32]float64{1: 10.0, 2: 5.0, 3: 20.0, 4: 15.0}
	top2 := topKFromScores(scores, 2)
	require.Len(t, top2, 2)
	// Results should be in descending score order
	require.InDelta(t, 20.0, top2[0].score, 0.001)
	require.InDelta(t, 15.0, top2[1].score, 0.001)
}

// ---------------------------------------------------------------------------
// BM25 V2 – LexicalSeedDocIDs returns deterministic seed candidates
// LexicalSeedDocIDs(maxTerms, docsPerTerm int) → []string (doc IDs)
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_LexicalSeedDocIDs(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("doc1", "graph database nornicdb")
	idx.Index("doc2", "relational database postgres")
	idx.Index("doc3", "graph embeddings neural")

	seeds := idx.LexicalSeedDocIDs(10, 10)
	require.NotEmpty(t, seeds)

	// Deterministic: second call should produce identical results
	seeds2 := idx.LexicalSeedDocIDs(10, 10)
	require.Equal(t, len(seeds), len(seeds2))
	for i := range seeds {
		require.Equal(t, seeds[i], seeds2[i])
	}
}

func TestFulltextIndexV2LexicalSeedMinimumDocumentFrequency(t *testing.T) {
	t.Setenv("NORNICDB_BM25_IDF_MIN_DOC_FREQ", "3")
	idx := NewFulltextIndexV2()
	idx.Index("d1", "shared rareone")
	idx.Index("d2", "shared raretwo")
	idx.Index("d3", "shared rarethree")

	require.Equal(t, []string{"d1", "d2", "d3"}, idx.LexicalSeedDocIDs(10, 10))
}

// ---------------------------------------------------------------------------
// BM25 V2 – PhraseSearch determinism
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_PhraseSearchDeterministic(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("doc1", "the quick brown fox jumps over lazy dog")
	idx.Index("doc2", "quick brown bread is delicious")
	idx.Index("doc3", "the lazy brown cat sleeps")

	// "quick brown" appears as exact phrase in doc1 and doc2
	r1 := idx.PhraseSearch("quick brown", 10)
	r2 := idx.PhraseSearch("quick brown", 10)
	require.Equal(t, len(r1), len(r2))
	for i := range r1 {
		require.Equal(t, r1[i].ID, r2[i].ID)
	}
}

// ---------------------------------------------------------------------------
// BM25 V2 – IndexBatch and Remove
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_IndexBatchAndRemove(t *testing.T) {
	idx := NewFulltextIndexV2()

	batch := []FulltextBatchEntry{
		{ID: "b1", Text: "hello world"},
		{ID: "b2", Text: "hello nornicdb"},
		{ID: "b3", Text: "world of graphs"},
	}
	idx.IndexBatch(batch)
	require.Equal(t, 3, idx.Count())

	// Re-indexing same ID should update, not duplicate
	idx.Index("b1", "hello updated world")
	require.Equal(t, 3, idx.Count())

	// Remove should decrease count
	idx.Remove("b2")
	require.Equal(t, 2, idx.Count())

	// Remove non-existent should be a no-op
	idx.Remove("nonexistent")
	require.Equal(t, 2, idx.Count())

	// Search should still find remaining docs
	results := idx.Search("hello", 10)
	require.NotEmpty(t, results)
}

// ---------------------------------------------------------------------------
// BM25 V2 – Search with no matching terms
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_SearchNoMatch(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("doc1", "graph database")

	results := idx.Search("zzzzunknownterm", 10)
	require.Empty(t, results)
}

// ---------------------------------------------------------------------------
// BM25 V2 – Empty index edge cases
// ---------------------------------------------------------------------------

func TestFulltextIndexV2_EmptyIndexOperations(t *testing.T) {
	idx := NewFulltextIndexV2()

	require.Equal(t, 0, idx.Count())
	require.Empty(t, idx.Search("anything", 10))
	require.Empty(t, idx.PhraseSearch("any phrase", 10))
	require.Empty(t, idx.LexicalSeedDocIDs(10, 10))

	// Remove from empty should not panic
	idx.Remove("nonexistent")
	require.Equal(t, 0, idx.Count())
}
