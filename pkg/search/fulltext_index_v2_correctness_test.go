package search

import (
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFulltextIndexV2DefaultSearchUsesExactTerms(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "")
	idx := NewFulltextIndexV2()
	idx.Index("exact", "kinase")
	idx.Index("prefix-only", "kinases kinases kinases")

	results := idx.Search("kinase", 10)

	require.Equal(t, []string{"exact"}, resultIDs(results))
}

func TestFulltextIndexV2PrefixSearchRemainsExplicitlyAvailable(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "4")
	idx := NewFulltextIndexV2()
	idx.Index("prefix-only", "kinases")

	results := idx.Search("kinase", 10)

	require.Equal(t, []string{"prefix-only"}, resultIDs(results))
}

func TestFulltextIndexV2OptimizedSearchMatchesExhaustiveScoring(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "0")
	random := rand.New(rand.NewSource(20260828))
	vocabulary := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta"}
	idx := NewFulltextIndexV2()
	for document := 0; document < 200; document++ {
		terms := make([]string, 1+random.Intn(30))
		for position := range terms {
			terms[position] = vocabulary[random.Intn(len(vocabulary))]
		}
		idx.Index(documentID(document), strings.Join(terms, " "))
	}

	for _, query := range []string{"alpha beta", "gamma delta epsilon", "zeta alpha theta"} {
		actual := idx.Search(query, 17)
		expected := exhaustiveBM25V2Search(idx, query, 17)
		require.Len(t, actual, len(expected))
		seen := make(map[string]struct{}, len(actual))
		for position := range actual {
			require.InDelta(t, expected[position].Score, actual[position].Score, 1e-12, "query %q rank %d", query, position)
			_, duplicate := seen[actual[position].ID]
			require.False(t, duplicate, "query %q duplicate result %q", query, actual[position].ID)
			seen[actual[position].ID] = struct{}{}
		}
	}
}

func TestFulltextIndexV2EqualScoresUseDocumentIDOrder(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "0")
	lexical := NewFulltextIndexV2()
	lexical.Index("doc-a", "shared term")
	lexical.Index("doc-b", "shared term")
	lexical.Index("doc-c", "shared term")
	require.True(t, lexical.docIDsLexicalByNum)
	require.Equal(t, []string{"doc-a"}, resultIDs(lexical.Search("shared", 1)))

	idx := NewFulltextIndexV2()
	idx.Index("doc-c", "shared term")
	idx.Index("doc-a", "shared term")
	idx.Index("doc-b", "shared term")
	require.False(t, idx.docIDsLexicalByNum)

	for iteration := 0; iteration < 100; iteration++ {
		require.Equal(t, []string{"doc-a"}, resultIDs(idx.Search("shared", 1)))
		results := idx.Search("shared", 3)
		require.Equal(t, []string{"doc-a", "doc-b", "doc-c"}, resultIDs(results))
		for _, result := range results[1:] {
			require.Equal(t, results[0].Score, result.Score)
		}
	}
}

func TestDocIDsAreLexicalByNumber(t *testing.T) {
	require.True(t, docIDsAreLexicalByNumber([]string{"doc-a", "", "doc-b"}))
	require.False(t, docIDsAreLexicalByNumber([]string{"doc-c", "doc-a", "doc-b"}))
}

func TestFulltextIndexV2LoadDerivesDocumentIDOrder(t *testing.T) {
	path := t.TempDir() + "/bm25"
	idx := NewFulltextIndexV2()
	idx.Index("doc-c", "shared")
	idx.Index("doc-a", "shared")
	require.NoError(t, idx.Save(path))

	loaded := NewFulltextIndexV2()
	require.NoError(t, loaded.Load(path))
	require.False(t, loaded.docIDsLexicalByNum)
	require.Equal(t, []string{"doc-a"}, resultIDs(loaded.Search("shared", 1)))
}

func TestFulltextIndexV2RareQueryUsesSparseScratch(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.docCount = 100_000
	idx.avgDocLength = 1
	idx.docLengths = make([]uint32, idx.docCount)
	idx.docLengths[0] = 1
	idx.docNumToID = []string{"rare-document"}
	idx.termIndex["rare"] = &bm25TermState{
		Postings: []bm25Posting{{DocNum: 0, TF: 1}},
		IDF:      1,
	}

	require.Equal(t, []string{"rare-document"}, resultIDs(idx.Search("rare", 1)))

	require.False(t, shouldUseDenseBM25Scores(len(idx.docLengths), 1))
	scratch := idx.getScoreScratch(false, 1)
	require.Zero(t, cap(scratch.scores), "rare queries must not allocate corpus-sized score arrays")
	require.Zero(t, cap(scratch.generations), "rare queries must not allocate corpus-sized generation arrays")
	idx.putScoreScratch(scratch)
}

func TestFulltextIndexV2AdaptiveScratchPolicy(t *testing.T) {
	require.False(t, shouldUseDenseBM25Scores(10_000_000, 1))
	require.False(t, shouldUseDenseBM25Scores(100_000, bm25DenseMinPostingVisits-1))
	require.True(t, shouldUseDenseBM25Scores(100_000, 12_500))

	idx := NewFulltextIndexV2()
	idx.docLengths = make([]uint32, bm25MaxPooledDenseDocSlots+1)
	scratch := idx.getScoreScratch(true, len(idx.docLengths))
	require.Len(t, scratch.scores, len(idx.docLengths))
	idx.putScoreScratch(scratch)
	require.Nil(t, scratch.scores)
	require.Nil(t, scratch.generations)
	require.Nil(t, scratch.touched)
}

func TestFulltextIndexV2PooledScoresDoNotLeakAcrossQueries(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("alpha", "alpha alpha")
	idx.Index("beta", "beta beta")

	require.Equal(t, []string{"alpha"}, resultIDs(idx.Search("alpha", 1)))
	require.Equal(t, []string{"beta"}, resultIDs(idx.Search("beta", 1)))
	require.Equal(t, []string{"alpha"}, resultIDs(idx.Search("alpha", 1)))
}

func TestFulltextIndexV2MatchesCanonicallyEquivalentUnicode(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "0")
	idx := NewFulltextIndexV2()
	idx.Index("accented", "cafe\u0301")

	require.Equal(t, []string{"accented"}, resultIDs(idx.Search("CAFÉ", 10)))
}

func TestTokenizePreservesLanguageNeutralUnicodeTerms(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		expected []string
	}{
		{name: "Latin", text: "Hello CAFÉ", expected: []string{"hello", "café"}},
		{name: "Greek", text: "ΑΘΗΝΑ κόσμος", expected: []string{"αθηνα", "κόσμοσ"}},
		{name: "Cyrillic", text: "МОСКВА поиск", expected: []string{"москва", "поиск"}},
		{name: "Arabic", text: "مرحبا بالعالم", expected: []string{"مرحبا", "بالعالم"}},
		{name: "Devanagari", text: "नमस्ते दुनिया", expected: []string{"नमस्ते", "दुनिया"}},
		{name: "CJK", text: "東京 検索", expected: []string{"東京", "検索"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, tokenize(test.text))
		})
	}
}

func TestFulltextIndexV2ConcurrentColdQueries(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "0")
	idx := NewFulltextIndexV2()
	idx.Index("document", "common retrieval term")

	const workers = 32
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer waitGroup.Done()
			<-start
			results := idx.Search("common missing"+documentID(worker), 1)
			require.Equal(t, []string{"document"}, resultIDs(results))
		}()
	}
	close(start)
	waitGroup.Wait()
}

func TestServiceFulltextPropertyProjection(t *testing.T) {
	service := NewService(storage.NewMemoryEngine())
	service.SetFulltextProperties([]string{"title", "text", "title", ""})
	node := &storage.Node{
		ID:     "physical-id",
		Labels: []string{"BEIRDocument"},
		Properties: map[string]any{
			"beir_id": "metadata-id",
			"title":   "Canonical Title",
			"text":    "Canonical body",
		},
	}

	require.Equal(t, "Canonical Title Canonical body", service.extractSearchableText(node))
	require.Equal(t, []string{"title", "text"}, service.FulltextProperties())
	require.Contains(t, service.composeBM25BuildSettings(), "props=title,text")

	defaultService := NewService(storage.NewMemoryEngine())
	require.Contains(t, defaultService.composeBM25BuildSettings(), "props=*")
	require.NotEqual(t, defaultService.composeBM25BuildSettings(), service.composeBM25BuildSettings())
}

func exhaustiveBM25V2Search(idx *FulltextIndexV2, query string, limit int) []indexResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	weightedTerms := idx.expandAndWeightTermsLocked(tokenize(query))
	scores := make(map[uint32]float64)
	for _, weightedTerm := range weightedTerms {
		for _, posting := range weightedTerm.postings {
			documentLength := idx.docLengths[posting.DocNum]
			if documentLength == 0 {
				continue
			}
			termFrequency := float64(posting.TF)
			numerator := termFrequency * (bm25K1 + 1)
			denominator := termFrequency + bm25K1*(1-bm25B+bm25B*(float64(documentLength)/idx.avgDocLength))
			scores[posting.DocNum] += weightedTerm.weight * weightedTerm.idf * (numerator / denominator)
		}
	}

	results := make([]indexResult, 0, len(scores))
	for documentNumber, score := range scores {
		documentID := idx.docNumToID[documentNumber]
		if documentID != "" {
			results = append(results, indexResult{ID: documentID, Score: score})
		}
	}
	sort.Slice(results, func(left, right int) bool {
		if results[left].Score == results[right].Score {
			return results[left].ID < results[right].ID
		}
		return results[left].Score > results[right].Score
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

func resultIDs(results []indexResult) []string {
	ids := make([]string, len(results))
	for index, result := range results {
		ids[index] = result.ID
	}
	return ids
}

func documentID(number int) string {
	const digits = "0123456789"
	if number == 0 {
		return "0"
	}
	var reversed [20]byte
	position := len(reversed)
	for number > 0 {
		position--
		reversed[position] = digits[number%10]
		number /= 10
	}
	return string(reversed[position:])
}
