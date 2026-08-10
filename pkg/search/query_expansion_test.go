package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedPassageResolver struct {
	text string
}

func (r fixedPassageResolver) ResolvePassages(_ context.Context, sources []ExpansionSource) ([]ExpansionSource, error) {
	for index := range sources {
		sources[index].Text = r.text
	}
	return sources, nil
}

func TestModifiedDice(t *testing.T) {
	t.Run("identical repeated bigrams are bounded", func(t *testing.T) {
		assert.Equal(t, 1.0, characterDice("aaaa", "aaaa"))
		assert.Equal(t, 1.0, wordBigramDice([]string{"a", "a", "a"}, []string{"a", "a", "a"}))
	})
	t.Run("character bigrams stay within tokens", func(t *testing.T) {
		assert.Equal(t, 0.0, characterDice("ab", "bc"))
		assert.Equal(t, -1.0, wordBigramDice([]string{"single"}, []string{"single"}))
	})
	t.Run("morphological variants match without semantic merging", func(t *testing.T) {
		assert.GreaterOrEqual(t, characterDice("reaction", "reactions"), 0.85)
		assert.Less(t, wordBigramDice([]string{"adverse", "reactions"}, []string{"adverse", "effects"}), 0.85)
	})
}

func TestDensePRF(t *testing.T) {
	expander := NewDensePRFDiceExpander(QueryExpansionConfig{
		MaxCandidates:  64,
		MaxTerms:       10,
		MaxPhraseWords: 3,
		UseDice:        true,
		DiceThreshold:  0.85,
		IDF: func(term string) float64 {
			if term == "general" || term == "include" {
				return 0.01
			}
			return 2
		},
	})
	result, err := expander.Expand(context.Background(), "ibuprofen side effects", []ExpansionSource{
		{VectorID: "a", SemanticRank: 1, SemanticScore: 0.9, Text: "NSAID nausea dizziness adverse reactions"},
		{VectorID: "b", SemanticRank: 2, SemanticScore: 0.8, Text: "NSAID nausea general include"},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, result.Sources)
	assert.LessOrEqual(t, len(result.Terms), 10)
	assert.Contains(t, result.Terms, "nsaid")
	assert.NotContains(t, result.Terms, "ibuprofen")
	assert.NotContains(t, result.Terms, "general")
	assert.NotContains(t, result.Terms, "include")
}

func TestDensePRF_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewDensePRFDiceExpander(QueryExpansionConfig{}).Expand(ctx, "query", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryExpansionCacheKeySuffix(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_ENABLED", "false")
	disabled := searchCacheKey("query", DefaultSearchOptions())
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_ENABLED", "true")
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_DICE_ENABLED", "false")
	withoutDice := searchCacheKey("query", DefaultSearchOptions())
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_DICE_ENABLED", "true")
	withDice := searchCacheKey("query", DefaultSearchOptions())
	assert.NotEqual(t, disabled, withoutDice)
	assert.NotEqual(t, withoutDice, withDice)
}

func TestRRFHybridSearch_QueryExpansion(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_ENABLED", "true")
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_SOURCE_TOP_K", "1")
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_TERMS", "1")
	t.Setenv("NORNICDB_SEARCH_QUERY_EXPANSION_MAX_PHRASE_WORDS", "1")

	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	service := NewServiceWithDimensions(engine, 2)
	semanticSource := &storage.Node{
		ID:              "semantic-source",
		Labels:          []string{"Document"},
		Properties:      map[string]any{"content": "unrelated vector source"},
		ChunkEmbeddings: [][]float32{{1, 0}},
	}
	lexicalTarget := &storage.Node{
		ID:         "lexical-target",
		Labels:     []string{"Document"},
		Properties: map[string]any{"content": "NSAID nausea guidance"},
	}
	_, err := engine.CreateNode(semanticSource)
	require.NoError(t, err)
	_, err = engine.CreateNode(lexicalTarget)
	require.NoError(t, err)
	require.NoError(t, service.IndexNode(semanticSource))
	require.NoError(t, service.IndexNode(lexicalTarget))
	service.SetPassageResolver(fixedPassageResolver{text: "NSAID nausea dizziness"})

	opts := DefaultSearchOptions()
	opts.Limit = 10
	response, err := service.rrfHybridSearch(context.Background(), "ibuprofen side effects", []float32{1, 0}, opts)
	require.NoError(t, err)
	assert.Equal(t, "ibuprofen side effects", response.Query)
	assert.Contains(t, resultIDs(response.Results), "lexical-target")
}

func resultIDs(results []SearchResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.ID)
	}
	return ids
}
