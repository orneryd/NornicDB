package search

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
)

func TestTextAnalyzerUsesCapturedBatchStemmer(t *testing.T) {
	var singleCalls atomic.Int64
	var batchCalls atomic.Int64
	analyzer := NewStemmedTextAnalyzer(stemmer.Registration{
		APIVersion: stemmer.APIVersion,
		ID:         "test.batch",
		Version:    "1.0.0",
		Digest:     strings.Repeat("a", 64),
		Stem: func(token string) string {
			singleCalls.Add(1)
			return token + "-single"
		},
		StemTokens: func(tokens []string) []string {
			batchCalls.Add(1)
			for i := range tokens {
				tokens[i] += "-batch"
			}
			return tokens
		},
	})

	require.Equal(t, []string{"alpha-batch", "beta-batch"}, analyzer.Analyze("Alpha beta"))
	require.Equal(t, int64(1), batchCalls.Load())
	require.Zero(t, singleCalls.Load())
}

func TestStemmedAnalyzerAppliesMultilingualFixturesToBM25V1AndV2(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		indexed string
		query   string
		stems   map[string]string
	}{
		{
			name:    "ukrainian",
			id:      "test.ukrainian",
			indexed: "Україною",
			query:   "України",
			stems:   map[string]string{"україна": "україн", "україни": "україн", "україною": "україн"},
		},
		{
			name:    "chinese",
			id:      "test.chinese",
			indexed: "检索",
			query:   "搜索",
			stems:   map[string]string{"检索": "搜", "搜索": "搜"},
		},
		{
			name:    "french",
			id:      "test.french",
			indexed: "mangeons",
			query:   "manger",
			stems:   map[string]string{"mangeons": "mang", "manger": "mang"},
		},
		{
			name:    "spanish",
			id:      "test.spanish",
			indexed: "hablando",
			query:   "hablar",
			stems:   map[string]string{"hablando": "habl", "hablar": "habl"},
		},
		{
			name:    "dutch",
			id:      "test.dutch",
			indexed: "huizen",
			query:   "huis",
			stems:   map[string]string{"huizen": "huis", "huis": "huis"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			analyzer := NewStemmedTextAnalyzer(stemmer.Registration{
				APIVersion: stemmer.APIVersion,
				ID:         test.id,
				Version:    "1.0.0",
				Digest:     strings.Repeat("b", 64),
				Stem: func(token string) string {
					if stemmed, ok := test.stems[token]; ok {
						return stemmed
					}
					return token
				},
			})

			v1 := NewFulltextIndexWithAnalyzer(analyzer)
			v1.Index("doc", test.indexed)
			require.Len(t, v1.Search(test.query, 10), 1)
			defaultV1 := NewFulltextIndex()
			defaultV1.Index("doc", test.indexed)
			require.Empty(t, defaultV1.Search(test.query, 10))

			v2 := NewFulltextIndexV2WithAnalyzer(analyzer)
			v2.Index("doc", test.indexed)
			require.Len(t, v2.Search(test.query, 10), 1)
			defaultV2 := NewFulltextIndexV2()
			defaultV2.Index("doc", test.indexed)
			require.Empty(t, defaultV2.Search(test.query, 10))
		})
	}
}
