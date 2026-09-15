package search

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
)

var benchmarkAnalyzerSink []string

const benchmarkAnalyzerText = "mangeons hablar huizen україною 检索 mangeons hablar huizen україною 检索"

func BenchmarkTextAnalyzerStemmerSingleFallback(b *testing.B) {
	analyzer := NewStemmedTextAnalyzer(stemmer.Registration{
		APIVersion: stemmer.APIVersion,
		ID:         "bench.single",
		Version:    "1.0.0",
		Digest:     strings.Repeat("e", 64),
		Stem: func(token string) string {
			return token
		},
	})
	for i := 0; i < b.N; i++ {
		benchmarkAnalyzerSink = analyzer.Analyze(benchmarkAnalyzerText)
	}
}

func BenchmarkTextAnalyzerStemmerBatch(b *testing.B) {
	analyzer := NewStemmedTextAnalyzer(stemmer.Registration{
		APIVersion: stemmer.APIVersion,
		ID:         "bench.batch",
		Version:    "1.0.0",
		Digest:     strings.Repeat("f", 64),
		Stem: func(token string) string {
			return token
		},
		StemTokens: func(tokens []string) []string {
			for i := range tokens {
				tokens[i] = tokens[i]
			}
			return tokens
		},
	})
	for i := 0; i < b.N; i++ {
		benchmarkAnalyzerSink = analyzer.Analyze(benchmarkAnalyzerText)
	}
}
