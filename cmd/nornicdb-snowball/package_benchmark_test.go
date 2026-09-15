package main

import (
	"sync"
	"testing"
)

type benchmarkSnowballEnv struct {
	current string
}

func (e *benchmarkSnowballEnv) SetCurrent(current string) { e.current = current }
func (e *benchmarkSnowballEnv) Current() string           { return e.current }

var (
	benchmarkBridgePool = sync.Pool{New: func() any { return &benchmarkSnowballEnv{} }}
	benchmarkTokens     = []string{"mangeons", "hablar", "huizen", "україною", "检索", "manger", "hablando", "huis"}
	benchmarkStringSink string
	benchmarkTokensSink []string
)

func benchmarkStem(*benchmarkSnowballEnv) {}

func benchmarkStemToken(token string) string {
	env := benchmarkBridgePool.Get().(*benchmarkSnowballEnv)
	env.SetCurrent(token)
	benchmarkStem(env)
	result := env.Current()
	env.SetCurrent("")
	benchmarkBridgePool.Put(env)
	return result
}

func benchmarkStemTokens(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}
	env := benchmarkBridgePool.Get().(*benchmarkSnowballEnv)
	for i, token := range tokens {
		env.SetCurrent(token)
		benchmarkStem(env)
		tokens[i] = env.Current()
	}
	env.SetCurrent("")
	benchmarkBridgePool.Put(env)
	return tokens
}

func BenchmarkGeneratedBridgePerTokenPoolRoundTrip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, token := range benchmarkTokens {
			benchmarkStringSink = benchmarkStemToken(token)
		}
	}
}

func BenchmarkGeneratedBridgeBatchPoolRoundTrip(b *testing.B) {
	tokens := make([]string, len(benchmarkTokens))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(tokens, benchmarkTokens)
		benchmarkTokensSink = benchmarkStemTokens(tokens)
	}
}
