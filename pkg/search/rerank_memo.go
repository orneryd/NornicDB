package search

import (
	"context"
	"sync"
)

type rerankMemoContextKey struct{}

type rerankMemo struct {
	mu      sync.RWMutex
	results map[rerankMemoKey]rerankMemoEntry
}

type rerankMemoKey struct {
	query string
	id    string
}

type rerankMemoEntry struct {
	result  RerankResult
	include bool
}

func newRerankMemo() *rerankMemo {
	return &rerankMemo{results: make(map[rerankMemoKey]rerankMemoEntry)}
}

func withRerankMemo(ctx context.Context, memo *rerankMemo) context.Context {
	if memo == nil {
		return ctx
	}
	return context.WithValue(ctx, rerankMemoContextKey{}, memo)
}

func rerankMemoFromContext(ctx context.Context) *rerankMemo {
	if ctx == nil {
		return nil
	}
	memo, _ := ctx.Value(rerankMemoContextKey{}).(*rerankMemo)
	return memo
}

func (m *rerankMemo) get(query, id string) (RerankResult, bool, bool) {
	if m == nil {
		return RerankResult{}, false, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.results[rerankMemoKey{query: query, id: id}]
	return entry.result, ok, entry.include
}

func (m *rerankMemo) put(query string, candidates []RerankCandidate, results []RerankResult) {
	if m == nil || len(candidates) == 0 {
		return
	}
	returned := make(map[string]RerankResult, len(results))
	for _, result := range results {
		returned[result.ID] = result
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, candidate := range candidates {
		result, include := returned[candidate.ID]
		m.results[rerankMemoKey{query: query, id: candidate.ID}] = rerankMemoEntry{result: result, include: include}
	}
}
