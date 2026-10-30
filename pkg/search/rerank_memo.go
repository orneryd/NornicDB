package search

import (
	"context"
	"sync"
)

type rerankMemoContextKey struct{}

type rerankMemo struct {
	mu        sync.RWMutex
	results   map[rerankMemoKey]rerankMemoEntry
	nextOrder map[string]uint64
}

type rerankMemoKey struct {
	query string
	id    string
}

type rerankMemoEntry struct {
	id            string
	crossScore    float64
	finalScore    float64
	filtered      bool
	providerOrder uint64
	returned      bool
}

func newRerankMemo() *rerankMemo {
	return &rerankMemo{
		results:   make(map[rerankMemoKey]rerankMemoEntry),
		nextOrder: make(map[string]uint64),
	}
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

func (m *rerankMemo) get(query, id string) (rerankMemoEntry, bool) {
	if m == nil {
		return rerankMemoEntry{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.results[rerankMemoKey{query: query, id: id}]
	return entry, ok
}

func (m *rerankMemo) put(query string, candidate RerankCandidate, result RerankResult, returned bool) rerankMemoEntry {
	entry := rerankMemoEntry{
		id:         candidate.ID,
		crossScore: result.CrossScore,
		finalScore: result.FinalScore,
		filtered:   result.Filtered,
		returned:   returned,
	}
	if m == nil {
		return entry
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := rerankMemoKey{query: query, id: candidate.ID}
	if existing, ok := m.results[key]; ok {
		return existing
	}
	if returned {
		entry.providerOrder = m.nextOrder[query]
		m.nextOrder[query]++
	}
	m.results[key] = entry
	return entry
}

// bind recreates the provider result for the current candidate pool. Memo
// state is identity-keyed: provider scores, filtering and provider order are
// stable, while pool positions and candidate scores always come from this run.
func (e rerankMemoEntry) bind(candidate RerankCandidate, index int) RerankResult {
	return RerankResult{
		ID:           e.id,
		Content:      candidate.Content,
		OriginalRank: index + 1,
		BiScore:      candidate.Score,
		CrossScore:   e.crossScore,
		FinalScore:   e.finalScore,
		Filtered:     e.filtered,
	}
}
