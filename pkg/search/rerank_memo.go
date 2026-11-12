package search

import (
	"context"
	"sync"
)

type rerankMemoContextKey struct{}

// rerankMemo is one ranked continuation's retained rerank work: the provider
// decision for each candidate it submitted, and the windows those candidates
// were admitted in.
type rerankMemo struct {
	mu        sync.RWMutex
	results   map[rerankMemoKey]rerankMemoEntry
	nextOrder map[string]uint64
	windows   map[string][][]string
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
	failed        bool
	providerOrder uint64
	returned      bool
}

func newRerankMemo() *rerankMemo {
	return &rerankMemo{
		results:   make(map[rerankMemoKey]rerankMemoEntry),
		nextOrder: make(map[string]uint64),
		windows:   make(map[string][][]string),
	}
}

func withRerankMemo(ctx context.Context, memo *rerankMemo) context.Context {
	if memo == nil {
		return ctx
	}
	return context.WithValue(ctx, rerankMemoContextKey{}, memo)
}

func rerankMemoFromContext(ctx context.Context) *rerankMemo {
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

// putUnranked retains a candidate's provider decision when the provider gave no
// result for it: a valid subset response omitted it, or the batch that held it
// failed. The decision is retained once per query, so a deeper pull rebuilds it
// instead of submitting the candidate - or its failed batch - again.
func (m *rerankMemo) putUnranked(query string, candidate RerankCandidate, failed bool) rerankMemoEntry {
	entry := rerankMemoEntry{id: candidate.ID, failed: failed}
	if m == nil {
		return entry
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := rerankMemoKey{query: query, id: candidate.ID}
	if existing, ok := m.results[key]; ok {
		return existing
	}
	m.results[key] = entry
	return entry
}

// acceptedWindows returns the candidate batches already admitted for query, in
// admission order. Each batch is a copy the caller owns.
func (m *rerankMemo) acceptedWindows(query string) [][]string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	accepted := m.windows[query]
	windows := make([][]string, len(accepted))
	for index := range accepted {
		windows[index] = append([]string(nil), accepted[index]...)
	}
	return windows
}

// acceptWindow records the candidates one provider batch was built from. A
// batch is recorded when its candidates are first admitted, so a deeper pull
// rebuilds that exact membership instead of re-forming a window over a longer
// prefix.
func (m *rerankMemo) acceptWindow(query string, ids []string) {
	if m == nil || len(ids) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.windows[query] = append(m.windows[query], append([]string(nil), ids...))
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
