package search

// Close-out lane L3 (v1.5.2 verification campaign): bug list row 24 / ledger
// BUG24. A hybrid retrieve (query text and embedding, as db.retrieve with a
// where filter) evaluated its where filter more than once per candidate:
// on b6d8e762a 230 evaluations for 200 candidates (max 2) with every candidate
// passing, and 610 (max 5) when 10% pass. db.retrieve's where becomes
// SearchOptions.Where (pkg/cypher/call_rag.go runSearchRequest), so the count
// is taken at that seam. Test-side only.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func closeoutL3WhereCount(t *testing.T, pass int, query string, embedding []float32) (results, candidates, total, most int) {
	t.Helper()
	eng := newFilterTestEngine(t)
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_, err := eng.CreateNode(t.Context(), &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("nornic:n%03d", i)),
			Labels:          []string{"Doc"},
			Properties:      map[string]any{"content": fmt.Sprintf("walrus notes %d", i), "i": int64(i)},
			ChunkEmbeddings: [][]float32{{1, float32(i) / 1000, 0}},
		})
		require.NoError(t, err)
	}
	svc := NewService(eng, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	var mu sync.Mutex
	calls := map[storage.NodeID]int{}
	opts := DefaultSearchOptions()
	opts.Limit = 10
	opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
		mu.Lock()
		calls[node.ID]++
		mu.Unlock()
		i, _ := node.Properties["i"].(int64)
		return int(i)%(100/pass) == 0, nil
	}
	resp, err := svc.Search(ctx, query, embedding, opts)
	require.NoError(t, err)
	for _, c := range calls {
		total += c
		if c > most {
			most = c
		}
	}
	return len(resp.Results), len(calls), total, most
}

func TestCloseoutL3_BUG24_HybridWhereEvaluationCount(t *testing.T) {
	t.Run("control_keyword_only", func(t *testing.T) {
		results, candidates, total, most := closeoutL3WhereCount(t, 100, "walrus", nil)
		t.Logf("CLOSEOUT_L3 BUG24 control keyword-only results=%d candidatesEvaluated=%d evaluations=%d maxPerCandidate=%d", results, candidates, total, most)
		require.Greater(t, candidates, 0, "the counting seam is reached")
		require.Greater(t, results, 0)
	})
	for _, pass := range []int{100, 10} {
		pass := pass
		t.Run(fmt.Sprintf("hybrid_pass%d", pass), func(t *testing.T) {
			results, candidates, total, most := closeoutL3WhereCount(t, pass, "walrus", []float32{1, 0, 0})
			t.Logf("CLOSEOUT_L3 BUG24 WHERECOUNT pass=%d%% results=%d candidatesEvaluated=%d evaluations=%d maxPerCandidate=%d", pass, results, candidates, total, most)
			require.Greater(t, candidates, 0, "the counting seam is reached")
			require.Greater(t, results, 0)
			require.Equal(t, 1, most, "each candidate's where filter is evaluated once per request")
		})
	}
}
