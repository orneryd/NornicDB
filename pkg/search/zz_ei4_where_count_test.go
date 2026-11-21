package search

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// L570: how many times a hybrid search (keyword + vector, as db.retrieve with
// a query text and an embedding) evaluates its where filter per candidate.
func TestEI4HybridWhereEvaluationCount(t *testing.T) {
	for _, pass := range []int{100, 10} {
		t.Run(fmt.Sprintf("pass%d", pass), func(t *testing.T) {
			eng := newFilterTestEngine(t)
			ctx := context.Background()
			for i := 0; i < 200; i++ {
				_, err := eng.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%03d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"content": fmt.Sprintf("walrus notes %d", i), "i": int64(i)}, ChunkEmbeddings: [][]float32{{1, float32(i) / 1000, 0}}})
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
			resp, err := svc.Search(ctx, "walrus", []float32{1, 0, 0}, opts)
			require.NoError(t, err)
			total, most := 0, 0
			for _, c := range calls {
				total += c
				if c > most {
					most = c
				}
			}
			t.Logf("WHERECOUNT pass=%d%% results=%d candidatesEvaluated=%d evaluations=%d maxPerCandidate=%d", pass, len(resp.Results), len(calls), total, most)
			require.Equal(t, 1, most, "each candidate's filter is evaluated once per request")
		})
	}
}
