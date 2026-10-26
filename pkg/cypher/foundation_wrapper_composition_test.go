package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The finite grammar crosses every permutation of namespace, WAL and tracing,
// plus the single-decorator forms. Seed through the namespace before wrapping:
// nil WAL is a read-only adapter here, not a fabricated mutation publisher.
func TestFoundationPublicWrapperComposition(t *testing.T) {
	orders := []string{"n", "tn", "wn", "ntw", "nwt", "tnw", "twn", "wnt", "wtn"}
	for _, namespace := range []string{"system", "tenant"} {
		for _, order := range orders {
			for _, indexed := range []bool{false, true} {
				for _, bound := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/index=%t/bound=%t", namespace, order, indexed, bound), func(t *testing.T) {
						base := newTestMemoryEngine(t)
						seed := storage.NewNamespacedEngine(base, namespace)
						for id := 1; id <= 2; id++ {
							labels := []string{"WrapperRecord"}
							_, err := seed.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("record-%d", id)), Labels: labels, Properties: map[string]any{"id": int64(id)}})
							require.NoError(t, err)
						}
						if indexed {
							require.NoError(t, NewStorageExecutor(seed).addPropertyIndex(context.Background(), "wrapper_record_id", "WrapperRecord", []string{"id"}))
						}
						var view storage.Engine = base
						for i := len(order) - 1; i >= 0; i-- {
							switch order[i] {
							case 'n':
								view = storage.NewNamespacedEngine(view, namespace)
							case 't':
								view = storage.NewTracedEngine(view)
							case 'w':
								view = storage.NewWALEngine(view, nil)
							}
						}
						node, err := seed.GetNode("record-1")
						require.NoError(t, err)
						params := map[string]any{"node": node}
						query := "MATCH (n:WrapperRecord) WHERE n.id>0 RETURN n.id ORDER BY n.id LIMIT 1"
						if bound {
							query = "WITH $node AS n MATCH (n:WrapperRecord) RETURN n.id"
						}
						executor := NewStorageExecutor(view)
						// Public admission is distinct from the matcher: system MATCH refuses;
						// the same tenant statement returns the ordinary record through each stack.
						public, err := executor.Execute(context.Background(), query, params)
						if namespace == "system" {
							requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
						} else {
							require.NoError(t, err)
							want := [][]any{{int64(1)}}
							require.Equal(t, want, public.Rows)
						}
						// Reproduce #1621's unbound matcher consumer as well. Public system
						// admission otherwise masks the recorded WAL(Traced(...)) refusal.
						result, err := executor.executeWithoutTransaction(withQueryParams(context.Background(), params), query, strings.ToUpper(query))
						require.NoError(t, err)
						want := [][]any{{int64(1)}}
						require.Equal(t, want, result.Rows)
					})
				}
			}
		}
	}
}
