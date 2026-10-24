package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each timed query performs exactly evaluations correlated probes. The WHERE
// shape traverses two hops; the CALL shape imports a scalar. The control uses
// the same traversal and outer CALL without the EXISTS. Every execution uses
// a fresh nonce, including the warmup, so the result cache cannot answer it.
func BenchmarkExistsSubqueryEvaluationOwner(b *testing.B) {
	for _, evaluations := range []int{1000, 5000, 15000} {
		b.Run(fmt.Sprintf("evaluations_%d", evaluations), func(b *testing.B) {
			store := storage.NewNamespacedEngine(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "exists_bench")
			defer store.Close()
			e := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "doc"}})
			require.NoError(b, err)
			_, err = store.CreateNode(&storage.Node{ID: "target", Labels: []string{"Node"}})
			require.NoError(b, err)
			for i := 1; i <= evaluations; i++ {
				id := fmt.Sprint(i)
				source := "source-" + id
				_, err = store.CreateNode(&storage.Node{ID: storage.NodeID("own-" + id), Labels: []string{"Node"}, Properties: map[string]any{"document_id": "doc", "source_id": source}})
				require.NoError(b, err)
				_, err = store.CreateNode(&storage.Node{ID: storage.NodeID(source), Labels: []string{"Source"}, Properties: map[string]any{"id": source, "roles": []any{}}})
				require.NoError(b, err)
				require.NoError(b, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("l-" + id), StartNode: "doc", EndNode: storage.NodeID("own-" + id), Type: "REL", Properties: map[string]any{"source_id": source}}))
				require.NoError(b, store.CreateEdge(&storage.Edge{ID: storage.EdgeID("l2-" + id), StartNode: storage.NodeID("own-" + id), EndNode: "target", Type: "REL", Properties: map[string]any{"source_id": source}}))
			}
			_, err = e.Execute(ctx, "CREATE INDEX source_id FOR (s:Source) ON (s.id)", nil)
			require.NoError(b, err)
			prefix := "MATCH (d:Doc) WHERE $nonce IS NOT NULL CALL (d) { MATCH (d)-[l:REL]-(own:Node)-[l2:REL]-(s:Node) WHERE own.document_id=d.id"
			tail := " RETURN count(*) AS c } RETURN sum(c)"
			roles := " WHERE any(j IN range(0,size(coalesce(e.roles,[]))-1) WHERE e.roles[j] <> 'own')"
			queries := []struct{ name, query string }{
				{"correlated_where", prefix + " AND NOT EXISTS { MATCH (e:Source {id:l.source_id})" + roles + " }" + tail},
				{"per_row_call", "UNWIND range(1,$evaluations) AS i WITH i WHERE $nonce IS NOT NULL CALL (i) { RETURN NOT EXISTS { MATCH (e:Source {id:'source-'+toString(i)})" + roles + " } AS found } RETURN sum(CASE WHEN found THEN 1 ELSE 0 END)"},
				{"no_exists", prefix + tail},
			}
			var nonce int64
			for _, shape := range queries {
				b.Run(shape.name, func(b *testing.B) {
					params := map[string]any{"evaluations": int64(evaluations)}
					run := func() {
						nonce++
						params["nonce"] = nonce
						result, err := e.Execute(ctx, shape.query, params)
						require.NoError(b, err)
						require.Equal(b, [][]any{{int64(evaluations)}}, result.Rows)
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.ReportMetric(float64(evaluations), "evaluations/op")
				})
			}
		})
	}
}
