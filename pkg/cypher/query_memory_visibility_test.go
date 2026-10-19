package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every whole-edge source variant must apply the query endpoint policy before
// projection, paths or aggregation can observe an internal node's relationship.
func TestQueryMemoryStreamedRelationshipEndpointVisibility(t *testing.T) {
	for _, namespace := range []string{"test", "system"} {
		t.Run(namespace, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
			exec.cache = nil
			ctx := context.Background()
			if namespace == "system" {
				_, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN r", nil)
				require.ErrorContains(t, err, "Graph access is not supported on the system database")
				return
			}
			_, err := exec.Execute(ctx, `CREATE (a:Visible {id:'a'}), (b:Visible {id:'b'}),
				(h:_meta {id:'hidden'}), (m:Visible:_meta {id:'mixed'})
				CREATE (a)-[:REL {tag:'public'}]->(b), (h)-[:REL {tag:'start'}]->(b),
				(a)-[:REL {tag:'end'}]->(h), (m)-[:REL {tag:'mixed'}]->(b),
				(h)-[:OTHER {tag:'both'}]->(m)`, nil)
			require.NoError(t, err)
			for _, tc := range []struct {
				query string
				rows  int
			}{
				{"MATCH ()-[r]->() RETURN r", 1},
				{"MATCH ()-[r:REL]->() RETURN r.tag", 1},
				{"MATCH ()-[r:REL|OTHER]->() RETURN startNode(r), endNode(r)", 1},
				{"MATCH ()<-[r]-() RETURN r.tag", 1},
				{"MATCH ()-[r]-() RETURN r.tag", 2},
				{"MATCH (a)-[r]->(b) RETURN a, b", 1},
				{"MATCH (a:Visible)-[r]->(b:Visible) RETURN r.tag", 1},
				{"MATCH p=()-[r]->() RETURN p", 1},
				{"MATCH ()-[r]->() RETURN startNode(r).id, endNode(r).id", 1},
				{"MATCH ()-[r]->() WITH r RETURN count(r)", 1},
			} {
				t.Run(tc.query, func(t *testing.T) {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err)
					if tc.query == "MATCH ()-[r]->() WITH r RETURN count(r)" {
						require.Equal(t, [][]interface{}{{int64(tc.rows)}}, result.Rows)
					} else {
						require.Len(t, result.Rows, tc.rows)
					}
				})
			}
			// Bolt-style pull must filter before the first batch as well as
			// the materializing Execute adapter used above.
			run := executeStreamed(ctx, exec, "MATCH ()-[r]->() RETURN startNode(r).id, endNode(r).id", 1)
			require.True(t, run.stream.streamed())
			var streamed [][]interface{}
			for {
				batch, done := run.stream.Next(1)
				streamed = append(streamed, batch...)
				if done {
					break
				}
			}
			<-run.stream.Done()
			require.NoError(t, run.err)
			require.Equal(t, [][]interface{}{{"a", "b"}}, streamed)
			// Pending writes use the same endpoint checks through transaction
			// storage forwarding; internal labels must survive projection.
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (a:Visible {id:'a'}) CREATE (h:_meta {id:'pending'})-[:REL]->(a)", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() RETURN r.tag", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"public"}}, result.Rows)
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		})
	}
}
