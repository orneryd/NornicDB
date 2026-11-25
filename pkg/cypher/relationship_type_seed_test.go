package cypher

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A one-hop match from an anonymous, unlabelled start node over typed
// relationships starts from those relationships' endpoints, read from the
// type index, with the same rows as a node scan:
// both directions and undirected, several types, relationship properties,
// a self-loop, a deleted relationship and a typed match with no relationship.
func TestRelationshipTypeSeedMatches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relseed"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(1, 50) AS i CREATE (:Plain {k: i})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (a:A {k: 'a'})-[:RELATION {id: 'r1', kind: 'merged_into'}]->(b:B {k: 'b'}),
		(b)-[:RELATION {id: 'r2', kind: 'same_as'}]->(c:C {k: 'c'}), (c)-[:OTHER {id: 'o1'}]->(a), (a)-[:RELATION {id: 'loop'}]->(a),
		(c)-[:RELATION {id: 'gone'}]->(a)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH ()-[r:RELATION {id: 'gone'}]->() DELETE r", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH ()-[r:RELATION {id: 'r1'}]->() RETURN r.kind AS v":                            {{"merged_into"}},
		"UNWIND range(1, 3) AS i MATCH ()-[r:RELATION {id: $id}]->() RETURN count(r) AS v":   {{int64(3)}},
		"MATCH (x)-[r:RELATION]->(y) RETURN x.k + '>' + y.k AS v ORDER BY v":                 {{"a>a"}, {"a>b"}, {"b>c"}},
		"MATCH (x)<-[r:RELATION]-(y) RETURN x.k + '<' + y.k AS v ORDER BY v":                 {{"a<a"}, {"b<a"}, {"c<b"}},
		"MATCH (x)-[r:RELATION]-(y) RETURN x.k + '-' + y.k AS v ORDER BY v":                  {{"a-a"}, {"a-b"}, {"b-a"}, {"b-c"}, {"c-b"}},
		"MATCH (x)-[r:RELATION|OTHER]->(y) WHERE r.id <> 'loop' RETURN r.id AS v ORDER BY v": {{"o1"}, {"r1"}, {"r2"}},
		"MATCH ()-[r:RELATION {id: 'gone'}]->() RETURN count(r) AS v":                        {{int64(0)}},
		"MATCH ()-[r:NONE]->() RETURN count(r) AS v":                                         {{int64(0)}},
		"MATCH ()-[r:RELATION]->(y:C) RETURN r.id AS v":                                      {{"r2"}},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"id": "r1"})
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}

func TestRelationshipTypeSeedWhereEqualityRetirement(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			ctx := context.Background()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retire")
			exec := NewStorageExecutor(store)
			nodes := make([]*storage.Node, 100)
			for i := range nodes {
				nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))}
			}
			require.NoError(t, store.BulkCreateNodes(ctx, nodes))
			edges := make([]*storage.Edge, 1000)
			for i := range edges {
				edges[i] = &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: nodes[i%100].ID, EndNode: nodes[(i+1)%100].ID, Type: "AUTO", Properties: map[string]interface{}{"bucket": int64(i % 10)}}
			}
			require.NoError(t, store.BulkCreateEdges(ctx, edges))
			run := func(query string, params map[string]interface{}) *ExecuteResult {
				t.Helper()
				result, err := exec.Execute(ctx, query, params)
				require.NoError(t, err, query)
				return result
			}
			none := run("PROFILE MATCH ()-[r:AUTO]->() WHERE r.bucket = $bucket SET r.retired = true RETURN count(r)", map[string]interface{}{"bucket": int64(99)})
			require.Equal(t, [][]interface{}{{int64(0)}}, none.Rows)
			plan, ok := none.Metadata["plan"].(*ExecutionPlan)
			require.True(t, ok)
			require.Zero(t, plan.TotalDBHits, "no-match equality must not load endpoints or expand adjacency")
			sparse := run("MATCH ()-[r:AUTO]->() WHERE $bucket = r.bucket SET r.retired = true RETURN count(r)", map[string]interface{}{"bucket": int64(7)})
			require.Equal(t, [][]interface{}{{int64(100)}}, sparse.Rows)
			require.Equal(t, [][]interface{}{{int64(100)}}, run("MATCH ()-[r:AUTO]->() WHERE r.retired = true RETURN count(r)", nil).Rows)
			require.Equal(t, [][]interface{}{{int64(200)}}, run("MATCH ()-[r:AUTO]->() WHERE r.bucket = 8 OR r.retired = true RETURN count(r)", nil).Rows)
			require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH ()-[r:AUTO]->() WHERE r.bucket = null RETURN count(r)", nil).Rows)
		})
	}
}

func TestRelationshipTypeSeedWhereEqualityPendingWrites(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "retire_tx"))
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE (:Endpoint {id: 1})-[:AUTO {bucket: 1}]->(:Endpoint {id: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if exec.txContext != nil && exec.txContext.active {
			_, _ = exec.handleRollback(context.Background(), nil)
		}
	})
	run := func(query string) int64 {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		value, ok := result.Rows[0][0].(int64)
		require.True(t, ok)
		return value
	}
	require.Equal(t, int64(1), run("MATCH ()-[r:AUTO]->() WHERE r.bucket = 1 SET r.bucket = 2 RETURN count(r)"))
	require.Equal(t, int64(0), run("MATCH ()-[r:AUTO]->() WHERE r.bucket = 1 RETURN count(r)"))
	require.Equal(t, int64(1), run("MATCH ()-[r:AUTO]->() WHERE r.bucket = 2 RETURN count(r)"))
	_, err = exec.Execute(ctx, "CREATE (:Endpoint {id: 3})-[:AUTO {bucket: 2}]->(:Endpoint {id: 4})", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), run("MATCH ()-[r:AUTO]->() WHERE r.bucket = 2 RETURN count(r)"))
}
