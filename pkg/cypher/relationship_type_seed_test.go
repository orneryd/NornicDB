package cypher

import (
	"context"
	"testing"

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
