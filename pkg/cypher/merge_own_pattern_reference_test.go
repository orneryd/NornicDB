package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship MERGE whose end node or relationship map reads the
// pattern's own start (or end) node reads it once the node is bound, when
// matching and when creating, as Neo4j 5.26 does. It stored the text "a.foo"
// and never matched an existing pattern.
func TestMergeMapReadsOwnPattern(t *testing.T) {
	for _, tc := range []struct {
		statements []string
		want       [][][]interface{}
	}{
		{[]string{"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{
			"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MERGE (a:MgW {foo: 1})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{{int64(1)}}, {{int64(1)}}, {{int64(2)}}}},
		{[]string{
			"CREATE (:MgW {foo: 5})-[:MgT]->(:MgW {foo: 5})",
			"MERGE (a:MgW {foo: 5})-[:MgT]->(b:MgW {foo: a.foo}) RETURN b.foo AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{}, {{int64(5)}}, {{int64(2)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[r:MgT {w: a.x}]->(b:MgW) RETURN r.w AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{"MERGE (a:MgW {x: 2})-[:MgT]->(b:MgW {y: a.x * 10}) RETURN b.y AS v"}, [][][]interface{}{{{int64(20)}}}},
		{[]string{"MERGE (a:MgW {x: 3})-[r:MgT {w: a.x + b.y}]->(b:MgW {y: 4}) RETURN r.w AS v"}, [][][]interface{}{{{int64(7)}}}},
		{[]string{"MERGE (a:MgW {x: 1})<-[:MgT]-(b:MgW {y: a.x}) RETURN b.y AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{
			"CREATE (:MgW {x: 1})-[:MgT {w: 1}]->(:MgW)",
			"MERGE (a:MgW {x: 1})-[r:MgT {w: a.x}]->(b:MgW) RETURN r.w AS v",
			"MATCH (n:MgW) RETURN count(n) AS c",
		}, [][][]interface{}{{}, {{int64(1)}}, {{int64(2)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[:MgT {}]->(b:MgW {y: a.x}) RETURN b.y AS v"}, [][][]interface{}{{{int64(1)}}}},
		{[]string{"MERGE (a:MgW {x: 1})-[:MgT]->(b:`Mg{W`) RETURN labels(b) AS v"}, [][][]interface{}{{{[]interface{}{"Mg{W"}}}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_own_pattern"))
		for index, statement := range tc.statements {
			result, err := exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err, statement)
			want := tc.want[index]
			if len(want) == 0 {
				require.Empty(t, result.Rows, statement)
				continue
			}
			require.Equal(t, want, result.Rows, statement)
		}
	}
}

// A store error while finding a relationship MERGE's end nodes, read once
// or per start node, is the statement's error.
func TestMergeMapReadsOwnPatternStoreError(t *testing.T) {
	for _, statement := range []string{
		"MERGE (a:MgS {x: 1})-[:MgT]->(b:MgE {y: a.x}) RETURN b.y AS v",
		"MERGE (a:MgS {x: 1})-[:MgT]->(b:MgE) RETURN b AS v",
	} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_own_pattern_error")
		_, err := NewStorageExecutor(store).Execute(context.Background(), "CREATE (:MgS {x: 1})", nil)
		require.NoError(t, err)
		lookupErr := errors.New("end lookup failed")
		exec := NewStorageExecutor(&createMatchErrEngine{Engine: store, labelErrs: map[string]error{"MgE": lookupErr}})
		_, err = exec.Execute(context.Background(), statement, nil)
		require.ErrorIs(t, err, lookupErr, statement)
	}
}
