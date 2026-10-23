package cypher

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieveRelationships' and db.retrieveGraph's where is checked on
// relationship candidates before limit: the candidate bound as relationship,
// its endpoints as start and end, node null, with the statement's parameters
// and the calling row's variables; one CASE predicate serves both kinds, and
// admitted relationships ranked behind many rejected ones aren't cut.
func TestRetrieveWhereFiltersRelationshipCandidates(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)

	// 100 rejected relationships rank first (short text); the two admitted
	// ones rank last (the same term in a longer text).
	_, err := exec.Execute(ctx, `CREATE (:C {id: 'in1'}), (:C {id: 'in2'}), (:C {id: 'out1'}), (:C {id: 'out2'}),
		(:Doc {name: 'current', content: 'zephyrine', current: true}), (:Doc {name: 'old', content: 'zephyrine', current: false})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (x:C {id: 'out1'}), (y:C {id: 'out2'}) UNWIND range(1, 100) AS i
		CREATE (x)-[:R {n: i, reason: 'zephyrine'}]->(y)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:C {id: 'in1'}), (b:C {id: 'in2'}) UNWIND [1001, 1002] AS i
		CREATE (a)-[:R {n: i, reason: 'zephyrine with a much longer explanation of the late payment terms and the bridge'}]->(b)`, nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Eventually(t, func() bool {
		result, err := exec.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine', limit: 200}) YIELD relationship RETURN count(*) AS c", nil)
		return err == nil && result.Rows[0][0] == int64(102)
	}, 5*time.Second, 20*time.Millisecond)

	ids := map[string]interface{}{"ids": []interface{}{"in1", "in2"}}
	numbers := func(statement string, params map[string]interface{}) []int64 {
		t.Helper()
		result, err := exec.Execute(ctx, statement, params)
		require.NoError(t, err, statement)
		var got []int64
		for _, row := range result.Rows {
			got = append(got, row[0].(int64))
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		return got
	}

	// A parameter, the endpoints and node null.
	for _, where := range []string{
		"start.id IN $ids AND end.id IN $ids",
		"node IS NULL AND relationship.n > 1000",
		"EXISTS { (start)-[:R]->(end) WHERE end.id = 'in2' }",
	} {
		require.Equal(t, []int64{1001, 1002}, numbers(
			"CALL db.retrieveRelationships({query: 'zephyrine', limit: 2, where: $w}) YIELD relationship RETURN relationship.n",
			map[string]interface{}{"w": where, "ids": ids["ids"]}), where)
	}

	// The calling row's variables.
	require.Equal(t, []int64{1001, 1002}, numbers(`MATCH (c:C) WHERE c.id STARTS WITH 'in' WITH collect(c.id) AS scope
		CALL (scope) { CALL db.retrieveRelationships({query: 'zephyrine', limit: 2, where: 'start.id IN scope'}) YIELD relationship RETURN relationship.n AS n }
		RETURN n`, nil))

	// A correlated Fabric record's bindings, as a node candidate has them.
	exec.fabricRecordBindings = map[string]interface{}{"scope": []interface{}{"in1"}}
	require.Equal(t, []int64{1001, 1002}, numbers(
		"CALL db.retrieveRelationships({query: 'zephyrine', limit: 2, where: 'start.id IN scope'}) YIELD relationship RETURN relationship.n", nil))
	exec.fabricRecordBindings = nil

	// One predicate over both kinds in db.retrieveGraph.
	for _, where := range []string{
		"CASE WHEN node IS NOT NULL THEN relationship IS NULL AND start IS NULL AND node.current ELSE start.id IN $ids AND relationship.n > 1000 END",
		"(node IS NOT NULL AND node.current) OR (relationship IS NOT NULL AND start.id IN $ids AND end.id IN $ids)",
		"CASE WHEN node IS NOT NULL THEN node.current ELSE start.id IN $ids AND end.id IN $ids END",
	} {
		result, err := exec.Execute(ctx, `CALL db.retrieveGraph({query: 'zephyrine', limit: 3, where: $w})
			YIELD kind, node, relationship RETURN kind, coalesce(node.name, toString(relationship.n)) AS name`,
			map[string]interface{}{"w": where, "ids": ids["ids"]})
		require.NoError(t, err, where)
		got := map[string][]string{}
		for _, row := range result.Rows {
			got[row[0].(string)] = append(got[row[0].(string)], row[1].(string))
		}
		sort.Strings(got["relationship"])
		require.Equal(t, map[string][]string{"node": {"current"}, "relationship": {"1001", "1002"}}, got, where)
	}
}
