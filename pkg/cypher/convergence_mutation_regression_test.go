package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newConvergenceExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "convergence")
	return NewStorageExecutor(store), context.Background()
}

func requireSingleValue(t *testing.T, result *ExecuteResult, want interface{}) {
	t.Helper()
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	require.Equal(t, want, result.Rows[0][0])
}

func TestRelationshipSetExpressionsUseRelationshipScope(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:O {id:2}), (:I {sku:'a1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (o:O {id:2}), (i:I {sku:'a1'}) CREATE (o)-[:HAS {n:5}]->(i)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = h.n + 1 RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(6))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 10 RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(10))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 2 * h.n RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(20))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.m = toString(h.n) + 'x' RETURN h.m AS m", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, "20x")

	readback, err := exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(:I) RETURN h.n AS n, h.m AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(20), "20x"}}, readback.Rows)
}

func TestSetMapMergeEvaluatesBoundExpressions(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:1, name:'n1'}), (:A {id:2, name:'n2'}), (:A {id:3, name:'n3'}), (:A {id:4, name:'n4'})", nil)
	require.NoError(t, err)
	rows := []interface{}{
		map[string]interface{}{"id": int64(1), "name": "n1"},
		map[string]interface{}{"id": int64(2), "name": "n2"},
	}
	result, err := exec.Execute(ctx, "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1), "n1"}, {int64(2), "n2"}}, result.Rows)

	result, err = exec.Execute(ctx, "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name + 'x'} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1), "n1x"}, {int64(2), "n2x"}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (a:A {id: 3}) SET a += {name: a.name + 'y', id2: a.id * 2} RETURN a.name AS n, a.id2 AS id2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"n3y", int64(6)}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (a:A {id: 4}) SET a += {name: toUpper(a.name), n2: 1 + 2} RETURN a.name AS n, a.n2 AS n2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"N4", int64(3)}}, result.Rows)
}

func TestRelationshipMergeAppliesCreateAndMatchAssignments(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:P {name:'Ann'}), (:P {name:'Dee'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 RETURN r.w AS w", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(1))

	result, err = exec.Execute(ctx, "MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON MATCH SET r.w = r.w + 1 RETURN r.w AS w", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(2))

	readback, err := exec.Execute(ctx, "MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) RETURN r.w AS w, count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, readback.Rows)
}

func TestRemovePropertyThenSetLabelPreservesMatchedScope(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:P {name:'Ann'}), (:P {name:'Bob'}), (:P {name:'Cid'}), (:P {name:'Dee', city:'Riga'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.name AS n, p.city AS city, labels(p) AS l", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Dee", nil, []interface{}{"P", "VIP"}}}, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH (p:VIP) RETURN count(p) AS vip", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(1))
}

func TestAssigningNullRemovesNodeProperty(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:I {sku:'a1', price:10.5, qty:3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (i:I {sku:'a1'}) SET i.price = null RETURN i.price AS p, keys(i) AS k", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Nil(t, result.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"sku", "qty"}, result.Rows[0][1])

	readback, err := exec.Execute(ctx, "MATCH (i:I {sku:'a1'}) RETURN keys(i) AS k", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"sku", "qty"}, readback.Rows[0][0])
}

func TestSetAddsChainedLabels(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:4})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:A {id:4}) SET a:Extra:Hot RETURN labels(a) AS l", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])

	result, err = exec.Execute(ctx, "MATCH (a:A {id:4}) SET a:Extra, a:Hot RETURN labels(a) AS l", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])
}

func TestMutationExpressionsAndClauseCompositionInExplicitTransactions(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
		_, err = exec.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		return result
	}

	run("CREATE (:P {name:'Ann'}), (:P {name:'Dee', city:'Riga'}), (:A {id:1, name:'n1'}), (:A {id:4})", nil)
	run("MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) CREATE (a)-[:KNOWS {w:1}]->(d)", nil)

	result := run("MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) SET r.w = r.w + 1 RETURN r.w AS w", nil)
	requireSingleValue(t, result, int64(2))

	rows := []interface{}{map[string]interface{}{"id": int64(1), "name": "updated"}}
	result = run("UNWIND $rows AS row MERGE (a:A {id:row.id}) SET a += {name:row.name} RETURN a.name AS name", map[string]interface{}{"rows": rows})
	requireSingleValue(t, result, "updated")

	result = run("MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:LIKES]->(d) ON CREATE SET r.weight = 3 RETURN r.weight AS weight", nil)
	requireSingleValue(t, result, int64(3))

	result = run("MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.city AS city, labels(p) AS labels", nil)
	require.Nil(t, result.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"P", "VIP"}, result.Rows[0][1])

	result = run("MATCH (a:A {id:1}) SET a.name = null RETURN keys(a) AS keys", nil)
	require.ElementsMatch(t, []interface{}{"id"}, result.Rows[0][0])

	result = run("MATCH (a:A {id:4}) SET a:Extra:Hot RETURN labels(a) AS labels", nil)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])
}

func TestSetEvaluationFailureRollsBackEarlierAssignments(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:1})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (a:A {id:1}) SET a.transient = 1, a += {broken:} RETURN a", nil)
	require.Error(t, err)

	readback, err := exec.Execute(ctx, "MATCH (a:A {id:1}) RETURN a.transient AS transient, keys(a) AS keys", nil)
	require.NoError(t, err)
	require.Nil(t, readback.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"id"}, readback.Rows[0][1])
}
