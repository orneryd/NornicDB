package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A SET clause applies its items in order; a run of property assignments to
// one variable is evaluated before any of it is written (Neo4j 5.26.30,
// #907).
func TestSetPropertyRunsMatchNeo4j(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		query string
		want  []interface{}
	}{
		{"run reads old values", "CREATE (n:W {a: 5}) SET n.a = 1, n.b = n.a + 1 RETURN n.a AS a, n.b AS b", []interface{}{int64(1), int64(6)}},
		{"run on a new node", "CREATE (n:W) SET n.a = 1, n.b = n.a + 1 RETURN n.b AS b", []interface{}{nil}},
		{"same key twice", "CREATE (n:W {a: 5}) SET n.a = n.a + 1, n.a = n.a + 10 RETURN n.a AS a", []interface{}{int64(15)}},
		{"separate SET clauses", "CREATE (n:W) SET n.a = 1 SET n.b = n.a + 1 RETURN n.b AS b", []interface{}{int64(2)}},
		{"map merge ends the run", "CREATE (n:W {a: 5}) SET n += {a: 1}, n.b = n.a RETURN n.b AS b", []interface{}{int64(1)}},
		{"map replace ends the run", "CREATE (n:W {a: 5}) SET n = {c: 1}, n.b = n.a RETURN n.b AS b, n.c AS c", []interface{}{nil, int64(1)}},
		{"label ends the run", "CREATE (n:W {a: 5}) SET n.a = 1, n:X, n.b = n.a RETURN n.b AS b", []interface{}{int64(1)}},
		{"map after a run", "CREATE (n:W {a: 5}) SET n.a = 1, n.b = n.a, n += {c: n.a} RETURN n.b AS b, n.c AS c", []interface{}{int64(5), int64(1)}},
		{"comprehension reads the old value", "CREATE (n:W {a: 5}) WITH n SET n.a = 2, n.b = [x IN [1] | n.a] RETURN n.b AS b", []interface{}{[]interface{}{int64(5)}}},
		{"another variable reads the new value", "CREATE (n:W {a: 5}), (m:W) SET n.a = 1, m.b = n.a RETURN m.b AS b", []interface{}{int64(1)}},
		{"in order across variables", "CREATE (n:W {a: 5}), (m:W) SET n.a = 1, m.b = n.a, n.c = n.a RETURN m.b AS b, n.c AS c", []interface{}{int64(1), int64(1)}},
		{"swap", "CREATE (n:W {a: 5}), (m:W {a: 7}) SET n.a = m.a, m.a = n.a RETURN n.a AS na, m.a AS ma", []interface{}{int64(7), int64(7)}},
		{"later items see earlier variables", "CREATE (n:W {a: 5}), (m:W) SET n.a = 1, m.b = n.a, n.a = 2, m.c = n.a RETURN m.b AS b, m.c AS c", []interface{}{int64(1), int64(2)}},
		{"relationship run", "CREATE (n:W)-[r:R {w: 1}]->(m:W) SET r.w = 2, r.v = r.w RETURN r.v AS v", []interface{}{int64(1)}},
		{"an alias is another variable", "CREATE (n:W {a: 5}) WITH n, n AS k SET n.a = 1, k.b = n.a RETURN k.b AS b", []interface{}{int64(1)}},
		{"match set", "MATCH (n:P) SET n.a = 1, n.b = n.a RETURN n.b AS b", []interface{}{int64(9)}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "set_runs"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:P {a: 9})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{testCase.want}, result.Rows)
		})
	}
}

// MERGE's SET applies the same way (applySetRuns).
func TestMergeSetPropertyRunsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_set_runs"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "MERGE (n:M {id: 1}) ON CREATE SET n.a = 5, n.b = n.a RETURN n.a AS a, n.b AS b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5), nil}}, result.Rows)

	result, err = exec.Execute(ctx, "MERGE (n:M {id: 1}) ON MATCH SET n.a = 1, n.b = n.a RETURN n.a AS a, n.b AS b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(5)}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (n:M {id: 1}) MERGE (n)-[r:T]->(m:M {id: 2}) ON CREATE SET r.w = 1, n.c = r.w, r.v = n.c RETURN n.c AS c, r.v AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)

	// Items on other variables apply too, in every MERGE form.
	result, err = exec.Execute(ctx, "MATCH (m:M {id: 2}) MERGE (n:M {id: 3}) ON CREATE SET n.a = 1, m.b = n.a RETURN m.b AS b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (m:M {id: 2}) MERGE (n:M {id: 3}) ON MATCH SET m.c = 7 RETURN m.c AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (m:M {id: 2}) MERGE (n:M {id: 4}) SET m.d = 8 RETURN m.d AS d", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(8)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (m:M {id: 2}) RETURN m.b AS b, m.c AS c, m.d AS d", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(7), int64(8)}}, result.Rows)
}

// A store failure while SET's writes are stored is the statement's error,
// on every route (persistSetEntities).
func TestSetRunsStoreFailures(t *testing.T) {
	base := newTestMemoryEngine(t)
	setup := NewStorageExecutor(storage.NewNamespacedEngine(base, "set_failures"))
	ctx := context.Background()
	_, err := setup.Execute(ctx, "CREATE (:M {id: 1}), (:M {id: 2})-[:T]->(:M {id: 3})", nil)
	require.NoError(t, err)

	failing := &updateErrorEngine{Engine: storage.NewNamespacedEngine(base, "set_failures")}
	exec := NewStorageExecutor(failing)
	for _, testCase := range []struct {
		name     string
		query    string
		nodeFail bool
	}{
		{"merge relationship, node write", "MATCH (a:M {id: 1}), (b:M {id: 2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.w = 1, a.c = 1", true},
		{"merge relationship, relationship write", "MATCH (a:M {id: 1}), (b:M {id: 2}) MERGE (a)-[r:R2]->(b) ON CREATE SET r.w = 1, a.c = 1", false},
		{"merge node, other node write", "MATCH (m:M {id: 2}) MERGE (n:M {id: 9}) ON CREATE SET n.a = 1, m.b = 1", true},
		{"merge node, relationship write", "MATCH (:M {id: 2})-[r:T]->() MERGE (n:M {id: 10}) ON CREATE SET n.a = 1, r.z = 2", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			failing.nodeErr, failing.edgeErr = nil, nil
			if testCase.nodeFail {
				failing.nodeErr = errors.New("node write failed")
			} else {
				failing.edgeErr = errors.New("edge write failed")
			}
			_, err := exec.Execute(ctx, testCase.query, nil)
			require.Error(t, err)
		})
	}
}

// A value SET can't store is the statement's error on MERGE's routes too.
func TestMergeSetRunsInvalidValue(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_set_invalid"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:M {id: 1}), (:M {id: 2})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (a:M {id: 1}), (b:M {id: 2}) MERGE (a)-[r:R]->(b) ON CREATE SET r.w = 1, a.c = {x: 1}",
		"MATCH (m:M {id: 2}) MERGE (n:M {id: 20}) ON CREATE SET n.a = 1, m.b = {x: 1}",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}

func TestSetClauseRunsAndTargets(t *testing.T) {
	require.Equal(t, []setRun{{variable: "n", text: "n.a = 1, , n.b = 2"}}, setClauseRuns("n.a = 1, , n.b = 2"))
	require.Equal(t, []setRun{
		{variable: "n", text: "n.a = 1, n.b = n.a"},
		{variable: "m", text: "m.c = 2"},
		{variable: "n", text: "n:L, n.d = 3"},
		{variable: "n", text: "n.e = 4"},
	}, setClauseRuns("n.a = 1, n.b = n.a, m.c = 2, n:L, n.d = 3 SET n.e = 4"))

	for _, testCase := range []struct {
		body string
		want bool
	}{
		{"n.a = 1, n.b = 'x, m.c'", true},
		{"n += {a: 1, b: [m, 2]}, n:L", true},
		{"n.a = 1 SET n.b = 2", true},
		{"n.a = 1, m.b = 2", false},
		{"n.a = 1 SET m.b = 2", false},
		{"nn.a = 1", false},
		{"n.a = (m.b)", true},
	} {
		require.Equal(t, testCase.want, setClauseTargetsOnly(testCase.body, "n"), testCase.body)
	}
}
