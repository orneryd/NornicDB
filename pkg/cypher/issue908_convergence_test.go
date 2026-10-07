package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue908Convergence(t *testing.T) {
	tests := []struct {
		name      string
		setup     []string
		query     string
		want      interface{}
		wantError bool
	}{
		{"merge null", nil, "UNWIND [{k: null}, {k: null}] AS row MERGE (n:P {k: row.k}) RETURN count(*) AS c", nil, true},
		{"merge set order", []string{"UNWIND [1] AS x MERGE (n:P {k: x}) ON CREATE SET n.v = 1 SET n.v = 2 RETURN count(*) AS c"}, "MATCH (n:P) RETURN n.v", int64(2), false},
		{"optional count", nil, "UNWIND [1, 2] AS x OPTIONAL MATCH (m:Missing {k: x}) MERGE (n:P {k: x}) RETURN count(m) AS c", int64(0), false},
		{"property count", []string{"CREATE (:L {a: 1, b: 1}), (:L {a: 1, b: 2})"}, "MATCH (n:L) WHERE n.a = n.b RETURN count(n) AS c", int64(1), false},
		{"xor count", []string{"CREATE (:A {id: 1}), (:A {id: 2})"}, "MATCH (a:A) WHERE a.id = 1 XOR a.id = 2 RETURN count(*) AS c", int64(2), false},
		{"path precedence", []string{"CREATE (:L {a: 1, b: 0, c: 0})-[:R]->(:M), (:L {a: 0, b: 2, c: 3})-[:R]->(:M)"}, "MATCH (n:L)-[:R]->(m) WHERE n.a = 1 OR n.b = 2 AND n.c = 3 RETURN count(*) AS c", int64(2), false},
		{"with precedence", []string{"CREATE (:O {flag: true}), (:L)-[:R]->(:M {y: 0, z: 0})"}, "MATCH (o:O) WITH o.flag AS f MATCH (a:L)-[:R]->(b) WHERE f = true OR b.y = 2 AND b.z = 3 RETURN count(*) AS c", int64(1), false},
		{"seed properties", []string{"CREATE (:A {k: 1, x: 5})-[:R]->(:B {n: 'ok'}), (:A {k: 2, x: 5})-[:R]->(:B {n: 'bad'})"}, "MATCH (a:A {k: 1})-[:R]->(b) WHERE a.x = 5 RETURN b.n AS n", "ok", false},
		{"seed labels", []string{"CREATE INDEX FOR (a:A) ON (a.x)", "CREATE (:A {x: 1})-[:R]->(:M)"}, "MATCH (a:A:B {x: 1})-[:R]->(b) RETURN count(*) AS c", int64(0), false},
		{"optional edge properties", []string{"CREATE (:P {name: 'a'})-[:KNOWS {since: 2019}]->(:P {name: 'b'})"}, "MATCH (a:P {name: 'a'}) OPTIONAL MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN b.name AS b", nil, false},
		{"optional source labels", []string{"CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})"}, "MATCH (a:P {name: 'a'}) OPTIONAL MATCH (a:Missing)-[r]->(b) RETURN b.name AS b", nil, false},
		{"zero length", []string{"CREATE (:N)-[:R]->(:N)"}, "MATCH (a)-[*0..1]->(a) RETURN count(*) AS c", int64(2), false},
		{"comma uniqueness", []string{"CREATE (:N)-[:R]->(:N)"}, "MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN count(*) AS c", int64(0), false},
		{"anonymous edge uniqueness", []string{"CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'}), (:P {name: 'c'})-[:KNOWS]->(:P {name: 'd'})"}, "MATCH ()-[:KNOWS]->(), ()-[:KNOWS]->() RETURN count(*) AS c", int64(2), false},
		{"anonymous multiplicity", []string{"CREATE (:A {x: 1}), (:A {x: 2}), (:B), (:B), (:B)"}, "MATCH (a:A), (:B) RETURN count(*) AS c", int64(6), false},
		{"numeric join", []string{"CREATE (:A {v: 1}), (:B {v: 1.0})"}, "MATCH (a:A), (b:B) WHERE a.v = b.v RETURN count(*) AS c", int64(1), false},
		{"create invalid suffix", nil, "CREATE (:P {x: 1}) -- note", nil, true},
		{"optional empty count", nil, "UNWIND [] AS x OPTIONAL MATCH (m:Missing {k: x}) MERGE (n:P {k: x}) RETURN count(m) AS c", int64(0), false},
		{"optional populated count", []string{"CREATE (:Found {k: 1})"}, "UNWIND [1, 2] AS x OPTIONAL MATCH (m:Found {k: x}) MERGE (n:P {k: x}) RETURN count(m) AS c", int64(1), false},
		{"merge match set order", []string{"CREATE (:P {k: 1, v: 0})", "UNWIND [1] AS x MERGE (n:P {k: x}) ON MATCH SET n.v = 1 SET n.v = 2 RETURN count(*) AS c"}, "MATCH (n:P) RETURN n.v", int64(2), false},
		{"seed not null properties", []string{"CREATE (:A {k: 1, x: 5})-[:R]->(:B {n: 'ok'}), (:A {k: 2, x: 5})-[:R]->(:B {n: 'bad'})"}, "MATCH (a:A {k: 1})-[:R]->(b) WHERE a.x IS NOT NULL RETURN b.n AS n", "ok", false},
		{"separate match reuse", []string{"CREATE (:N)-[:R]->(:N)"}, "MATCH (a)-[r1]->(b) MATCH (c)-[r2]->(d) RETURN count(*) AS c", int64(1), false},
		{"comma variable length uniqueness", []string{"CREATE (:N)-[:R]->(:N)"}, "MATCH (a)-[r1*1..1]->(b), (c)-[r2]->(d) RETURN count(*) AS c", int64(0), false},
		{"comma named path uniqueness", []string{"CREATE (:N)-[:R]->(:N)"}, "MATCH p = (a)-[r1]->(b), q = (c)-[r2]->(d) RETURN count(*) AS c", int64(0), false},
		{"count null predicate", []string{"CREATE (:L {a: 1})"}, "MATCH (n:L) WHERE n.a = n.missing RETURN count(*) AS c", int64(0), false},
		{"join all labels and properties", []string{"CREATE (:A:Extra {v: 1, k: 7}), (:A {v: 1, k: 7}), (:A:Extra {v: 1, k: 8}), (:B {v: 1.0})"}, "MATCH (a:A:Extra {k: 7}), (b:B) WHERE a.v = b.v RETURN count(*) AS c", int64(1), false},
		{"join nulls", []string{"CREATE (:A), (:B)"}, "MATCH (a:A), (b:B) WHERE a.v = b.v RETURN count(*) AS c", int64(0), false},
		{"join residual", []string{"CREATE (:A {v: 1, k: 7}), (:B {v: 1.0})"}, "MATCH (a:A), (b:B) WHERE a.v = b.v AND a.k = 8 RETURN count(*) AS c", int64(0), false},
		{"join offset", []string{"CREATE (:A {v: 2}), (:B {v: 1})"}, "MATCH (a:A), (b:B) WHERE a.v = b.v + 1 RETURN count(*) AS c", int64(1), false},
		{"join or", []string{"CREATE (:A {v: 1}), (:B {v: 1}), (:B {v: 2})"}, "MATCH (a:A), (b:B) WHERE a.v = b.v OR b.v = 2 RETURN count(*) AS c", int64(2), false},
		{"join repeated variable", []string{"CREATE (:A:B {v: 1})"}, "MATCH (a:A), (a:B) WHERE a.v = a.v RETURN count(*) AS c", int64(1), false},
		{"join third part", []string{"CREATE (:A {v: 1}), (:B {v: 1.0}), (:C {v: 1})"}, "MATCH (a:A), (b:B), (c:C) WHERE a.v = b.v AND b.v = c.v RETURN count(*) AS c", int64(1), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			for _, setup := range test.setup {
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, test.query, nil)
			if test.wantError {
				require.Error(t, err)
				if test.name == "merge null" {
					require.Contains(t, statusText(err), "Neo.ClientError.Statement.SemanticError")
				} else {
					require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
				}
				nodes, readErr := exec.storage.GetNodesByLabel("P")
				require.NoError(t, readErr)
				require.Empty(t, nodes)
				return
			}
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, test.want, result.Rows[0][0])
		})
	}
}

func TestIssue908ProductBindings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A), (:B), (:N)-[:R]->(:N)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "WITH 7 AS __nornic_match_product_0 MATCH (a:A), (:B) RETURN *", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "__nornic_match_product_0"}, result.Columns)
	require.Len(t, result.Rows, 1)
	result, err = exec.Execute(ctx, "MATCH p = ()-[:R]->(), (:B) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"p"}, result.Columns)
	require.Len(t, result.Rows, 1)
	rows, handled, err := exec.pipelineApplyMatchProduct(ctx, []pipelineRow{{"__nornic_match_product_0": int64(7)}}, []string{"(:A)", "(:B)"}, "true")
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []pipelineRow{{"__nornic_match_product_0": int64(7)}}, rows)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = exec.pipelineApplyMatchProduct(cancelled, []pipelineRow{{}}, []string{"(a:A)", "(b:B)"}, "")
	require.Error(t, err)
}

func TestIssue908ProductPathValues(t *testing.T) {
	edge := &storage.Edge{ID: "test:r"}
	path := PathResult{Relationships: []*storage.Edge{edge}}
	for _, row := range []pipelineRow{
		{"p": nil},
		{"p": map[string]interface{}{}},
		{"p": map[string]interface{}{"_pathResult": PathResult{Relationships: []*storage.Edge{nil}}}},
		{"p": map[string]interface{}{"_pathResult": path}, "q": map[string]interface{}{"_pathResult": &path}},
	} {
		variables := []string{"p"}
		if _, exists := row["q"]; exists {
			variables = append(variables, "q")
		}
		require.False(t, pipelineProductPathsUnique(row, variables))
	}
	require.True(t, pipelineProductPathsUnique(pipelineRow{"p": map[string]interface{}{"_pathResult": &path}}, []string{"p"}))
}

func TestIssue908NodeJoinBindings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {v: 1}), (:B {v: 1.0})", nil)
	require.NoError(t, err)
	nodes, err := exec.storage.GetNodesByLabel("A")
	require.NoError(t, err)
	parts := []string{"(a:A)", "(b:B)"}
	rows, handled, err := exec.pipelineApplyNodeJoinProduct(ctx, []pipelineRow{{"a": nodes[0], "marker": int64(7)}}, parts, "a.v = b.v")
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, rows, 1)
	require.Equal(t, int64(7), rows[0]["marker"])
	for _, value := range []interface{}{nil, int64(1), &storage.Node{Labels: []string{"Other"}, Properties: map[string]interface{}{"v": int64(1)}}} {
		rows, handled, err = exec.pipelineApplyNodeJoinProduct(ctx, []pipelineRow{{"a": value}}, parts, "a.v = b.v")
		require.NoError(t, err)
		require.True(t, handled)
		require.Empty(t, rows)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, handled, err = exec.pipelineApplyNodeJoinProduct(cancelled, []pipelineRow{{}}, parts, "a.v = b.v")
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	failing := withExpressionFailureSlot(ctx)
	failure := newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidType", "Invalid join predicate")
	recordExpressionFailure(failing, failure)
	_, handled, err = exec.pipelineApplyNodeJoinProduct(failing, []pipelineRow{{}}, parts, "a.v = b.v")
	require.True(t, handled)
	require.ErrorIs(t, err, failure)
}
