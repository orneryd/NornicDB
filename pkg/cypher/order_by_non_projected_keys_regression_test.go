package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regression tests for https://github.com/orneryd/NornicDB/issues/500.
//
// ORDER BY terms may reference any variable bound before a non-aggregating,
// non-DISTINCT RETURN, whether or not the term's expression is projected.
// Every expected row list below is the output of neo4j:2026-community on the
// same seed.

const orderByNonProjectedSeed = `
CREATE (r:Repository {id:'repo-x', name:'x'})-[:REPO_CONTAINS]->(f:File {relative_path:'a.go'}),
       (f)-[:CONTAINS]->(:Function {id:'fn-09', name:'Worker',        cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-03', name:'Apply',         cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-11', name:'start',         cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-01', name:'Divide',        cyclomatic_complexity:7}),
       (f)-[:CONTAINS]->(:Function {id:'fn-07', name:'Error',         cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-12', name:'Map',           cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-05', name:'calculate_sum', cyclomatic_complexity:2}),
       (f)-[:CONTAINS]->(:Function {id:'fn-02', name:'Min',           cyclomatic_complexity:9}),
       (f)-[:CONTAINS]->(:Function {id:'fn-10', name:'get_first',     cyclomatic_complexity:2}),
       (f)-[:CONTAINS]->(:Function {id:'fn-04', name:'Pop',           cyclomatic_complexity:4}),
       (f)-[:CONTAINS]->(:Function {id:'fn-08', name:'show',          cyclomatic_complexity:1}),
       (f)-[:CONTAINS]->(:Function {id:'fn-06', name:'Error',         cyclomatic_complexity:7})
`

const orderByNonProjectedDeploysSeed = `
CREATE (c:Repository {id:'repo-c', name:'repo-c'}),
       (b:Repository {id:'repo-b', name:'repo-b'}),
       (a:Repository {id:'repo-a', name:'repo-a'}),
       (c)-[:DEPLOYS_FROM]->(:Target {id:'tgt-2'}),
       (a)-[:DEPLOYS_FROM]->(:Target {id:'tgt-9'}),
       (b)-[:DEPLOYS_FROM]->(:Target {id:'tgt-1'}),
       (a)-[:DEPLOYS_FROM]->(:Target {id:'tgt-3'}),
       (c)-[:DEPLOYS_FROM]->(:Target {uid:'tgt-0'}),
       (b)-[:DEPLOYS_FROM]->(:Target {id:'tgt-7'}),
       (a)-[:DEPLOYS_FROM]->(:Target {uid:'tgt-5'}),
       (c)-[:DEPLOYS_FROM]->(:Target {id:'tgt-8'}),
       (b)-[:DEPLOYS_FROM]->(:Target {uid:'tgt-4'}),
       (a)-[:DEPLOYS_FROM]->(:Target {id:'tgt-6'}),
       (c)-[:DEPLOYS_FROM]->(:Target {id:'tgt-1b'}),
       (b)-[:DEPLOYS_FROM]->(:Target {id:'tgt-0b'})
`

const orderByNonProjectedOptional = `MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(repo:Repository)
WHERE coalesce(e.cyclomatic_complexity, 0) > 0
RETURN e.id AS id, e.name AS name, coalesce(e.cyclomatic_complexity, 0) AS complexity `

const orderByNonProjectedDeploys = `MATCH (s:Repository)-[r:DEPLOYS_FROM]->(t)
RETURN coalesce(s.id, s.uid, s.name) AS source_id, coalesce(t.id, t.uid, t.name) AS target_id `

var orderByNameThenID = []string{"fn-03", "fn-01", "fn-06", "fn-07", "fn-12", "fn-02", "fn-04", "fn-09", "fn-05", "fn-10", "fn-08", "fn-11"}

var orderByComplexityTop5 = []string{"fn-02", "fn-03", "fn-01", "fn-06", "fn-09"}

func newOrderByNonProjectedExecutor(t *testing.T, seed string) (*StorageExecutor, context.Context) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, seed, nil)
	require.NoError(t, err)
	return exec, ctx
}

// firstColumnStrings returns column col of every row as a string, reading
// the "id" property when the cell holds a node.
func firstColumnStrings(t *testing.T, rows [][]interface{}, col int) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		require.Greater(t, len(row), col)
		switch v := row[col].(type) {
		case string:
			out = append(out, v)
		case *storage.Node:
			id, _ := v.Properties["id"].(string)
			out = append(out, id)
		case map[string]interface{}:
			if props, ok := v["properties"].(map[string]interface{}); ok {
				id, _ := props["id"].(string)
				out = append(out, id)
				continue
			}
			id, _ := v["id"].(string)
			out = append(out, id)
		default:
			t.Fatalf("unexpected cell type %T in column %d", v, col)
		}
	}
	return out
}

func TestOrderByNonProjectedKeys_FunctionGraph(t *testing.T) {
	exec, ctx := newOrderByNonProjectedExecutor(t, orderByNonProjectedSeed)

	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "relationship pattern, first key not returned",
			query: `MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e.id AS id ORDER BY e.name, e.id`,
			want:  orderByNameThenID,
		},
		{
			name:  "relationship pattern, non-returned key then alias",
			query: `MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e.id AS id, e.name AS nm ORDER BY e.name, id`,
			want:  orderByNameThenID,
		},
		{
			name:  "relationship pattern, both keys returned (control)",
			query: `MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e.id AS id, e.name AS name ORDER BY e.name, e.id`,
			want:  orderByNameThenID,
		},
		{
			name:  "bare MATCH, property keys (control)",
			query: `MATCH (e:Function) RETURN e.id AS id ORDER BY e.name, e.id`,
			want:  orderByNameThenID,
		},
		{
			name:  "OPTIONAL MATCH, keys not returned",
			query: `MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File) RETURN e.id AS id ORDER BY e.name, e.id`,
			want:  orderByNameThenID,
		},
		{
			name:  "OPTIONAL MATCH, returned properties sorted by expression",
			query: orderByNonProjectedOptional + `ORDER BY complexity DESC, e.name, e.id LIMIT 5`,
			want:  orderByComplexityTop5,
		},
		{
			name:  "OPTIONAL MATCH, alias keys (control)",
			query: orderByNonProjectedOptional + `ORDER BY complexity DESC, name, id LIMIT 5`,
			want:  orderByComplexityTop5,
		},
		{
			name:  "OPTIONAL MATCH, function key written out",
			query: orderByNonProjectedOptional + `ORDER BY coalesce(e.cyclomatic_complexity, 0) DESC, e.name, e.id LIMIT 5`,
			want:  orderByComplexityTop5,
		},
		{
			name:  "bare MATCH, function key",
			query: `MATCH (e:Function) RETURN e.id AS id ORDER BY coalesce(e.cyclomatic_complexity, 0) DESC, e.id`,
			want:  []string{"fn-02", "fn-01", "fn-03", "fn-06", "fn-09", "fn-04", "fn-07", "fn-11", "fn-12", "fn-05", "fn-10", "fn-08"},
		},
		{
			name:  "relationship pattern, returned node, .id means the id property",
			query: `MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e ORDER BY e.id`,
			want:  []string{"fn-01", "fn-02", "fn-03", "fn-04", "fn-05", "fn-06", "fn-07", "fn-08", "fn-09", "fn-10", "fn-11", "fn-12"},
		},
		{
			name:  "relationship pattern, returned node, name then id",
			query: `MATCH (f:File)-[:CONTAINS]->(e:Function) RETURN e ORDER BY e.name, e.id`,
			want:  orderByNameThenID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, firstColumnStrings(t, res.Rows, 0))
		})
	}
}

func TestOrderByNonProjectedKeys_DeploysFrom(t *testing.T) {
	exec, ctx := newOrderByNonProjectedExecutor(t, orderByNonProjectedDeploysSeed)

	full := [][2]string{
		{"repo-a", "tgt-3"}, {"repo-a", "tgt-5"}, {"repo-a", "tgt-6"}, {"repo-a", "tgt-9"},
		{"repo-b", "tgt-0b"}, {"repo-b", "tgt-1"}, {"repo-b", "tgt-4"}, {"repo-b", "tgt-7"},
		{"repo-c", "tgt-0"}, {"repo-c", "tgt-1b"}, {"repo-c", "tgt-2"}, {"repo-c", "tgt-8"},
	}
	cases := []struct {
		name  string
		query string
		want  [][2]string
	}{
		{"non-returned and function keys", orderByNonProjectedDeploys + `ORDER BY s.id, coalesce(t.id, t.uid)`, full},
		{"non-returned and function keys, LIMIT", orderByNonProjectedDeploys + `ORDER BY s.id, coalesce(t.id, t.uid) LIMIT 6`, full[:6]},
		{"alias keys (control)", orderByNonProjectedDeploys + `ORDER BY source_id, target_id LIMIT 6`, full[:6]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			got := make([][2]string, 0, len(res.Rows))
			for _, row := range res.Rows {
				require.Len(t, row, 2)
				s, _ := row[0].(string)
				tg, _ := row[1].(string)
				got = append(got, [2]string{s, tg})
			}
			require.Equal(t, tc.want, got)
		})
	}
}
