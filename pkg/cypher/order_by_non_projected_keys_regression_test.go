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
			name:  "relationship pattern, order term matches a RETURN item's expression text (not its alias), plus a bare alias key",
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

// The following edge-case tests (section 4.4 of the design) cover shapes the
// FunctionGraph/DeploysFrom cases above do not: alias shadowing, SKIP/LIMIT
// interaction with hidden columns, null ordering through an OPTIONAL MATCH,
// stable-sort ties, spacing-insensitive expression matching, a WHERE clause
// between a single (non-chained) OPTIONAL MATCH and RETURN, DISTINCT
// interaction, and a hidden key that is null for some rows. Every expected
// value below is either the output of neo4j:2026-community on the same seed
// (noted per case) or, for the stable-sort case, an invariant of this
// engine's own row order that does not depend on Neo4j's unspecified tie
// behavior.

// orderByEdgeCasesSeed is orderByNonProjectedSeed scoped to a single File so
// queries can anchor on {relative_path:'a.go'} without a Repository hop.
const orderByEdgeCasesSeed = orderByNonProjectedSeed

func TestOrderByNonProjectedKeys_EdgeCases(t *testing.T) {
	exec, ctx := newOrderByNonProjectedExecutor(t, orderByEdgeCasesSeed)

	t.Run("alias shadowing: RETURN e.name AS e ORDER BY e sorts on the alias", func(t *testing.T) {
		// neo4j:2026-community: MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function)
		// RETURN e.name AS e ORDER BY e
		want := []string{"Apply", "Divide", "Error", "Error", "Map", "Min", "Pop", "Worker", "calculate_sum", "get_first", "show", "start"}
		res, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN e.name AS e ORDER BY e`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"e"}, res.Columns)
		got := make([]string, 0, len(res.Rows))
		for _, row := range res.Rows {
			require.Len(t, row, 1)
			s, _ := row[0].(string)
			got = append(got, s)
		}
		require.Equal(t, want, got)
	})

	t.Run("DESC + SKIP + LIMIT: hidden columns absent, Columns length unchanged", func(t *testing.T) {
		// neo4j:2026-community: MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function)
		// RETURN e.id AS id ORDER BY e.name DESC, e.id SKIP 2 LIMIT 3
		want := []string{"fn-10", "fn-05", "fn-09"}
		res, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN e.id AS id ORDER BY e.name DESC, e.id SKIP 2 LIMIT 3`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"id"}, res.Columns, "hidden ORDER BY columns must not leak into Columns")
		require.Equal(t, want, firstColumnStrings(t, res.Rows, 0))
		for _, row := range res.Rows {
			require.Len(t, row, 1, "hidden ORDER BY columns must not leak into Rows")
		}
	})

	t.Run("spacing variant: coalesce(e.x,0) vs the RETURN item's coalesce(e.x, 0)", func(t *testing.T) {
		// neo4j:2026-community: MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function)
		// RETURN e.id AS id, coalesce(e.cyclomatic_complexity, 0) AS c
		// ORDER BY coalesce(e.cyclomatic_complexity,0) DESC, e.id
		want := []string{"fn-02", "fn-01", "fn-03", "fn-06", "fn-09", "fn-04", "fn-07", "fn-11", "fn-12", "fn-05", "fn-10", "fn-08"}
		query := `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN e.id AS id, coalesce(e.cyclomatic_complexity, 0) AS c ORDER BY coalesce(e.cyclomatic_complexity,0) DESC, e.id`
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"id", "c"}, res.Columns)
		require.Equal(t, want, firstColumnStrings(t, res.Rows, 0))
	})

	t.Run("single OPTIONAL MATCH with WHERE before RETURN, non-projected keys", func(t *testing.T) {
		// neo4j:2026-community: MATCH (e:Function)
		// OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(repo:Repository)
		// WHERE coalesce(e.cyclomatic_complexity, 0) > 3
		// RETURN e.id AS id ORDER BY e.name, e.id
		//
		// A WHERE clause immediately after OPTIONAL MATCH constrains only the
		// optional pattern's candidate matches; a predicate over a variable
		// bound before the OPTIONAL MATCH (here e) never drops the row, so
		// the result equals the unfiltered orderByNameThenID list (confirmed
		// against neo4j:2026-community directly, not inferred).
		query := `MATCH (e:Function) OPTIONAL MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(repo:Repository) WHERE coalesce(e.cyclomatic_complexity, 0) > 3 RETURN e.id AS id ORDER BY e.name, e.id`
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, orderByNameThenID, firstColumnStrings(t, res.Rows, 0))
	})

	t.Run("aggregate RETURN with ORDER BY on the aggregate alias is unchanged", func(t *testing.T) {
		// neo4j:2026-community: MATCH (e:Function) RETURN e.name AS name, count(*) AS c
		// ORDER BY c DESC, name LIMIT 3
		type row struct {
			name string
			c    int64
		}
		want := []row{{"Error", 2}, {"Apply", 1}, {"Divide", 1}}
		res, err := exec.Execute(ctx, `MATCH (e:Function) RETURN e.name AS name, count(*) AS c ORDER BY c DESC, name LIMIT 3`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name", "c"}, res.Columns)
		got := make([]row, 0, len(res.Rows))
		for _, r := range res.Rows {
			require.Len(t, r, 2)
			name, _ := r[0].(string)
			got = append(got, row{name, toInt64(r[1])})
		}
		require.Equal(t, want, got)
	})

	t.Run("stable sort: an all-tied ORDER BY key keeps join order", func(t *testing.T) {
		// Implementation invariant, not a Neo4j comparison: Neo4j's tie order
		// for equal keys is unspecified, so this asserts only that this
		// engine's sort.SliceStable preserves the pre-sort row order when
		// every ORDER BY key ties (a constant expression), by comparing
		// against the same query with no ORDER BY at all.
		baseline, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN e.id AS id`, nil)
		require.NoError(t, err)
		tied, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN e.id AS id ORDER BY 1`, nil)
		require.NoError(t, err)
		require.Equal(t, firstColumnStrings(t, baseline.Rows, 0), firstColumnStrings(t, tied.Rows, 0))
	})
}

func TestOrderByNonProjectedKeys_NullHiddenKey(t *testing.T) {
	// Owner is bound only through an OPTIONAL MATCH that misses for some
	// functions, so o.name is null on those rows -- exercising nulls-last
	// ASC / nulls-first DESC on a hidden (non-projected) ORDER BY key.
	const seed = `
CREATE (f:File {relative_path:'b.go'}),
       (f)-[:CONTAINS]->(e1:Function {id:'fn-a', name:'Alpha'}),
       (f)-[:CONTAINS]->(e2:Function {id:'fn-b', name:'Bravo'}),
       (f)-[:CONTAINS]->(e3:Function {id:'fn-c', name:'Charlie'}),
       (f)-[:CONTAINS]->(e4:Function {id:'fn-d', name:'Delta'}),
       (o1:Owner {name:'zed'}),
       (o2:Owner {name:'amy'}),
       (e1)-[:OWNED_BY]->(o1),
       (e3)-[:OWNED_BY]->(o2)
`
	exec, ctx := newOrderByNonProjectedExecutor(t, seed)

	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			// neo4j:2026-community: MATCH (e:Function) OPTIONAL MATCH (e)-[:OWNED_BY]->(o:Owner)
			// RETURN e.id AS id ORDER BY o.name ASC
			name:  "nulls last ASC",
			query: `MATCH (e:Function) OPTIONAL MATCH (e)-[:OWNED_BY]->(o:Owner) RETURN e.id AS id ORDER BY o.name ASC`,
			want:  []string{"fn-c", "fn-a", "fn-b", "fn-d"},
		},
		{
			// neo4j:2026-community: MATCH (e:Function) OPTIONAL MATCH (e)-[:OWNED_BY]->(o:Owner)
			// RETURN e.id AS id ORDER BY o.name DESC
			name:  "nulls first DESC",
			query: `MATCH (e:Function) OPTIONAL MATCH (e)-[:OWNED_BY]->(o:Owner) RETURN e.id AS id ORDER BY o.name DESC`,
			want:  []string{"fn-b", "fn-d", "fn-a", "fn-c"},
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

func TestOrderByNonProjectedKeys_CoalesceNullHiddenKey(t *testing.T) {
	// Extends orderByNonProjectedDeploysSeed with one target that has
	// neither id nor uid, so coalesce(t.id, t.uid) (the ORDER BY key) is
	// null on that row even though the returned target_id column
	// (coalesce(t.id, t.uid, t.name)) is not.
	const seed = orderByNonProjectedDeploysSeed + `,
       (a)-[:DEPLOYS_FROM]->(:Target {name:'unnamed-target'})
`
	exec, ctx := newOrderByNonProjectedExecutor(t, seed)

	// neo4j:2026-community: MATCH (s:Repository)-[r:DEPLOYS_FROM]->(t)
	// RETURN coalesce(s.id, s.uid, s.name) AS source_id, coalesce(t.id, t.uid, t.name) AS target_id
	// ORDER BY s.id, coalesce(t.id, t.uid)
	want := [][2]string{
		{"repo-a", "tgt-3"}, {"repo-a", "tgt-5"}, {"repo-a", "tgt-6"}, {"repo-a", "tgt-9"}, {"repo-a", "unnamed-target"},
		{"repo-b", "tgt-0b"}, {"repo-b", "tgt-1"}, {"repo-b", "tgt-4"}, {"repo-b", "tgt-7"},
		{"repo-c", "tgt-0"}, {"repo-c", "tgt-1b"}, {"repo-c", "tgt-2"}, {"repo-c", "tgt-8"},
	}
	res, err := exec.Execute(ctx, orderByNonProjectedDeploys+`ORDER BY s.id, coalesce(t.id, t.uid)`, nil)
	require.NoError(t, err)
	got := make([][2]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 2)
		s, _ := row[0].(string)
		tg, _ := row[1].(string)
		got = append(got, [2]string{s, tg})
	}
	require.Equal(t, want, got)
}

func TestOrderByNonProjectedKeys_Distinct(t *testing.T) {
	exec, ctx := newOrderByNonProjectedExecutor(t, orderByEdgeCasesSeed)

	t.Run("DISTINCT with an alias key sorts unchanged", func(t *testing.T) {
		// neo4j:2026-community: MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function)
		// RETURN DISTINCT e.name AS name ORDER BY name LIMIT 3
		want := []string{"Apply", "Divide", "Error"}
		res, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN DISTINCT e.name AS name ORDER BY name LIMIT 3`, nil)
		require.NoError(t, err)
		got := make([]string, 0, len(res.Rows))
		for _, row := range res.Rows {
			s, _ := row[0].(string)
			got = append(got, s)
		}
		require.Equal(t, want, got)
	})

	t.Run("DISTINCT with a non-projected key does not add hidden columns or crash", func(t *testing.T) {
		// neo4j:2026-community rejects this query outright: "In a WITH/RETURN
		// with DISTINCT or an aggregation, it is not possible to access
		// variables declared before the WITH/RETURN: e". This engine does not
		// validate Cypher scoping the way Neo4j does, so per the design
		// (section 4.1: "DISTINCT / aggregation: do not add hidden items"),
		// this keeps today's behaviour unchanged -- no hidden column is
		// added, the query does not error, and the row/column shape stays
		// exactly what DISTINCT on e.name alone would produce (11 distinct
		// names out of 12 functions; "Error" repeats).
		res, err := exec.Execute(ctx, `MATCH (f:File {relative_path:'a.go'})-[:CONTAINS]->(e:Function) RETURN DISTINCT e.name AS name ORDER BY e.id`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name"}, res.Columns)
		require.Len(t, res.Rows, 11)
		for _, row := range res.Rows {
			require.Len(t, row, 1)
		}
	})
}

func TestSplitOrderByDirection(t *testing.T) {
	cases := []struct {
		in       string
		wantExpr string
		wantDesc bool
	}{
		{"n.name", "n.name", false},
		{"n.name DESC", "n.name", true},
		{"n.name descending", "n.name", true},
		{"n.name ASCENDING", "n.name", false},
		{"  coalesce(e.x, 0)   DESC  ", "coalesce(e.x, 0)", true},
		{"coalesce(n.x, ')') DESC", "coalesce(n.x, ')')", true},
		{`coalesce(n.x, "(") ASC`, `coalesce(n.x, "(")`, false},
		{"n.`weird name` DESC", "n.`weird name`", true},
		{"n.`a) b` DESC", "n.`a) b`", true},
		{"'a DESC'", "'a DESC'", false},
		{`coalesce(n.x, 'it\'s ) DESC') DESC`, `coalesce(n.x, 'it\'s ) DESC')`, true},
		{"n.desc", "n.desc", false},
	}
	for _, tc := range cases {
		expr, desc := splitOrderByDirection(tc.in)
		require.Equal(t, tc.wantExpr, expr, "input %q", tc.in)
		require.Equal(t, tc.wantDesc, desc, "input %q", tc.in)
	}
}
