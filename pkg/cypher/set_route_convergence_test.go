package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SET behaves the same after MATCH, CREATE and MERGE (#470, #480, #543, #544,
// #545, #514, #568). Expected values are Neo4j 5's. Each case runs on a fresh
// database, on the in-memory engine and the server stack, in auto-commit and
// inside an explicit transaction.
type setRouteCase struct {
	name   string
	setup  []string
	stmt   string
	params map[string]interface{}
	// want is the single returned row; wantErr expects the statement to fail.
	want    []interface{}
	wantErr bool
	// check reads the stored graph afterwards (optional).
	check     string
	wantCheck [][]interface{}
	// labels compares the first returned list as a set.
	labels bool
}

func setRouteCases() []setRouteCase {
	t1 := []string{"CREATE (:T {id: 1, a: 5})"}
	tOnly := []string{"CREATE (:T {id: 1})"}
	spr := []string{"CREATE (:S {p: 1, q: 2})-[:R {w: 3}]->(:T {z: 9})"}
	rs := []string{
		"CREATE (:R {id: 'r1'}), (:R {id: 'r2'}), (:R {id: 'r3'}), (:R {id: 'r4'})",
		"MATCH (a:R {id: 'r3'}), (b:R {id: 'r4'}) CREATE (a)-[:DEP {t: 1}]->(b)",
	}
	merge568 := "UNWIND $rows AS row MATCH (a:R {id: row.a}) MATCH (b:R {id: row.b}) MERGE (a)-[rel:DEP]->(b) SET rel.t = row.t"
	one := func(v interface{}) []interface{} { return []interface{}{v} }
	strs := func(s ...string) []interface{} {
		out := make([]interface{}, len(s))
		for i, v := range s {
			out[i] = v
		}
		return out
	}
	countNodes := "MATCH (n) RETURN count(n) AS c"
	return []setRouteCase{
		// #470: SET to null removes the key on every route
		{name: "470 match += null", setup: t1, stmt: "MATCH (n:T {id: 1}) SET n += {a: null} RETURN keys(n) AS k", want: one(strs("id"))},
		{name: "470 create += null", stmt: "CREATE (n:U {id: 1, a: 5}) SET n += {a: null} RETURN keys(n) AS k", want: one(strs("id"))},
		{name: "470 create = map with null", stmt: "CREATE (n:U {id: 1, a: 5}) SET n = {id: 1, a: null} RETURN keys(n) AS k", want: one(strs("id"))},
		{name: "470 create rel += null", stmt: "CREATE (:U)-[r:R {x: 1}]->(:U) SET r += {x: null} RETURN keys(r) AS k", want: one(strs())},
		{name: "470 create n.a = null", stmt: "CREATE (n:U {id: 1, a: 5}) SET n.a = null RETURN keys(n) AS k", want: one(strs("id"))},
		{name: "470 merge += null", stmt: "MERGE (n:U {id: 1}) SET n += {a: 5} SET n += {a: null} RETURN keys(n) AS k", want: one(strs("id"))},
		// #480: label chains and quoted labels
		{name: "480 match n:A:B", setup: tOnly, stmt: "MATCH (n:T {id: 1}) SET n:A:B RETURN labels(n) AS l", want: one(strs("T", "A", "B")), labels: true},
		{name: "480 create n:A:B", stmt: "CREATE (n:U {id: 1}) SET n:A:B RETURN labels(n) AS l", want: one(strs("U", "A", "B")), labels: true},
		{name: "480 merge n:A:B", stmt: "MERGE (n:U {id: 1}) SET n:A:B RETURN labels(n) AS l", want: one(strs("U", "A", "B")), labels: true},
		{name: "480 merge on create n:A:B", stmt: "MERGE (n:U {id: 1}) ON CREATE SET n:A:B RETURN labels(n) AS l", want: one(strs("U", "A", "B")), labels: true},
		{name: "480 merge on match n:A:B", setup: tOnly, stmt: "MERGE (n:T {id: 1}) ON MATCH SET n:A:B RETURN labels(n) AS l", want: one(strs("T", "A", "B")), labels: true},
		{name: "480 create quoted label", stmt: "CREATE (n:U {id: 1}) SET n:`A B` RETURN labels(n) AS l", want: one(strs("U", "A B")), labels: true},
		{name: "480 merge quoted label", stmt: "MERGE (n:U {id: 1}) SET n:`A B` RETURN labels(n) AS l", want: one(strs("U", "A B")), labels: true},
		// #543: malformed SET / empty label are errors and write nothing
		{name: "543 create SET n.x", setup: tOnly, stmt: "CREATE (n:U) SET n.x RETURN n.x AS x", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 match SET n.x", setup: tOnly, stmt: "MATCH (n:T) SET n.x RETURN n.x AS x", wantErr: true},
		{name: "543 create (a:)", setup: tOnly, stmt: "CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 create (a:) SET", setup: tOnly, stmt: "CREATE (a:) SET a.x = 1 RETURN a.x AS x", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		// #544: SET from another node / relationship
		{name: "544 match n += a", setup: spr, stmt: "MATCH (a:S), (n:T) SET n += a RETURN properties(n) AS p", want: one(map[string]interface{}{"p": int64(1), "q": int64(2), "z": int64(9)})},
		{name: "544 match n += r", setup: spr, stmt: "MATCH (a:S)-[r:R]->(n:T) SET n += r RETURN properties(n) AS p", want: one(map[string]interface{}{"w": int64(3), "z": int64(9)})},
		{name: "544 match n = a", setup: spr, stmt: "MATCH (a:S), (n:T) SET n = a RETURN properties(n) AS p", want: one(map[string]interface{}{"p": int64(1), "q": int64(2)})},
		{name: "544 create n += a", stmt: "CREATE (a:X {p: 1}), (n:Y) SET n += a RETURN n.p AS p", want: one(int64(1))},
		{name: "544 create n = a", stmt: "CREATE (a:X {p: 1}), (n:Y) SET n = a RETURN n.p AS p", want: one(int64(1))},
		{name: "544 merge n += a", setup: []string{"CREATE (:X {p: 1})"}, stmt: "MATCH (a:X) MERGE (n:Y {k: 1}) SET n += a RETURN n.p AS p", want: one(int64(1))},
		// #545: named path with SET
		{name: "545 create p = ... SET", stmt: "CREATE p = (a:T {id: 1})-[:R]->(b:T {id: 2}) SET a.x = 1 RETURN length(p) AS l", want: one(int64(1)),
			check: "MATCH (a:T {id: 1})-[:R]->(:T {id: 2}) RETURN a.x AS x", wantCheck: [][]interface{}{{int64(1)}}},
		// #514: property map referencing another created node
		{name: "514 create ref", stmt: "CREATE (a:T {name: 'x'}), (b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		{name: "514 create ref + SET", stmt: "CREATE (a:T {name: 'x'}), (b:U {name: a.name}) SET a.y = 1 RETURN b.name AS n", want: one("x")},
		// #543 / #514 on every CREATE route: after MATCH, after UNWIND, after WITH,
		// and in a trailing CREATE.
		{name: "543 match create (a:)", setup: tOnly, stmt: "MATCH (t:T) CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 unwind create (a:)", setup: tOnly, stmt: "UNWIND [1] AS i CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 with create (a:)", setup: tOnly, stmt: "WITH 1 AS i CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 trailing create (a:)", setup: tOnly, stmt: "CREATE (x:U) SET x.a = 1 CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "543 merge then create (a:)", setup: tOnly, stmt: "MERGE (x:T {id: 1}) CREATE (a:) RETURN a", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "514 match create ref", setup: tOnly, stmt: "MATCH (t:T) CREATE (a:V {name: 'x'}), (b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		{name: "514 match create ref to matched", setup: []string{"CREATE (:T {id: 1, name: 'm'})"}, stmt: "MATCH (t:T) CREATE (b:U {name: t.name}) RETURN b.name AS n", want: one("m")},
		{name: "514 unwind create ref", stmt: "UNWIND ['x'] AS v CREATE (a:V {name: v}), (b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		{name: "514 with create ref", stmt: "WITH 'x' AS v CREATE (a:V {name: v}), (b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		{name: "514 trailing create ref", stmt: "CREATE (a:V {name: 'x'}) SET a.y = 1 CREATE (b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		{name: "514 match create rel ref", setup: tOnly, stmt: "MATCH (t:T) CREATE (a:V {name: 'x'})-[:R]->(b:U {name: a.name}) RETURN b.name AS n", want: one("x")},
		// The same on the UNWIND $rows bulk fast paths.
		{name: "543 unwind rows create (a:)", setup: tOnly, stmt: "UNWIND $rows AS row CREATE (a:) RETURN a",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": int64(1)}}}, wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "514 unwind rows create ref", stmt: "UNWIND $rows AS row CREATE (a:V {name: row.n}), (b:U {name: a.name}) RETURN b.name AS n",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"n": "x"}}}, want: one("x")},
		{name: "470 create null prop", stmt: "CREATE (n:U {a: null, b: 1}) RETURN keys(n) AS k", want: one(strs("b"))},
		{name: "470 unwind rows create null prop", stmt: "UNWIND $rows AS row CREATE (n:U {id: row.id, t: row.t})",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": int64(1), "t": nil}}},
			check:  "MATCH (n:U) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "470 unwind rows match create rel null prop", setup: rs, stmt: "UNWIND $rows AS row MATCH (a:R {id: row.a}) MATCH (b:R {id: row.b}) CREATE (a)-[:L {x: row.x}]->(b)",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "r1", "b": "r2", "x": nil}}},
			check:  "MATCH ()-[l:L]->() RETURN keys(l) AS k", wantCheck: [][]interface{}{{strs()}}},
		{name: "470 unwind rows merge node null prop", stmt: "UNWIND $rows AS row MERGE (n:M {id: row.id}) ON CREATE SET n.t = row.t",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": "m1", "t": nil}}},
			check:  "MATCH (n:M) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		// Values a property cannot hold, and non-map replacements, fail on every route.
		{name: "invalid match n.x = map", setup: tOnly, stmt: "MATCH (n:T) SET n.x = {a: 1} RETURN n.x AS x", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "invalid create n.x = map", setup: tOnly, stmt: "CREATE (n:U) SET n.x = {a: 1} RETURN n.x AS x", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "invalid merge n.x = map", setup: tOnly, stmt: "MERGE (n:T {id: 1}) SET n.x = {a: 1} RETURN n.x AS x", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "invalid match n = 5", setup: tOnly, stmt: "MATCH (n:T) SET n = 5 RETURN n", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "invalid create n = 5", setup: tOnly, stmt: "CREATE (n:U) SET n = 5 RETURN n", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "invalid merge n = 5", setup: tOnly, stmt: "MERGE (n:T {id: 1}) SET n = 5 RETURN n", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "invalid create unknown var", setup: tOnly, stmt: "CREATE (n:U) SET m.x = 1 RETURN n", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "fn create randomUUID", stmt: "CREATE (n:U) SET n.id = randomUUID() RETURN size(n.id) AS s", want: one(int64(36))},
		{name: "fn match randomUUID", setup: tOnly, stmt: "MATCH (n:T) SET n.u = randomUUID() RETURN size(n.u) AS s", want: one(int64(36))},
		{name: "fn create unknown", setup: tOnly, stmt: "CREATE (n:U) SET n.x = noSuchFn(1) RETURN n.x AS x", wantErr: true, check: countNodes, wantCheck: [][]interface{}{{int64(1)}}},
		{name: "fn match unknown", setup: tOnly, stmt: "MATCH (n:T) SET n.x = noSuchFn(1) RETURN n.x AS x", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "fn merge unknown", setup: tOnly, stmt: "MERGE (n:T {id: 1}) SET n.x = noSuchFn(1) RETURN n.x AS x", wantErr: true,
			check: "MATCH (n:T) RETURN keys(n) AS k", wantCheck: [][]interface{}{{strs("id")}}},
		{name: "470 match create += null", setup: tOnly, stmt: "MATCH (a:T) CREATE (b:U {x: 5}) SET b += {x: null, y: 1} RETURN keys(b) AS k", want: one(strs("y"))},
		{name: "480 match create n:A:B", setup: tOnly, stmt: "MATCH (a:T) CREATE (b:U) SET b:A:B RETURN labels(b) AS l", want: one(strs("U", "A", "B")), labels: true},
		{name: "544 match create n += a", setup: []string{"CREATE (:T {p: 1})"}, stmt: "MATCH (a:T) CREATE (b:U) SET b += a RETURN b.p AS p", want: one(int64(1))},
		{name: "470 unwind merge node null", stmt: "UNWIND $rows AS row MERGE (n:M {id: row.id}) SET n.t = row.t",
			params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": "m1", "t": nil}, map[string]interface{}{"id": "m2"}}},
			check:  "MATCH (n:M) RETURN keys(n) AS k ORDER BY n.id", wantCheck: [][]interface{}{{strs("id")}, {strs("id")}}},
		// #568: UNWIND ... MERGE rel ... SET rel.p = row.p with null / missing
		{name: "568 merge creates, key missing", setup: rs, stmt: merge568, params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "r1", "b": "r2"}}},
			check: "MATCH (:R {id: 'r1'})-[rel:DEP]->(:R {id: 'r2'}) RETURN keys(rel) AS k", wantCheck: [][]interface{}{{strs()}}},
		{name: "568 merge creates, null", setup: rs, stmt: merge568, params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "r2", "b": "r3", "t": nil}}},
			check: "MATCH (:R {id: 'r2'})-[rel:DEP]->(:R {id: 'r3'}) RETURN keys(rel) AS k", wantCheck: [][]interface{}{{strs()}}},
		{name: "568 merge matches, null", setup: rs, stmt: merge568, params: map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "r3", "b": "r4", "t": nil}}},
			check: "MATCH (:R {id: 'r3'})-[rel:DEP]->(:R {id: 'r4'}) RETURN keys(rel) AS k", wantCheck: [][]interface{}{{strs()}}},
	}
}

func TestSetRoutesConverge(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range setRouteCases() {
				t.Run(stack+"/"+mode+"/"+tc.name, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					for _, q := range tc.setup {
						_, err := exec.Execute(ctx, q, nil)
						require.NoError(t, err, q)
					}
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, tc.params)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, cerr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, cerr)
						}
					}
					if tc.wantErr {
						assert.Error(t, err, tc.stmt)
					} else if assert.NoError(t, err, tc.stmt) && tc.want != nil {
						if assert.Len(t, res.Rows, 1, tc.stmt) {
							got := res.Rows[0]
							if tc.labels {
								assert.ElementsMatch(t, tc.want[0], got[0], tc.stmt)
							} else {
								assert.Equal(t, tc.want, normalizeSetRouteRow(got), tc.stmt)
							}
						}
					}
					if tc.check != "" {
						chk, err := exec.Execute(ctx, tc.check, nil)
						require.NoError(t, err, tc.check)
						assert.Equal(t, tc.wantCheck, normalizeSetRouteRows(chk.Rows), "%s after %s", tc.check, tc.stmt)
					}
				})
			}
		}
	}
}

// normalizeSetRouteRow sorts string lists (keys()) so key order doesn't matter.
func normalizeSetRouteRow(row []interface{}) []interface{} {
	out := make([]interface{}, len(row))
	for i, v := range row {
		out[i] = v
		list, ok := v.([]interface{})
		if !ok {
			continue
		}
		strs := make([]string, 0, len(list))
		for _, item := range list {
			s, isString := item.(string)
			if !isString {
				strs = nil
				break
			}
			strs = append(strs, s)
		}
		if strs == nil && len(list) > 0 {
			continue
		}
		sort.Strings(strs)
		sorted := make([]interface{}, len(strs))
		for j, s := range strs {
			sorted[j] = s
		}
		out[i] = sorted
	}
	return out
}

func normalizeSetRouteRows(rows [][]interface{}) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		out[i] = normalizeSetRouteRow(row)
	}
	return out
}

// newSetRouteServerStackExecutor builds the server's storage stack
// (Badger -> WAL -> Async -> Namespaced).
func newSetRouteServerStackExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	dir := t.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	wal, err := storage.NewWAL(dir+"/wal", nil)
	require.NoError(t, err)
	async := storage.NewAsyncEngine(storage.NewWALEngine(badger, wal), nil)
	t.Cleanup(func() {
		_ = async.Close()
		_ = wal.Close()
		_ = badger.Close()
	})
	return NewStorageExecutor(storage.NewNamespacedEngine(async, "test"))
}

// The UNWIND ... MERGE batch fast paths write SET values with the same
// semantics as the shared applicator: null removes the key, maps are not
// property values.
func TestUnwindMergeBatchSetSemantics(t *testing.T) {
	resolve := func(expr string, row map[string]interface{}) interface{} { return row[expr] }

	node := &storage.Node{ID: "n", Properties: map[string]interface{}{"t": int64(1)}}
	changed, err := applyUnwindMergeChainSetAssignment(node, unwindSimpleSetAssignment{prop: "t", expr: "t"}, map[string]interface{}{"t": nil}, resolve)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.NotContains(t, node.Properties, "t")
	changed, err = applyUnwindMergeChainSetAssignment(node, unwindSimpleSetAssignment{prop: "t", expr: "t"}, map[string]interface{}{"t": nil}, resolve)
	require.NoError(t, err)
	assert.False(t, changed, "removing an absent key is not a change")
	_, err = applyUnwindMergeChainSetAssignment(node, unwindSimpleSetAssignment{prop: "t", expr: "t"}, map[string]interface{}{"t": map[string]interface{}{"a": 1}}, resolve)
	assert.Error(t, err)

	edge := &storage.Edge{ID: "e", Properties: map[string]interface{}{"a": int64(1)}}
	changed, err = applyUnwindMergeChainEdgeSetAssignment(edge, unwindSimpleSetAssignment{mergeMap: true, expr: "p"}, map[string]interface{}{"p": map[string]interface{}{"a": nil, "b": int64(2)}}, resolve)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, map[string]interface{}{"b": int64(2)}, edge.Properties)

	props, ok := normalizeRelationshipBatchRowProperties(map[string]interface{}{"a": nil, "b": 2}, nil)
	assert.True(t, ok)
	assert.Equal(t, map[string]interface{}{"b": int64(2)}, props)
	_, ok = normalizeRelationshipBatchRowProperties(map[string]interface{}{"a": map[string]interface{}{"x": 1}}, nil)
	assert.False(t, ok, "a map value hands the row to the row-wise executor, which rejects it")
}

// The UNWIND bulk fast paths apply the same CREATE / SET value rules as the
// row-wise routes. Each case asserts the fast path it exercises.
func TestUnwindFastPathsShareSetCreateSemantics(t *testing.T) {
	setup := []string{
		"CREATE INDEX r_id FOR (n:R) ON (n.id)",
		"CREATE (:R {id: 'r1'}), (:R {id: 'r2'})",
	}
	for _, tc := range []struct {
		name      string
		stmt      string
		rows      []interface{}
		fastPath  func(HotPathTrace) bool
		wantErr   string
		check     string
		wantCheck [][]interface{}
	}{
		{
			name:      "multi-match CREATE leaves a null property unset",
			stmt:      "UNWIND $rows AS row MATCH (a:R {id: row.a}) MATCH (b:R {id: row.b}) CREATE (a)-[:L {x: row.x, y: row.y}]->(b)",
			rows:      []interface{}{map[string]interface{}{"a": "r1", "b": "r2", "x": nil, "y": 7}},
			fastPath:  func(tr HotPathTrace) bool { return tr.UnwindMultiMatchCreateBatch },
			check:     "MATCH ()-[l:L]->() RETURN keys(l) AS k, l.y AS y",
			wantCheck: [][]interface{}{{[]interface{}{"y"}, int64(7)}},
		},
		{
			name:     "merge-chain SET rejects a missing parameter",
			stmt:     "UNWIND $rows AS row MATCH (a:R {id: row.a}) MATCH (b:R {id: row.b}) MERGE (a)-[rel:DEP]->(b) SET rel.t = $missing",
			rows:     []interface{}{map[string]interface{}{"a": "r1", "b": "r2"}},
			fastPath: func(tr HotPathTrace) bool { return tr.UnwindMergeChainBatch },
			wantErr:  "$missing",
		},
		{
			name:      "merge-chain SET null removes the key",
			stmt:      "UNWIND $rows AS row MERGE (n:M {id: row.id}) SET n.t = row.t",
			rows:      []interface{}{map[string]interface{}{"id": "m1", "t": nil}},
			fastPath:  func(tr HotPathTrace) bool { return tr.UnwindMergeChainBatch },
			check:     "MATCH (n:M) RETURN keys(n) AS k",
			wantCheck: [][]interface{}{{[]interface{}{"id"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			for _, q := range setup {
				_, err := exec.Execute(ctx, q, nil)
				require.NoError(t, err, q)
			}
			_, err := exec.Execute(ctx, tc.stmt, map[string]interface{}{"rows": tc.rows})
			assert.True(t, tc.fastPath(exec.LastHotPathTrace()), "expected the fast path to handle %s", tc.stmt)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			chk, err := exec.Execute(ctx, tc.check, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCheck, normalizeSetRouteRows(chk.Rows))
		})
	}
}
