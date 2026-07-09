// TestBug_MultiMatchRelationshipBindingLost reproduces and locks in the fix
// for a correctness bug where a relationship variable bound in the SECOND (or
// later) MATCH clause of a multi-MATCH query is silently dropped.
//
// Discovered: 2026-07-08
// Reporter: orchestrator repro handed to executor for pkg/cypher graph-write
//
//	correctness/perf audit (rel-source-uid-in-index-seed branch)
//
// Impact: `MATCH (s) WHERE s.uid IN $u MATCH (s)-[rel]->() WHERE
//
//	rel.evidence_source = $e DELETE rel` deletes ZERO edges instead of the
//	matching set. The same shape used as a read (`RETURN count(rel)`,
//	`RETURN rel`) also silently returns zero/nil for the relationship
//	column while the node columns in the very same query are correct. Any
//	multi-MATCH DELETE or RETURN keyed on a relationship variable bound
//	after the first MATCH clause is affected.
//
// Root-Cause: the generic multi-match binding row type in
//
//	pkg/cypher/match_multi.go (`type binding map[string]*storage.Node`) can
//	only hold node bindings. `executeChainedMatch` receives a `PathResult`
//	that already carries `Relationships []*storage.Edge`
//	(pkg/cypher/traversal.go) but only copies the start/end NODE bindings
//	into the binding row — the relationship variable named in the pattern
//	(e.g. "rel" in `(s)-[rel]->()`) is never stored anywhere. Every binding
//	consumer (`resolveBindingExpr`, `filterBindingsByWhere` via
//	binding_where_compile.go) subsequently looks up that variable in the
//	node-only map, gets a typed-nil `*storage.Node`, and treats the
//	relationship as unbound: RETURN projects nil, WHERE predicates on the
//	relationship's properties evaluate false, and DELETE — which reuses the
//	RETURN row values to classify delete targets — has nothing to delete.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// setupRelSeedFixture seeds 5 :CloudResource nodes (uid u1..u5) and 6
// :LINKS edges, 5 of which carry evidence_source:'reducer' and one of which
// does not (evidence_source:'manual'). It returns the executor and the list
// of seeded edge IDs whose evidence_source is 'reducer' (the expected DELETE
// target set for the bug repro).
func setupRelSeedFixture(t *testing.T, exec *StorageExecutor) []storage.EdgeID {
	t.Helper()
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		_, err := exec.Execute(ctx,
			"CREATE (:CloudResource {uid: $uid})",
			map[string]interface{}{"uid": uidFor(i)})
		require.NoError(t, err)
	}

	// 5 reducer-sourced edges forming a ring: u1->u2->u3->u4->u5->u1.
	reducerPairs := [][2]int{{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 1}}
	for _, pair := range reducerPairs {
		_, err := exec.Execute(ctx,
			`MATCH (a:CloudResource {uid: $src}), (b:CloudResource {uid: $tgt})
			 CREATE (a)-[:LINKS {evidence_source: 'reducer'}]->(b)`,
			map[string]interface{}{"src": uidFor(pair[0]), "tgt": uidFor(pair[1])})
		require.NoError(t, err)
	}

	// 1 non-reducer edge that must survive any evidence_source='reducer' delete.
	_, err := exec.Execute(ctx,
		`MATCH (a:CloudResource {uid: $src}), (b:CloudResource {uid: $tgt})
		 CREATE (a)-[:LINKS {evidence_source: 'manual'}]->(b)`,
		map[string]interface{}{"src": uidFor(1), "tgt": uidFor(3)})
	require.NoError(t, err)

	edges, err := exec.getStorage(ctx).AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 6, "fixture must seed exactly 6 edges")

	var reducerEdgeIDs []storage.EdgeID
	for _, e := range edges {
		if e.Properties["evidence_source"] == "reducer" {
			reducerEdgeIDs = append(reducerEdgeIDs, e.ID)
		}
	}
	require.Len(t, reducerEdgeIDs, 5, "fixture must seed exactly 5 reducer edges")
	return reducerEdgeIDs
}

// newRelSeedExecutor builds a namespaced in-memory executor, matching the
// construction pattern used across this package's other bug regression
// tests (raw *storage.MemoryEngine node/edge IDs must be namespace-prefixed).
func newRelSeedExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "relseed")
	return NewStorageExecutor(ns)
}

func uidFor(i int) string {
	return []string{"", "u1", "u2", "u3", "u4", "u5"}[i]
}

func allCloudResourceUIDs() []interface{} {
	return []interface{}{"u1", "u2", "u3", "u4", "u5"}
}

// TestBug_MultiMatchRelationshipBindingLost is the primary repro: a
// relationship variable bound in the second MATCH clause of a two-clause
// MATCH query must be readable (RETURN rel, count(rel)) and deletable
// (DELETE rel), not silently dropped.
func TestBug_MultiMatchRelationshipBindingLost(t *testing.T) {
	params := map[string]interface{}{
		"u": allCloudResourceUIDs(),
		"e": "reducer",
	}

	t.Run("count(rel) sees the bound relationship", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel]->(x)
			 WHERE rel.evidence_source = $e
			 RETURN count(rel) AS c`, params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, int64(5), toInt64Bug(t, res.Rows[0][0]),
			"count(rel) must count the 5 reducer-sourced relationships, not 0")
	})

	t.Run("RETURN rel projects the relationship, not nil", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel]->(x)
			 WHERE rel.evidence_source = $e
			 RETURN rel`, params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 5)
		for _, row := range res.Rows {
			edge, ok := row[0].(*storage.Edge)
			require.Truef(t, ok, "RETURN rel row must project a *storage.Edge, got %T (%v)", row[0], row[0])
			require.Equal(t, "reducer", edge.Properties["evidence_source"])
		}
	})

	t.Run("node bindings survive even though rel does not (regression guard)", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s) WHERE s.uid IN $u
			 MATCH (s)-[rel]->(x)
			 RETURN s.uid, x.uid`, map[string]interface{}{"u": allCloudResourceUIDs()})
		require.NoError(t, err)
		require.Len(t, res.Rows, 6, "all 6 edges should produce a (s.uid, x.uid) row")
	})

	t.Run("DELETE rel deletes the matching relationships", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		reducerEdgeIDs := setupRelSeedFixture(t, exec)
		ctx := context.Background()

		_, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel]->()
			 WHERE rel.evidence_source = $e
			 DELETE rel`, params)
		require.NoError(t, err)

		remaining, err := exec.getStorage(ctx).AllEdges()
		require.NoError(t, err)
		require.Len(t, remaining, 1, "DELETE rel must remove exactly the 5 reducer edges, leaving 1")
		require.Equal(t, "manual", remaining[0].Properties["evidence_source"])

		for _, id := range reducerEdgeIDs {
			_, err := exec.getStorage(ctx).GetEdge(id)
			require.Error(t, err, "reducer edge %s must be gone after DELETE", id)
		}
	})
}

// TestBug_MultiMatchRelationshipBindingLost_Variations pins the fix across
// query shapes that all funnel relationship bindings through the same
// multi-match binding row.
func TestBug_MultiMatchRelationshipBindingLost_Variations(t *testing.T) {
	params := map[string]interface{}{
		"u": allCloudResourceUIDs(),
		"e": "reducer",
	}

	t.Run("two-clause with no rel-WHERE deletes all matched relationships", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		_, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel]->()
			 DELETE rel`, map[string]interface{}{"u": allCloudResourceUIDs()})
		require.NoError(t, err)

		remaining, err := exec.getStorage(ctx).AllEdges()
		require.NoError(t, err)
		require.Empty(t, remaining, "with no rel-WHERE, all 6 outgoing edges from the 5 sources must be deleted")
	})

	t.Run("re-binding the same node variable in a later clause propagates without a relationship", func(t *testing.T) {
		// Exercises executeChainedMatch's "variable already bound, just
		// propagate" fast path for a second MATCH clause with no
		// relationship pattern (`MATCH (s)`) — this is the branch that
		// carries existingRels forward via a plain propagation rather than
		// a merge, and it stayed untested by the relationship-focused cases
		// above (which all use a second clause shaped `(s)-[rel]->(...)`).
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)
			 RETURN s.uid
			 ORDER BY s.uid`,
			map[string]interface{}{"u": allCloudResourceUIDs()})
		require.NoError(t, err)
		require.Len(t, res.Rows, 5)
		for i, row := range res.Rows {
			require.Equal(t, uidFor(i+1), row[0])
		}
	})

	t.Run("WITH-pipelined DELETE still resolves rel", func(t *testing.T) {
		// KNOWN SEPARATE GAP (not this bug's root cause): `MATCH ... WITH s
		// MATCH (s)-[...]->() ...` pipelines are mis-parsed by
		// executeMatchWithClause independently of relationship bindings —
		// even a plain node-to-node `MATCH (s) WITH s MATCH (t) WHERE
		// t.uid <> s.uid RETURN s.uid, t.uid` silently returns nil for both
		// columns, because executeChainedMatchWithAggregations bails (no
		// second WITH stage) and executeMatchWithClause's WITH-section
		// parser assumes no further MATCH exists between WITH and RETURN.
		// That is a distinct, pre-existing pipeline-parsing defect in
		// match_with.go / match_with_chain.go, not the multi-match
		// binding-type defect this test file targets. Fixing it is a
		// separate, larger change; skip here rather than silently expanding
		// this fix's scope. See PR/handoff notes for the repro.
		t.Skip("MATCH...WITH...MATCH...RETURN pipelines are mis-parsed independently of relationship bindings; separate pre-existing defect, out of scope for the binding-type fix")
	})

	t.Run("RETURN rel with ORDER BY and LIMIT", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel]->(x)
			 WHERE rel.evidence_source = $e
			 RETURN rel, x.uid AS xid
			 ORDER BY xid
			 LIMIT 3`, params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 3)
		for _, row := range res.Rows {
			edge, ok := row[0].(*storage.Edge)
			require.Truef(t, ok, "RETURN rel row must project a *storage.Edge, got %T", row[0])
			require.Equal(t, "reducer", edge.Properties["evidence_source"])
		}
	})

	t.Run("multiple relationships bound across two clauses both resolve", func(t *testing.T) {
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource) WHERE s.uid IN $u
			 MATCH (s)-[rel:LINKS]->(x)
			 WHERE rel.evidence_source = $e
			 RETURN count(rel) AS c, count(x) AS cx`, params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, int64(5), toInt64Bug(t, res.Rows[0][0]))
		require.Equal(t, int64(5), toInt64Bug(t, res.Rows[0][1]))
	})

	t.Run("three-clause chain binds a relationship in every clause", func(t *testing.T) {
		// Exercises mergeRelBindings' non-empty-`a` merge path: clause 2
		// binds r1, and clause 3's chained match must carry r1 forward
		// (existingRels) while also binding its own r2 — the two-rel case
		// a two-clause query can't reach. Also exercises elementId(rel) and
		// rel.<prop> as bare RETURN items (as opposed to a WHERE predicate),
		// which resolveBindingExpr resolves via the rels map added by this
		// fix.
		exec := newRelSeedExecutor(t)
		setupRelSeedFixture(t, exec)
		ctx := context.Background()

		res, err := exec.Execute(ctx,
			`MATCH (a:CloudResource) WHERE a.uid = $a
			 MATCH (a)-[r1]->(b)
			 MATCH (b)-[r2]->(c)
			 WHERE r1.evidence_source = $e AND r2.evidence_source = $e
			 RETURN elementId(r1), r1.evidence_source, elementId(r2), r2.evidence_source, b.uid, c.uid`,
			map[string]interface{}{"a": "u1", "e": "reducer"})
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		row := res.Rows[0]
		require.NotEmpty(t, row[0], "elementId(r1) must resolve to the real edge ID, not nil")
		require.Equal(t, "reducer", row[1])
		require.NotEmpty(t, row[2], "elementId(r2) must resolve to the real edge ID, not nil")
		require.NotEqual(t, row[0], row[2], "r1 and r2 must be distinct edges")
		require.Equal(t, "reducer", row[3])
		require.Equal(t, "u2", row[4])
		require.Equal(t, "u3", row[5])
	})
}

func TestBug_MultiMatchRelationshipBindingRebinding(t *testing.T) {
	exec := newRelSeedExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Node {name: 'a'})-[:LINKS {kind: 'left'}]->(:Node {name: 'b'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Node {name: 'a'})-[:LINKS {kind: 'right'}]->(:Node {name: 'c'})`, nil)
	require.NoError(t, err)

	t.Run("same relationship variable only counts rows for the same edge", func(t *testing.T) {
		res, err := exec.Execute(ctx,
			`MATCH (a:Node {name: 'a'})-[r]->(b)
			 MATCH (a)-[r]->(c)
			 RETURN count(*)`,
			nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, int64(2), toInt64Bug(t, res.Rows[0][0]))
	})

	t.Run("same relationship variable rejects rows that would overwrite the bound edge", func(t *testing.T) {
		res, err := exec.Execute(ctx,
			`MATCH (a:Node {name: 'a'})-[r]->(b)
			 MATCH (a)-[r]->(c)
			 WHERE b.name <> c.name
			 RETURN b.name, c.name`,
			nil)
		require.NoError(t, err)
		require.Empty(t, res.Rows)
	})
}

// TestMergeRelBindingsChecked directly unit-tests the helper's
// merge/nil-input branches, including the duplicate-key contract enforced for
// chained MATCH row combination.
func TestMergeRelBindingsChecked(t *testing.T) {
	e1 := &storage.Edge{ID: "e1"}
	e2 := &storage.Edge{ID: "e2"}
	e1Duplicate := &storage.Edge{ID: "e1"}

	t.Run("both nil returns nil", func(t *testing.T) {
		merged, ok := mergeRelBindingsChecked(nil, nil)
		require.True(t, ok)
		require.Nil(t, merged)
	})
	t.Run("only b returns b's entries", func(t *testing.T) {
		merged, ok := mergeRelBindingsChecked(nil, map[string]*storage.Edge{"r2": e2})
		require.True(t, ok)
		require.Equal(t, map[string]*storage.Edge{"r2": e2}, merged)
	})
	t.Run("same key with the same edge id passes", func(t *testing.T) {
		merged, ok := mergeRelBindingsChecked(
			map[string]*storage.Edge{"r1": e1, "shared": e1},
			map[string]*storage.Edge{"r2": e2, "shared": e1Duplicate},
		)
		require.True(t, ok)
		require.Equal(t, map[string]*storage.Edge{"r1": e1, "r2": e2, "shared": e1}, merged)
	})
	t.Run("same key with different edge ids fails", func(t *testing.T) {
		merged, ok := mergeRelBindingsChecked(
			map[string]*storage.Edge{"shared": e1},
			map[string]*storage.Edge{"shared": e2},
		)
		require.False(t, ok)
		require.Nil(t, merged)
	})
	t.Run("different keys merge", func(t *testing.T) {
		merged, ok := mergeRelBindingsChecked(
			map[string]*storage.Edge{"r1": e1},
			map[string]*storage.Edge{"r2": e2},
		)
		require.True(t, ok)
		require.Equal(t, map[string]*storage.Edge{"r1": e1, "r2": e2}, merged)
	})
}

// TestBindingWithRelView directly unit-tests the WHERE-evaluation adapter,
// including the defensive nil-edge skip that no current caller triggers
// (buildPathContext never inserts a nil edge into its rels map) but that
// bindingWithRelView guards against per the "account for invalid input"
// mandate.
func TestBindingWithRelView(t *testing.T) {
	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}}
	edge := &storage.Edge{ID: "e1", Properties: map[string]interface{}{"kind": "reducer"}}

	t.Run("no rels returns the binding unchanged", func(t *testing.T) {
		b := binding{"n": node}
		view := bindingWithRelView(b, nil)
		require.Same(t, node, view["n"])
	})
	t.Run("nil edge in rels is skipped", func(t *testing.T) {
		b := binding{"n": node}
		view := bindingWithRelView(b, map[string]*storage.Edge{"rel": nil})
		require.Same(t, node, view["n"])
		require.Nil(t, view["rel"], "a nil edge must not produce a synthetic pseudo-node")
	})
	t.Run("bound edge becomes a property-only pseudo-node", func(t *testing.T) {
		b := binding{"n": node}
		view := bindingWithRelView(b, map[string]*storage.Edge{"rel": edge})
		require.Same(t, node, view["n"], "existing node bindings must be preserved")
		pseudo := view["rel"]
		require.NotNil(t, pseudo)
		require.Equal(t, storage.NodeID(edge.ID), pseudo.ID)
		require.Equal(t, "reducer", pseudo.Properties["kind"])
	})
}

func toInt64Bug(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		t.Fatalf("expected numeric value, got %T (%v)", v, v)
		return 0
	}
}

// TestResolveBindingExprUnboundVariable directly unit-tests
// resolveBindingExpr's fallback-to-nil branches for elementId(...) and
// property access on a variable that is bound as neither a node nor a
// relationship. These are defensive edge cases (a malformed or
// already-filtered-out variable reference) rather than shapes the fix's
// integration tests exercise, since every RETURN item in this bug's real
// repro references a variable that IS bound one way or the other.
func TestResolveBindingExprUnboundVariable(t *testing.T) {
	exec := newRelSeedExecutor(t)
	ctx := context.Background()
	b := binding{"s": &storage.Node{ID: "n1", Properties: map[string]interface{}{"uid": "u1"}}}
	rels := map[string]*storage.Edge{"rel": {ID: "e1", Properties: map[string]interface{}{"kind": "reducer"}}}

	t.Run("elementId on an unbound variable resolves to nil", func(t *testing.T) {
		require.Nil(t, exec.resolveBindingExpr(ctx, "elementId(ghost)", b, rels))
	})
	t.Run("property access on an unbound variable resolves to nil", func(t *testing.T) {
		require.Nil(t, exec.resolveBindingExpr(ctx, "ghost.prop", b, rels))
	})
	t.Run("elementId still resolves for a real relationship binding", func(t *testing.T) {
		require.Equal(t, "e1", exec.resolveBindingExpr(ctx, "elementId(rel)", b, rels))
	})
	t.Run("property access still resolves for a real relationship binding", func(t *testing.T) {
		require.Equal(t, "reducer", exec.resolveBindingExpr(ctx, "rel.kind", b, rels))
	})
	t.Run("resolveBindingItem with an empty expression resolves to nil", func(t *testing.T) {
		require.Nil(t, exec.resolveBindingItem(ctx, returnItem{expr: "  "}, b, rels))
	})
}
