// TestBug_InListStartNodeDoesNotIndexSeed reproduces and locks in the fix for
// a performance bug where a single-clause relationship-pattern MATCH with a
// `WHERE <startVar>.<prop> IN <list>` predicate on the start node does not
// index-seed the traversal's start nodes, even though the exact same
// IN-list predicate on a plain (non-relationship) node MATCH already does.
//
// Discovered: 2026-07-08
// Reporter: orchestrator repro handed to executor for pkg/cypher graph-write
//
//	correctness/perf audit (rel-source-uid-in-index-seed branch)
//
// Impact: `MATCH (s:CloudResource)-[rel]->() WHERE s.uid IN $u AND
//
//	rel.evidence_source = $e DELETE rel` (or the RETURN-shaped equivalent)
//	is correct but scans every :CloudResource node in the graph to find the
//	handful named in $u, then walks every one of their outgoing edges.
//	Measured 28s vs 0.7s for the equivalent node-only `WHERE uid IN`
//	lookup on a representative dataset. Any relationship-pattern query
//	whose only start-node pruning predicate is an IN-list is affected.
//
// Root-Cause: pkg/cypher/traversal.go's `traverseGraph` only index-seeds
//
//	start nodes for a single INLINE property (`{uid:$v}`,
//	`len(StartNode.properties)==1`) at line ~1366. A WHERE-clause IN-list
//	predicate is not an inline property, so it never reaches that fast
//	path. The higher-level caller,
//	executeMatchWithRelationshipsWithPath (traversal.go), *does* have a
//	WHERE-clause start-node pruning chain (tryCollectNodesFromIDEqualityCompound,
//	tryCollectNodesFromPropertyIndex, tryCollectNodesFromPropertyIndexNotNull,
//	tryCollectNodesFromStartPropertyScan) — but that chain never calls the
//	IN-list index-seek helpers (tryCollectNodesFromPropertyIndexIn /
//	tryCollectNodesFromPropertyIndexInLiteral) that match_index_seek.go
//	already implements and match.go/clauses.go/executor_mutations.go
//	already use for plain node MATCH and single-clause DELETE. The
//	traversal path simply never wires them in, so it falls through to
//	loadNodesWithTemporalViewport(labels) — an O(all label nodes) scan —
//	for every relationship-pattern query anchored by an IN-list.
package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedInListRelSeedPopulation creates `count` :CloudResource nodes with a
// unique `uid`, indexed via CREATE INDEX, plus one outgoing :LINKS edge per
// node so a relationship-pattern traversal has something to expand. Returns
// the uids of the first `targetCount` nodes (the IN-list sublist used by the
// test/benchmark) and their evidence_source ('reducer' for all but one, to
// mirror the original bug repro).
func seedInListRelSeedPopulation(t testing.TB, exec *StorageExecutor, count, targetCount int) []interface{} {
	t.Helper()
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE INDEX cloudresource_uid IF NOT EXISTS FOR (n:CloudResource) ON (n.uid)", nil)
	require.NoError(t, err)

	for i := 0; i < count; i++ {
		_, err := exec.Execute(ctx,
			"CREATE (:CloudResource {uid: $uid})",
			map[string]interface{}{"uid": fmt.Sprintf("cr-%06d", i)})
		require.NoErrorf(t, err, "seed node %d", i)
	}
	for i := 0; i+1 < count; i++ {
		src := fmt.Sprintf("cr-%06d", i)
		tgt := fmt.Sprintf("cr-%06d", i+1)
		evidence := "reducer"
		if i == 0 {
			evidence = "manual"
		}
		_, err := exec.Execute(ctx,
			`MATCH (a:CloudResource {uid: $src}), (b:CloudResource {uid: $tgt})
			 CREATE (a)-[:LINKS {evidence_source: $evidence}]->(b)`,
			map[string]interface{}{"src": src, "tgt": tgt, "evidence": evidence})
		require.NoErrorf(t, err, "seed edge %d", i)
	}

	targets := make([]interface{}, 0, targetCount)
	for i := 0; i < targetCount && i < count; i++ {
		targets = append(targets, fmt.Sprintf("cr-%06d", i))
	}
	return targets
}

// TestBug_InListStartNodeDoesNotIndexSeed pins the correctness contract: an
// IN-list-anchored relationship traversal must return the same rows whether
// or not the index-seek fast path fires. This is the safety net for the
// perf fix — it must not change results, only the seed source.
func TestBug_InListStartNodeDoesNotIndexSeed(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	targets := seedInListRelSeedPopulation(t, exec, 50, 10)
	ctx := context.Background()

	res, err := exec.Execute(ctx,
		`MATCH (s:CloudResource)-[rel]->(x)
		 WHERE s.uid IN $u
		 RETURN s.uid, x.uid
		 ORDER BY s.uid`,
		map[string]interface{}{"u": targets})
	require.NoError(t, err)
	// Each of the 10 target uids (cr-000000..cr-000009) has exactly one
	// outgoing :LINKS edge in the seeded chain, so 10 rows are expected
	// regardless of how start nodes were seeded.
	require.Len(t, res.Rows, 10)
	for i, row := range res.Rows {
		require.Equal(t, fmt.Sprintf("cr-%06d", i), row[0])
		require.Equal(t, fmt.Sprintf("cr-%06d", i+1), row[1])
	}
}

// TestBug_InListStartNodeDoesNotIndexSeed_NoLabelScan is the scan-budget pin
// (mirrors the TestMatchPatternProperty_*NoScan family in
// match_pattern_property_index_test.go): an IN-list-anchored relationship
// traversal over an indexed property must seed via the property index, not
// via a full GetNodesByLabel/AllNodes scan of the label population, and must
// not expand outgoing edges for source nodes outside the IN-list.
func TestBug_InListStartNodeDoesNotIndexSeed_NoLabelScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	const population = 500
	const subListSize = 20
	targets := seedInListRelSeedPopulation(t, exec, population, subListSize)
	wrapped.reset()

	ctx := context.Background()
	res, err := exec.Execute(ctx,
		`MATCH (s:CloudResource)-[rel]->(x)
		 WHERE s.uid IN $u
		 RETURN count(rel) AS c`,
		map[string]interface{}{"u": targets})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(subListSize), toInt64Bug(t, res.Rows[0][0]))

	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"IN-list-anchored relationship MATCH leaked %d GetNodesByLabel() calls — index seed missing",
		wrapped.GetNodesByLabelCalls())
	require.Zerof(t, wrapped.AllNodesCalls(),
		"IN-list-anchored relationship MATCH leaked %d AllNodes() calls — index seed missing",
		wrapped.AllNodesCalls())
	require.LessOrEqualf(t, wrapped.OutgoingEdgeCalls(), int64(subListSize),
		"IN-list-anchored relationship MATCH expanded %d source nodes' outgoing edges, want <= %d (sublist size) — full population scanned instead of index-seeded sublist",
		wrapped.OutgoingEdgeCalls(), subListSize)
}

// TestBug_InListStartNodeDoesNotIndexSeed_DeleteVariant pins the exact DELETE
// shape from the original repro: a single-clause relationship-pattern DELETE
// anchored by an IN-list on the start node AND a property predicate on the
// bound relationship.
//
// NOTE: this only pins correctness, not the scan budget. DELETE mutations
// auto-wrap in an implicit transaction (looksLikeWriteQuery in
// transaction.go), and transactionStorageWrapper.GetNodesByLabel /
// GetOutgoingEdges (transaction_storage_wrapper.go) read through
// `w.tx` (*storage.BadgerTransaction), not through `w.underlying` — the
// engine scanCountingEngine wraps. So a scan-count assertion here would
// silently pass regardless of whether the traversal seed fix is present,
// because the calls it's trying to observe happen on an object the counting
// wrapper never sees. TestBug_InListStartNodeDoesNotIndexSeed_NoLabelScan
// (the RETURN-shaped, non-transactional variant, which exercises the exact
// same executeMatchWithRelationshipsWithPath/traverseGraph code path) is the
// scan-budget proof for this fix; BenchmarkInListAnchoredRelDelete is the
// wall-clock proof for both shapes.
func TestBug_InListStartNodeDoesNotIndexSeed_DeleteVariant(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	const population = 300
	const subListSize = 15
	targets := seedInListRelSeedPopulation(t, exec, population, subListSize)

	ctx := context.Background()
	_, err := exec.Execute(ctx,
		`MATCH (s:CloudResource)-[rel]->()
		 WHERE s.uid IN $u AND rel.evidence_source = $e
		 DELETE rel`,
		map[string]interface{}{"u": targets, "e": "reducer"})
	require.NoError(t, err)

	// Correctness: cr-000000's edge is evidence_source='manual' (seeded
	// deliberately) and must survive; the rest in the sublist are 'reducer'
	// and must be gone.
	verify, err := exec.Execute(ctx,
		`MATCH (s:CloudResource)-[rel]->() WHERE s.uid IN $u RETURN count(rel) AS c`,
		map[string]interface{}{"u": targets})
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(1), toInt64Bug(t, verify.Rows[0][0]),
		"only the single manual-evidence edge among the sublist should survive")
}

// BenchmarkInListAnchoredRelMatch measures the cost of an IN-list-anchored
// relationship-pattern MATCH with a 100-node target sublist — the exact
// shape from the bug repro (`WHERE s.uid IN $u`), read-only so repeated b.N
// iterations are apples-to-apples (a DELETE variant would only remove
// matching edges once, making iteration 1 incomparable to the rest).
//
// Population is 50k rather than the originally-scoped 5k: at 5k nodes an
// in-process MemoryEngine's GetNodesByLabel/GetOutgoingEdges are cheap
// enough (~12.6us/op unfixed vs ~10.7us/op fixed) that the O(n) vs
// O(sublist) shape change is inside measurement noise. At 50k the
// algorithmic difference dominates and is unambiguous — see CHANGELOG.md
// for the recorded before/after numbers (~365x ns/op, ~364x B/op, ~496x
// allocs/op on this branch). Before this fix, this scans all 50000
// :CloudResource nodes and expands all 50000 outgoing edge sets every
// iteration; after the fix it seeds from the 100-entry property index
// lookup instead.
func BenchmarkInListAnchoredRelMatch(b *testing.B) {
	exec, _ := newCountingExecutor(b)
	const population = 50000
	const subListSize = 100
	targets := seedInListRelSeedPopulation(b, exec, population, subListSize)
	params := map[string]interface{}{"u": targets}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx,
			`MATCH (s:CloudResource)-[rel]->(x)
			 WHERE s.uid IN $u
			 RETURN count(rel) AS c`, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != 1 {
			b.Fatalf("expected 1 row, got %d", len(res.Rows))
		}
	}
}

// TestTryCollectNodesFromPropertyIndexInCompound directly unit-tests the new
// helper's branches (empty clause, simple param/literal forms, AND-conjunct
// param/literal forms, empty-conjunct skip, and the no-match fallback).
// Direct calls are used — rather than routing everything through
// exec.Execute — because the real entry points (executeMatch/executeDelete)
// always substitute `$param` references into literal text before reaching
// this helper, so the param-taking branches (added for parity with
// tryCollectNodesFromIDEqualityCompound and future callers that might not
// pre-substitute) are otherwise unreachable from an integration test.
func TestTryCollectNodesFromPropertyIndexInCompound(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx,
		"CREATE INDEX cr_uid IF NOT EXISTS FOR (n:CloudResource) ON (n.uid)", nil)
	require.NoError(t, err)
	for _, uid := range []string{"a1", "a2", "a3"} {
		_, err := exec.Execute(ctx, "CREATE (:CloudResource {uid: $uid})", map[string]interface{}{"uid": uid})
		require.NoError(t, err)
	}
	nodePattern := nodePatternInfo{variable: "s", labels: []string{"CloudResource"}}

	t.Run("empty clause", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern, "   ", nil)
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, nodes)
	})

	t.Run("simple param form", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"s.uid IN $u", map[string]interface{}{"u": []interface{}{"a1", "a2"}})
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 2)
	})

	t.Run("simple literal form", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"s.uid IN ['a1', 'a3']", nil)
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 2)
	})

	t.Run("AND-conjunct param form", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"s.uid IN $u AND rel.evidence_source = $e",
			map[string]interface{}{"u": []interface{}{"a1"}, "e": "reducer"})
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 1)
	})

	t.Run("AND-conjunct literal form", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"rel.evidence_source = 'reducer' AND s.uid IN ['a2', 'a3']", nil)
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 2)
	})

	t.Run("empty conjunct term is skipped", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"() AND s.uid IN ['a1']", nil)
		require.NoError(t, err)
		require.True(t, used)
		require.Len(t, nodes, 1)
	})

	t.Run("no recognized IN-list conjunct falls back", func(t *testing.T) {
		nodes, used, err := exec.tryCollectNodesFromPropertyIndexInCompound(ctx, nodePattern,
			"s.uid = 'a1' AND rel.evidence_source = 'reducer'", nil)
		require.NoError(t, err)
		require.False(t, used)
		require.Nil(t, nodes)
	})
}
