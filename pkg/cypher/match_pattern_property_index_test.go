// Pattern-inline property MATCH fast-path tests.
//
// `executeMatchForContext` historically only consulted property indexes when
// the equality was written in a WHERE clause. Pattern-inline equality
// (`MATCH (n {id: $x})` or `MATCH (n:Label {id: $x})`) fell through to either
// `AllNodes` (labelless) or `GetNodesByLabel` (with-label) and was then
// filtered linearly by `nodeMatchesProps`.
//
// That is O(label-population) per probe — and for graphify's edge MERGE shape
// (`MATCH (a {id:$src}),(b {id:$tgt}) MERGE ...`) it's O(node-count) twice
// per edge, which dominates ingestion time on any realistic graph.
//
// These tests pin a generic, shape-agnostic contract for the executor:
//
//   1. When a pattern carries an inline property and a property index covers
//      that property (regardless of whether the pattern names a label), the
//      executor MUST probe the index and MUST NOT issue an `AllNodes` /
//      `GetNodesByLabel` full-population scan.
//   2. The contract holds for N inline properties (each indexed property
//      narrows further), not just `{id}`.
//   3. The contract holds across labelled and labelless patterns.
//   4. Correctness is preserved: pattern-inline filters that the index can
//      narrow but not exactly express (e.g. a non-indexed second prop) still
//      end up with the correct row set after fast-path narrowing + residual
//      property filtering.
//
// The first three are pinned via a counting engine wrapper that fails the
// test if any `AllNodes` / `GetNodesByLabel` call escapes during a probe that
// SHOULD be satisfied by the index. The fourth is pinned via direct result
// equality.

package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// scanCountingEngine wraps a *storage.MemoryEngine and increments counters on
// the full-population scan paths so the test can assert the fast path was
// taken. It is intentionally a thin wrapper that delegates everything else,
// because the goal is to observe (not change) executor behaviour.
type scanCountingEngine struct {
	*storage.MemoryEngine
	allNodesCalls       int64
	getNodesByLabelHits int64
	outgoingEdgeCalls   int64
	outgoingEdgeErr     error
	incomingEdgeErr     error
	getNodeErr          error
}

func (e *scanCountingEngine) AllNodes() ([]*storage.Node, error) {
	atomic.AddInt64(&e.allNodesCalls, 1)
	return e.MemoryEngine.AllNodes()
}

func (e *scanCountingEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	atomic.AddInt64(&e.getNodesByLabelHits, 1)
	return e.MemoryEngine.GetNodesByLabel(label)
}

func (e *scanCountingEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	atomic.AddInt64(&e.outgoingEdgeCalls, 1)
	if e.outgoingEdgeErr != nil {
		return nil, e.outgoingEdgeErr
	}
	return e.MemoryEngine.GetOutgoingEdges(nodeID)
}

func (e *scanCountingEngine) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if e.incomingEdgeErr != nil {
		return nil, e.incomingEdgeErr
	}
	return e.MemoryEngine.GetIncomingEdges(nodeID)
}

func (e *scanCountingEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.MemoryEngine.GetNode(id)
}

func (e *scanCountingEngine) AllNodesCalls() int64 { return atomic.LoadInt64(&e.allNodesCalls) }
func (e *scanCountingEngine) GetNodesByLabelCalls() int64 {
	return atomic.LoadInt64(&e.getNodesByLabelHits)
}
func (e *scanCountingEngine) OutgoingEdgeCalls() int64 {
	return atomic.LoadInt64(&e.outgoingEdgeCalls)
}

func (e *scanCountingEngine) reset() {
	atomic.StoreInt64(&e.allNodesCalls, 0)
	atomic.StoreInt64(&e.getNodesByLabelHits, 0)
	atomic.StoreInt64(&e.outgoingEdgeCalls, 0)
}

// newCountingExecutor builds a StorageExecutor over a wrapped MemoryEngine.
// The wrapper sits BELOW the NamespacedEngine so that every AllNodes /
// GetNodesByLabel call the executor (or its tx wrapper) issues on the
// underlying engine increments the counter.
func newCountingExecutor(t testing.TB) (*StorageExecutor, *scanCountingEngine) {
	t.Helper()
	mem := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = mem.Close() })
	wrapped := &scanCountingEngine{MemoryEngine: mem}
	ns := storage.NewNamespacedEngine(wrapped, "scan")
	exec := NewStorageExecutor(ns)
	return exec, wrapped
}

// seedScanTestPopulation creates `count` :Code nodes with `{id, sku, tier}`
// properties. The id is unique per node, sku is unique per node, tier is a
// repeated bucket. Indexes are created on id and sku (the two indexed
// pattern properties). tier is intentionally NOT indexed so it can serve as
// the "residual filter must still narrow correctly" case.
func seedScanTestPopulation(t testing.TB, exec *StorageExecutor, count int) {
	t.Helper()
	ctx := context.Background()

	for _, ddl := range []string{
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
		"CREATE INDEX code_sku IF NOT EXISTS FOR (n:Code) ON (n.sku)",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoErrorf(t, err, "ddl %q", ddl)
	}

	for i := 0; i < count; i++ {
		params := map[string]interface{}{
			"id":   fmt.Sprintf("id_%05d", i),
			"sku":  fmt.Sprintf("sku_%05d", i),
			"tier": fmt.Sprintf("tier_%d", i%4),
		}
		_, err := exec.Execute(ctx,
			"CREATE (n:Code {id:$id, sku:$sku, tier:$tier})", params)
		require.NoErrorf(t, err, "seed %d", i)
	}
}

// --- correctness ------------------------------------------------------------

// TestMatchPatternProperty_LabelledSinglePropReturnsCorrectRow ensures the
// post-fast-path result set is correct for a single-prop labelled probe.
// This is a safety net — once the fast path lands, results must not regress.
func TestMatchPatternProperty_LabelledSinglePropReturnsCorrectRow(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 200)

	ctx := context.Background()
	res, err := exec.Execute(ctx,
		"MATCH (n:Code {id:$id}) RETURN n.sku",
		map[string]interface{}{"id": "id_00042"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "sku_00042", res.Rows[0][0])
}

// TestMatchPatternProperty_LabellessSinglePropReturnsCorrectRow does the same
// for a labelless `MATCH (n {id:$id})` — the shape graphify emits.
func TestMatchPatternProperty_LabellessSinglePropReturnsCorrectRow(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 200)

	ctx := context.Background()
	res, err := exec.Execute(ctx,
		"MATCH (n {id:$id}) RETURN n.sku",
		map[string]interface{}{"id": "id_00099"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "sku_00099", res.Rows[0][0])
}

// TestMatchPatternProperty_NaryIndexedReturnsCorrectRow probes with TWO
// inline indexed properties; the result set must equal the intersection of
// each individual probe.
func TestMatchPatternProperty_NaryIndexedReturnsCorrectRow(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 200)

	ctx := context.Background()
	// Same row matches both id and sku — should return exactly one.
	res, err := exec.Execute(ctx,
		"MATCH (n:Code {id:$id, sku:$sku}) RETURN n.tier",
		map[string]interface{}{"id": "id_00011", "sku": "sku_00011"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	// Mismatched id+sku — should return zero, NOT a partial index hit.
	res2, err := exec.Execute(ctx,
		"MATCH (n:Code {id:$id, sku:$sku}) RETURN n.tier",
		map[string]interface{}{"id": "id_00011", "sku": "sku_99999"})
	require.NoError(t, err)
	require.Empty(t, res2.Rows)
}

// TestMatchPatternProperty_IndexedPlusResidualFilter probes with one indexed
// property and one non-indexed property; the executor must use the index to
// narrow then apply the residual filter on its own.
func TestMatchPatternProperty_IndexedPlusResidualFilter(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 200)

	ctx := context.Background()
	// id_00007 → tier "tier_3" (7 % 4 = 3)
	res, err := exec.Execute(ctx,
		"MATCH (n:Code {id:$id, tier:$tier}) RETURN n.sku",
		map[string]interface{}{"id": "id_00007", "tier": "tier_3"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "sku_00007", res.Rows[0][0])

	// Same id, wrong tier — residual filter must drop the row.
	res2, err := exec.Execute(ctx,
		"MATCH (n:Code {id:$id, tier:$tier}) RETURN n.sku",
		map[string]interface{}{"id": "id_00007", "tier": "tier_0"})
	require.NoError(t, err)
	require.Empty(t, res2.Rows)
}

// --- scan-budget pins (these FAIL today, pass after fast-path lands) -------

// TestMatchPatternProperty_LabelledSinglePropNoScan asserts that a labelled
// MATCH with an indexed pattern-inline property does not trigger an
// `AllNodes` or `GetNodesByLabel` full-population scan.
//
// This is the generic, shape-agnostic contract — `MATCH (n:Label {p:$v})`
// must go through the property index when one exists for (Label, p).
func TestMatchPatternProperty_LabelledSinglePropNoScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)
	wrapped.reset()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := exec.Execute(ctx,
			"MATCH (n:Code {id:$id}) RETURN n.sku",
			map[string]interface{}{"id": fmt.Sprintf("id_%05d", i)})
		require.NoError(t, err)
	}

	require.Zerof(t, wrapped.AllNodesCalls(),
		"MATCH (n:Code {id:$id}) leaked %d AllNodes() calls — index probe missing",
		wrapped.AllNodesCalls())
	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"MATCH (n:Code {id:$id}) leaked %d GetNodesByLabel() calls — index probe missing",
		wrapped.GetNodesByLabelCalls())
}

// TestMatchPatternProperty_LabellessSinglePropNoScan asserts the same for the
// labelless shape graphify uses. The executor cannot dispatch to
// `GetNodesByLabel` (no label), so the only acceptable plan is the property
// index probe.
func TestMatchPatternProperty_LabellessSinglePropNoScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)
	wrapped.reset()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := exec.Execute(ctx,
			"MATCH (n {id:$id}) RETURN n.sku",
			map[string]interface{}{"id": fmt.Sprintf("id_%05d", i)})
		require.NoError(t, err)
	}

	require.Zerof(t, wrapped.AllNodesCalls(),
		"MATCH (n {id:$id}) leaked %d AllNodes() calls — labelless index probe missing",
		wrapped.AllNodesCalls())
}

// TestMatchPatternProperty_NaryIndexedNoScan asserts that N inline indexed
// properties are still served by the index (not just the first).
func TestMatchPatternProperty_NaryIndexedNoScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)
	wrapped.reset()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := exec.Execute(ctx,
			"MATCH (n:Code {id:$id, sku:$sku}) RETURN n.tier",
			map[string]interface{}{
				"id":  fmt.Sprintf("id_%05d", i),
				"sku": fmt.Sprintf("sku_%05d", i),
			})
		require.NoError(t, err)
	}

	require.Zerof(t, wrapped.AllNodesCalls(),
		"MATCH (n:Code {id, sku}) leaked %d AllNodes() calls", wrapped.AllNodesCalls())
	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"MATCH (n:Code {id, sku}) leaked %d GetNodesByLabel() calls — N-ary probe missing",
		wrapped.GetNodesByLabelCalls())
}

// TestMatchPatternProperty_IndexedPlusResidualNoScan ensures the fast path
// fires even when ONE of the inline props is non-indexed: the executor must
// probe the indexed prop and then apply the residual filter in-process.
func TestMatchPatternProperty_IndexedPlusResidualNoScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)
	wrapped.reset()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		_, err := exec.Execute(ctx,
			"MATCH (n:Code {id:$id, tier:$tier}) RETURN n.sku",
			map[string]interface{}{
				"id":   fmt.Sprintf("id_%05d", i),
				"tier": fmt.Sprintf("tier_%d", i%4),
			})
		require.NoError(t, err)
	}

	require.Zerof(t, wrapped.AllNodesCalls(),
		"MATCH (n:Code {id, tier}) leaked %d AllNodes() calls", wrapped.AllNodesCalls())
	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"MATCH (n:Code {id, tier}) leaked %d GetNodesByLabel() calls", wrapped.GetNodesByLabelCalls())
}

// TestMatchPatternProperty_EdgeShapeNoDoubleScan locks in the graphify edge
// shape: `MATCH (a {id:$src}),(b {id:$tgt}) ...` must not full-scan twice
// per edge. This is the workload that motivated the whole fix.
func TestMatchPatternProperty_EdgeShapeNoDoubleScan(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)
	wrapped.reset()

	ctx := context.Background()
	for i := 0; i+1 < 20; i++ {
		_, err := exec.Execute(ctx,
			"MATCH (a {id:$src}),(b {id:$tgt}) RETURN a.sku, b.sku",
			map[string]interface{}{
				"src": fmt.Sprintf("id_%05d", i),
				"tgt": fmt.Sprintf("id_%05d", i+1),
			})
		require.NoError(t, err)
	}

	require.Zerof(t, wrapped.AllNodesCalls(),
		"graphify edge MATCH leaked %d AllNodes() calls", wrapped.AllNodesCalls())
}

func TestMatchPatternProperty_BoundRelationshipDeleteUsesAdjacency(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 500)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		_, err := exec.Execute(ctx,
			"CREATE (:Repository {id:$id})",
			map[string]interface{}{"id": fmt.Sprintf("repository:unrelated-%03d", i)})
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	wrapped.reset()
	deleteResult, err := exec.Execute(ctx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPENDS_ON|DEPLOYS_FROM|USES_MODULE]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
DELETE rel
`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, deleteResult.Stats.RelationshipsDeleted)
	require.LessOrEqual(t, wrapped.OutgoingEdgeCalls(), int64(1),
		"bound relationship DELETE should expand only the bound source node, got %d outgoing probes",
		wrapped.OutgoingEdgeCalls())

	verify, err := exec.Execute(ctx, `
MATCH ()-[rel:DEPLOYS_FROM]->()
RETURN count(rel) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(0), verify.Rows[0][0])
}

func TestMatchPatternProperty_BoundRelationshipDeleteSegmentEligibility(t *testing.T) {
	exec, _ := newCountingExecutor(t)
	simple := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
`
	require.True(t, isSimpleBoundRelationshipDeleteMatchSegment(simple))

	limited, limitN, ok := parseBoundRelationshipDeleteWithLimitSegment(simple+`
WITH rel LIMIT 1`, "rel")
	require.True(t, ok)
	require.Equal(t, 1, limitN)
	require.Equal(t, strings.TrimSpace(simple), limited)

	pseudo := strings.TrimSpace(simple) + " RETURN __delete_probe__"
	returnIdx := findKeywordIndex(pseudo, "RETURN")
	whereIdx := lastKeywordIndexBefore(pseudo, "WHERE", returnIdx)
	clauses := splitMatchClauses(pseudo, whereIdx, returnIdx)
	require.Len(t, clauses, 2)
	sourcePattern := exec.parseNodePattern(context.Background(), clauses[0])
	require.Equal(t, "source_repo", sourcePattern.variable)
	relMatch := exec.parseTraversalPattern(context.Background(), clauses[1])
	require.NotNil(t, relMatch)
	require.Equal(t, "source_repo", relMatch.StartNode.variable)
	require.Equal(t, "rel", relMatch.Relationship.Variable)
	require.Equal(t, []string{"DEPLOYS_FROM"}, relMatch.Relationship.Types)
	require.Equal(t, "outgoing", relMatch.Relationship.Direction)
}

func TestMatchPatternProperty_TryBoundRelationshipDeleteDeletesSimpleMatch(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	cypher := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
DELETE rel
`
	result, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
`, cypher, "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)
}

func TestMatchPatternProperty_BoundRelationshipDeleteTransactionSourceUsesIndex(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		_, err = exec.Execute(ctx,
			"CREATE (:Repository {id:$id})",
			map[string]interface{}{"id": fmt.Sprintf("repository:unrelated-%03d", i)})
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)

	wrapped.reset()
	sourcePattern := nodePatternInfo{
		variable: "source_repo",
		labels:   []string{"Repository"},
		properties: map[string]interface{}{
			"id": "repository:source",
		},
	}
	sources, err := exec.collectBoundRelationshipDeleteSources(ctx, sourcePattern, true)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.Equal(t, "repository:source", sources[0].Properties["id"])
	require.Zerof(t, wrapped.GetNodesByLabelCalls(),
		"transaction-mode bound relationship DELETE source lookup leaked %d GetNodesByLabel calls",
		wrapped.GetNodesByLabelCalls())
}

func TestMatchPatternProperty_BoundRelationshipDeleteTransactionSourceKeepsPendingNodes(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)

	inner := exec.storage.(*storage.NamespacedEngine).GetInnerEngine().(*scanCountingEngine)
	require.NoError(t, inner.EnsureNamespaceMVCC("scan"))
	tx, err := inner.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("scan"))

	txWrapper := &transactionStorageWrapper{tx: tx, underlying: exec.storage, namespace: "scan", separator: ":"}
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)
	txExec := exec.cloneWithStorage(txWrapper)
	require.NoError(t, txWrapper.BulkCreateNodes([]*storage.Node{{
		ID:     "pending-source-node",
		Labels: []string{"Repository"},
		Properties: map[string]interface{}{
			"id": "repository:pending-source",
		},
	}}))

	sourcePattern := nodePatternInfo{
		variable: "source_repo",
		labels:   []string{"Repository"},
		properties: map[string]interface{}{
			"id": "repository:pending-source",
		},
	}
	sources, err := txExec.collectBoundRelationshipDeleteSources(txCtx, sourcePattern, true)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.Equal(t, "repository:pending-source", sources[0].Properties["id"])
}

func BenchmarkMatchPatternProperty_BoundRelationshipDeleteTransactionSourceLookup(b *testing.B) {
	for _, repoCount := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("repositories=%d", repoCount), func(b *testing.B) {
			exec, _ := newCountingExecutor(b)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
			require.NoError(b, err)
			for i := 0; i < repoCount; i++ {
				_, err = exec.Execute(ctx,
					"CREATE (:Repository {id:$id})",
					map[string]interface{}{"id": fmt.Sprintf("repository:unrelated-%05d", i)})
				require.NoError(b, err)
			}
			_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
			require.NoError(b, err)

			sourcePattern := nodePatternInfo{
				variable: "source_repo",
				labels:   []string{"Repository"},
				properties: map[string]interface{}{
					"id": "repository:source",
				},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sources, err := exec.collectBoundRelationshipDeleteSources(ctx, sourcePattern, true)
				if err != nil {
					b.Fatal(err)
				}
				if len(sources) != 1 {
					b.Fatalf("expected one source, got %d", len(sources))
				}
			}
		})
	}
}

func TestMatchPatternProperty_TryBoundRelationshipDeleteDeletesUnionAndLimitedMatches(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = exec.Execute(ctx,
			"CREATE (:Repository {id:$id})",
			map[string]interface{}{"id": fmt.Sprintf("repository:target-%d", i)})
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:$id})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, map[string]interface{}{"id": fmt.Sprintf("repository:target-%d", i)})
		require.NoError(t, err)
	}

	result, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPENDS_ON|DEPLOYS_FROM|USES_MODULE]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
WITH rel LIMIT 1
`, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)
}

func TestMatchPatternProperty_ExecuteDeleteUsesBoundRelationshipDelete(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	result, err := exec.executeDelete(ctx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
DELETE rel
`)
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)
}

func TestMatchPatternProperty_BoundRelationshipDeleteRoutingPredicates(t *testing.T) {
	cypher := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
DELETE rel
`
	trimmed := strings.TrimSpace(cypher)
	require.Equal(t, 0, findKeywordIndex(trimmed, "MATCH"))
	require.Equal(t, -1, findKeywordIndex(trimmed, "CREATE"))
	require.Greater(t, findKeywordIndex(trimmed, "DELETE"), 0)
	require.True(t, findKeywordIndex(trimmed, "DELETE") > 0)
}

func TestMatchPatternProperty_BoundRelationshipDeleteHonorsWithLimit(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = exec.Execute(ctx,
			"CREATE (:Repository {id:$id})",
			map[string]interface{}{"id": fmt.Sprintf("repository:target-%d", i)})
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:$id})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, map[string]interface{}{"id": fmt.Sprintf("repository:target-%d", i)})
		require.NoError(t, err)
	}

	deleteResult, err := exec.Execute(ctx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WITH rel LIMIT 1
DELETE rel
`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, deleteResult.Stats.RelationshipsDeleted)

	verify, err := exec.Execute(ctx, `
MATCH ()-[rel:DEPLOYS_FROM]->()
RETURN count(rel) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(1), verify.Rows[0][0])
}

func TestMatchPatternProperty_BoundRelationshipDeleteUsesTransactionSnapshot(t *testing.T) {
	exec, _ := newCountingExecutor(t)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	inner := exec.storage.(*storage.NamespacedEngine).GetInnerEngine().(*scanCountingEngine)
	require.NoError(t, inner.EnsureNamespaceMVCC("scan"))
	tx, err := inner.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("scan"))

	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:late-target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:late-target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	txWrapper := &transactionStorageWrapper{tx: tx, underlying: exec.storage, namespace: "scan", separator: ":"}
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)
	txExec := exec.cloneWithStorage(txWrapper)

	result, ok, err := txExec.tryExecuteBoundRelationshipDelete(txCtx, `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPLOYS_FROM]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
`, "", "rel", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)

	require.NoError(t, tx.Commit())

	verify, err := exec.Execute(ctx, `
MATCH ()-[rel:DEPLOYS_FROM]->()
RETURN count(rel) AS c
`, nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.Equal(t, int64(1), verify.Rows[0][0])
}

func TestMatchPatternProperty_BoundRelationshipDeleteReturnsCandidateLookupErrors(t *testing.T) {
	tests := []struct {
		name      string
		direction string
		configure func(*scanCountingEngine, error)
	}{
		{
			name:      "outgoing",
			direction: "outgoing",
			configure: func(wrapped *scanCountingEngine, lookupErr error) {
				wrapped.outgoingEdgeErr = lookupErr
			},
		},
		{
			name:      "incoming",
			direction: "incoming",
			configure: func(wrapped *scanCountingEngine, lookupErr error) {
				wrapped.incomingEdgeErr = lookupErr
			},
		},
		{
			name:      "both outgoing",
			direction: "both",
			configure: func(wrapped *scanCountingEngine, lookupErr error) {
				wrapped.outgoingEdgeErr = lookupErr
			},
		},
		{
			name:      "both incoming",
			direction: "both",
			configure: func(wrapped *scanCountingEngine, lookupErr error) {
				wrapped.incomingEdgeErr = lookupErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, wrapped := newCountingExecutor(t)
			lookupErr := errors.New(tt.name + " edge lookup failed")
			tt.configure(wrapped, lookupErr)

			_, err := exec.boundRelationshipDeleteCandidateEdges(wrapped, "source", tt.direction)
			require.ErrorIs(t, err, lookupErr)
		})
	}
}

func TestMatchPatternProperty_TryBoundRelationshipDeletePropagatesCandidateLookupError(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)
	seedScanTestPopulation(t, exec, 10)

	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX repo_id IF NOT EXISTS FOR (r:Repository) ON (r.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:source'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Repository {id:'repository:target'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Repository {id:'repository:source'})
MATCH (target:Repository {id:'repository:target'})
CREATE (source)-[:DEPLOYS_FROM {evidence_source:'resolver/cross-repo'}]->(target)
`, nil)
	require.NoError(t, err)

	lookupErr := errors.New("outgoing edge lookup failed")
	wrapped.outgoingEdgeErr = lookupErr
	cypher := `
MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPENDS_ON|DEPLOYS_FROM|USES_MODULE]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'
DELETE rel
`
	result, ok, err := exec.tryExecuteBoundRelationshipDelete(ctx,
		`MATCH (source_repo:Repository {id:'repository:source'})
MATCH (source_repo)-[rel:DEPENDS_ON|DEPLOYS_FROM|USES_MODULE]->(:Repository)
WHERE rel.evidence_source = 'resolver/cross-repo'`,
		cypher,
		"rel",
		false)
	require.ErrorIs(t, err, lookupErr)
	require.True(t, ok)
	require.Nil(t, result)
}

func TestMatchPatternProperty_BoundRelationshipDeleteReturnsTargetLookupError(t *testing.T) {
	exec, wrapped := newCountingExecutor(t)

	lookupErr := errors.New("target lookup failed")
	wrapped.getNodeErr = lookupErr
	_, ok, err := exec.boundRelationshipDeleteTargetNode(wrapped, "source", &storage.Edge{
		StartNode: "source",
		EndNode:   "target",
	}, "outgoing")
	require.ErrorIs(t, err, lookupErr)
	require.False(t, ok)
}
