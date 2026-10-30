// End-to-end scenario tests for the Graphify Cypher surface.
//
// Graphify emits a small, parameterised Cypher API (export.push_to_neo4j and
// export.push_to_falkordb): per-node MERGE on (label, id) with `SET n += $props`,
// per-edge MATCH on labelless `{id: ...}` patterns followed by MERGE on the
// relationship type with `SET r += $props`, plus the trailing read-side counts
// the graphify FalkorDB integration test runs to verify the push.
//
// This file ingests a realistic synthetic graph through the EXACT shapes
// graphify writes, idempotently re-runs every statement (graphify's documented
// "MERGE so re-running is safe" contract), and asserts:
//
//  1. Final node and edge counts match the source graph
//  2. Per-label and per-rel-type counts match the source graph
//  3. Each node carries the props graphify writes (id, label, file_type,
//     source_file, community)
//  4. Each edge carries the {confidence, relation} props graphify writes
//  5. Indexed MERGE goes through the schema-lookup fast path, not the scan
//     fallback — covering the "fast path probe verified" contract the user
//     called out
//  6. Re-running every per-node and per-edge statement on the same payload
//     leaves node/edge counts unchanged (true idempotency)
//
// Inventory sources for the query shapes asserted below:
//   - graphify/graphify/export.py: push_to_neo4j, push_to_falkordb
//   - graphify/tests/test_falkordb_integration.py: post-condition counts

package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// --- per-statement shapes graphify emits ------------------------------------

const graphifyNodeUpsertQuery = `MERGE (n:%s {id: $id}) SET n += $props`

const graphifyEdgeUpsertQuery = `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:%s]->(b) SET r += $props`

// Two read-side shapes used by graphify's FalkorDB integration test to
// post-verify a push.
const graphifyAllNodeCountQuery = `MATCH (n) RETURN count(n)`
const graphifyAllEdgeCountQuery = `MATCH ()-[r]->() RETURN count(r)`

// graphifyScenarioNode is the per-node payload graphify produces. The
// `props` map shape follows export.py's filter: id, label, primitive attrs
// (str|int|float|bool), and optionally community (int).
type graphifyScenarioNode struct {
	label string // sanitised :FType (Code / Document / Entity / ...)
	id    string
	props map[string]interface{}
}

type graphifyScenarioEdge struct {
	relType string // sanitised UPPER_SNAKE relation type
	src     string
	tgt     string
	props   map[string]interface{}
}

type graphifyScenarioPayload struct {
	nodes []graphifyScenarioNode
	edges []graphifyScenarioEdge
}

// buildGraphifyScenarioPayload constructs a synthetic graph shaped the way
// graphify produces them after a real `/graphify .` run: a population of Code
// nodes per file with intra-file CALLS edges plus inter-file IMPORTS edges,
// a smaller Document population referenced from code via REFERENCES edges,
// and a "fallback" Entity / RELATES_TO clique for the to_cypher fallback path.
func buildGraphifyScenarioPayload(codeCount, docCount, entityCount int) graphifyScenarioPayload {
	nodes := make([]graphifyScenarioNode, 0, codeCount+docCount+entityCount)
	edges := make([]graphifyScenarioEdge, 0, codeCount*2+docCount+entityCount)

	for i := 0; i < codeCount; i++ {
		id := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i, i)
		nodes = append(nodes, graphifyScenarioNode{
			label: "Code",
			id:    id,
			props: map[string]interface{}{
				"id":          id,
				"label":       fmt.Sprintf("Sym_%03d", i),
				"file_type":   "code",
				"source_file": fmt.Sprintf("pkg/mod_%03d.py", i),
				"community":   int64(i % 5),
			},
		})
	}
	for i := 0; i < docCount; i++ {
		id := fmt.Sprintf("docs/section_%03d.md::Heading_%03d", i, i)
		nodes = append(nodes, graphifyScenarioNode{
			label: "Document",
			id:    id,
			props: map[string]interface{}{
				"id":          id,
				"label":       fmt.Sprintf("Heading_%03d", i),
				"file_type":   "document",
				"source_file": fmt.Sprintf("docs/section_%03d.md", i),
				"community":   int64(i % 3),
			},
		})
	}
	for i := 0; i < entityCount; i++ {
		id := fmt.Sprintf("entity::%03d", i)
		nodes = append(nodes, graphifyScenarioNode{
			label: "Entity", // graphify's `_safe_label` fallback
			id:    id,
			props: map[string]interface{}{
				"id":        id,
				"label":     fmt.Sprintf("Thing_%03d", i),
				"community": int64(i % 2),
			},
		})
	}

	// CALLS: chain neighbouring Code nodes (extracted)
	for i := 0; i+1 < codeCount; i++ {
		src := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i, i)
		tgt := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i+1, i+1)
		edges = append(edges, graphifyScenarioEdge{
			relType: "CALLS",
			src:     src, tgt: tgt,
			props: map[string]interface{}{"relation": "calls", "confidence": "EXTRACTED"},
		})
	}
	// IMPORTS: every 3rd Code -> Code+3 (extracted)
	for i := 0; i+3 < codeCount; i += 3 {
		src := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i, i)
		tgt := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i+3, i+3)
		edges = append(edges, graphifyScenarioEdge{
			relType: "IMPORTS",
			src:     src, tgt: tgt,
			props: map[string]interface{}{"relation": "imports", "confidence": "EXTRACTED"},
		})
	}
	// REFERENCES: Code -> Document (inferred)
	for i := 0; i < docCount && i < codeCount; i++ {
		src := fmt.Sprintf("pkg/mod_%03d.py::Sym_%03d", i, i)
		tgt := fmt.Sprintf("docs/section_%03d.md::Heading_%03d", i, i)
		edges = append(edges, graphifyScenarioEdge{
			relType: "REFERENCES",
			src:     src, tgt: tgt,
			props: map[string]interface{}{"relation": "references", "confidence": "INFERRED"},
		})
	}
	// RELATES_TO: Entity -> any other entity (ambiguous fallback)
	for i := 0; i+1 < entityCount; i++ {
		edges = append(edges, graphifyScenarioEdge{
			relType: "RELATES_TO",
			src:     fmt.Sprintf("entity::%03d", i),
			tgt:     fmt.Sprintf("entity::%03d", i+1),
			props:   map[string]interface{}{"relation": "relates_to", "confidence": "AMBIGUOUS"},
		})
	}
	return graphifyScenarioPayload{nodes: nodes, edges: edges}
}

// graphifyCountingEngine wraps a storage.Engine to count node and edge writes,
// mirroring the graphitiCopyCountingEngine. Used to assert that the second
// idempotent re-run does not produce duplicate writes.
type graphifyCountingEngine struct {
	storage.Engine
	nodeCreates int64
	nodeUpdates int64
	edgeCreates int64
	edgeUpdates int64
}

func (e *graphifyCountingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	atomic.AddInt64(&e.nodeCreates, 1)
	return e.Engine.CreateNode(node)
}
func (e *graphifyCountingEngine) UpdateNode(node *storage.Node) error {
	atomic.AddInt64(&e.nodeUpdates, 1)
	return e.Engine.UpdateNode(node)
}
func (e *graphifyCountingEngine) CreateEdge(edge *storage.Edge) error {
	atomic.AddInt64(&e.edgeCreates, 1)
	return e.Engine.CreateEdge(edge)
}
func (e *graphifyCountingEngine) UpdateEdge(edge *storage.Edge) error {
	atomic.AddInt64(&e.edgeUpdates, 1)
	return e.Engine.UpdateEdge(edge)
}

func (e *graphifyCountingEngine) NodeCreateCount() int64 { return atomic.LoadInt64(&e.nodeCreates) }
func (e *graphifyCountingEngine) NodeUpdateCount() int64 { return atomic.LoadInt64(&e.nodeUpdates) }
func (e *graphifyCountingEngine) EdgeCreateCount() int64 { return atomic.LoadInt64(&e.edgeCreates) }
func (e *graphifyCountingEngine) EdgeUpdateCount() int64 { return atomic.LoadInt64(&e.edgeUpdates) }

// pushGraphifyPayload executes every per-node and per-edge statement graphify
// emits during a push_to_neo4j / push_to_falkordb run. It mirrors the
// per-statement loop in export.py exactly: one MERGE+SET per node, one
// MATCH+MERGE+SET per edge.
func pushGraphifyPayload(t testing.TB, exec *StorageExecutor, ctx context.Context, p graphifyScenarioPayload) (nodes, edges int) {
	for _, n := range p.nodes {
		q := fmt.Sprintf(graphifyNodeUpsertQuery, n.label)
		_, err := exec.Execute(ctx, q, map[string]interface{}{"id": n.id, "props": n.props})
		require.NoErrorf(t, err, "node upsert %s/%s", n.label, n.id)
	}
	for _, e := range p.edges {
		q := fmt.Sprintf(graphifyEdgeUpsertQuery, e.relType)
		_, err := exec.Execute(ctx, q, map[string]interface{}{"src": e.src, "tgt": e.tgt, "props": e.props})
		require.NoErrorf(t, err, "edge upsert %s:[%s->%s]", e.relType, e.src, e.tgt)
	}
	return len(p.nodes), len(p.edges)
}

// TestGraphifyScenarioE2E_FullPushAndVerify mirrors a full
// `graphify export neo4j` / `push_to_neo4j` round-trip: build a realistic
// payload, push every per-node and per-edge statement graphify emits, then
// verify counts and idempotency through the same read shapes graphify's own
// integration tests use.
func TestGraphifyScenarioE2E_FullPushAndVerify(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	// Graphify users with sizeable graphs need an :Entity(id) / :Code(id) /
	// :Document(id) lookup. Without those, every MERGE falls back to a label
	// scan. We seed the indexes graphify operators are expected to add, then
	// assert below that the MERGE statements actually drive the schema-lookup
	// fast path.
	for _, q := range []string{
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
		"CREATE INDEX document_id IF NOT EXISTS FOR (n:Document) ON (n.id)",
		"CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoErrorf(t, err, "scenario index: %s", q)
	}

	codeCount, docCount, entityCount := 60, 20, 10
	if testing.Short() {
		codeCount, docCount, entityCount = 12, 6, 4
	}
	payload := buildGraphifyScenarioPayload(codeCount, docCount, entityCount)

	// --- push ----------------------------------------------------------------
	expectedNodes, expectedEdges := pushGraphifyPayload(t, exec, ctx, payload)
	require.Equal(t, codeCount+docCount+entityCount, expectedNodes)

	// --- post-condition counts (the shapes graphify's FalkorDB test uses) ----
	require.Equal(t, int64(expectedNodes), mustCountRows(t, exec, ctx, graphifyAllNodeCountQuery, nil))
	require.Equal(t, int64(expectedEdges), mustCountRows(t, exec, ctx, graphifyAllEdgeCountQuery, nil))

	// per-label nodes
	require.Equal(t, int64(codeCount), mustCountRows(t, exec, ctx, "MATCH (n:Code) RETURN count(n)", nil))
	require.Equal(t, int64(docCount), mustCountRows(t, exec, ctx, "MATCH (n:Document) RETURN count(n)", nil))
	require.Equal(t, int64(entityCount), mustCountRows(t, exec, ctx, "MATCH (n:Entity) RETURN count(n)", nil))

	// per-rel-type edges
	require.Equal(t, int64(codeCount-1), mustCountRows(t, exec, ctx, "MATCH ()-[r:CALLS]->() RETURN count(r)", nil))
	expectedImports := 0
	for i := 0; i+3 < codeCount; i += 3 {
		expectedImports++
	}
	require.Equal(t, int64(expectedImports), mustCountRows(t, exec, ctx, "MATCH ()-[r:IMPORTS]->() RETURN count(r)", nil))
	expectedRefs := docCount
	if codeCount < docCount {
		expectedRefs = codeCount
	}
	require.Equal(t, int64(expectedRefs), mustCountRows(t, exec, ctx, "MATCH ()-[r:REFERENCES]->() RETURN count(r)", nil))
	require.Equal(t, int64(entityCount-1), mustCountRows(t, exec, ctx, "MATCH ()-[r:RELATES_TO]->() RETURN count(r)", nil))

	// --- property fidelity: a sample node and edge --------------------------
	row := mustOneRow(t, exec, ctx,
		"MATCH (n:Code {id:'pkg/mod_000.py::Sym_000'}) RETURN n.label, n.file_type, n.source_file, n.community", nil)
	require.Equal(t, "Sym_000", row[0])
	require.Equal(t, "code", row[1])
	require.Equal(t, "pkg/mod_000.py", row[2])
	require.Equal(t, int64(0), mustInt64(t, row[3]))

	row = mustOneRow(t, exec, ctx,
		"MATCH ({id:'pkg/mod_000.py::Sym_000'})-[r:CALLS]->({id:'pkg/mod_001.py::Sym_001'}) RETURN r.confidence, r.relation", nil)
	require.Equal(t, "EXTRACTED", row[0])
	require.Equal(t, "calls", row[1])

	// --- idempotency: re-run the entire push -------------------------------
	pushGraphifyPayload(t, exec, ctx, payload)
	require.Equal(t, int64(expectedNodes), mustCountRows(t, exec, ctx, graphifyAllNodeCountQuery, nil),
		"second push must NOT duplicate any nodes (graphify MERGE idempotency)")
	require.Equal(t, int64(expectedEdges), mustCountRows(t, exec, ctx, graphifyAllEdgeCountQuery, nil),
		"second push must NOT duplicate any edges (graphify MERGE idempotency)")
}

// TestGraphifyScenarioE2E_NodeUpsertFastPath asserts the node MERGE shape
// graphify emits drives the MergeSchemaLookup fast path (and NOT the scan
// fallback) when the recommended (Label, id) index is present. This is the
// "fast path probe verified" piece — a regression here means graphify
// operators are silently paying for full label scans per node.
func TestGraphifyScenarioE2E_NodeUpsertFastPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)", nil)
	require.NoError(t, err)

	props := map[string]interface{}{"id": "fast::1", "label": "Foo", "file_type": "code"}
	_, err = exec.Execute(ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "fast::1", "props": props,
	})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.True(t, trace.MergeSchemaLookupUsed, "indexed graphify node MERGE must hit MergeSchemaLookup")
	require.False(t, trace.MergeScanFallbackUsed, "indexed graphify node MERGE must NOT fall back to scan")

	// Second invocation also must drive the fast path AND must NOT create a
	// duplicate node (covered separately by the full scenario, repeated here
	// to keep the fast-path assertion focused).
	_, err = exec.Execute(ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "fast::1", "props": props,
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().MergeSchemaLookupUsed)
	require.Equal(t, int64(1), mustCountRows(t, exec, ctx, "MATCH (n:Code {id:'fast::1'}) RETURN count(n)", nil))
}

// TestGraphifyScenarioE2E_NodeUpsertScanFallbackWhenUnindexed pins the
// negative case: if the operator forgot the recommended (Label, id) index,
// graphify's MERGE statement still produces correct results but falls back
// to a scan. This guards against silent fast-path regressions hiding behind
// "the test passed".
func TestGraphifyScenarioE2E_NodeUpsertScanFallbackWhenUnindexed(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()
	// no index created on (Code, id)

	props := map[string]interface{}{"id": "slow::1", "label": "Bar", "file_type": "code"}
	_, err := exec.Execute(ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "slow::1", "props": props,
	})
	require.NoError(t, err)
	trace := exec.LastHotPathTrace()
	require.False(t, trace.MergeSchemaLookupUsed, "unindexed MERGE must not falsely report schema-lookup fast path")
	require.True(t, trace.MergeScanFallbackUsed, "unindexed MERGE must record the scan fallback")
}

// TestGraphifyScenarioE2E_EdgeUpsertOnExistingEndpoints exercises the edge
// upsert shape under the realistic graphify ordering (all nodes pushed first,
// then all edges). Verifies counts, idempotency, and that the labelless
// `{id: $src}` / `{id: $tgt}` MATCH binds to the correct nodes regardless of
// the node label.
func TestGraphifyScenarioE2E_EdgeUpsertOnExistingEndpoints(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
		"CREATE INDEX document_id IF NOT EXISTS FOR (n:Document) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	// push two Code nodes and one Document via graphify's node shape
	for _, args := range []struct {
		label, id, lbl string
		fileType       string
	}{
		{"Code", "src::1", "Src1", "code"},
		{"Code", "tgt::1", "Tgt1", "code"},
		{"Document", "doc::1", "Doc1", "document"},
	} {
		_, err := exec.Execute(ctx, fmt.Sprintf(graphifyNodeUpsertQuery, args.label), map[string]interface{}{
			"id": args.id,
			"props": map[string]interface{}{
				"id": args.id, "label": args.lbl, "file_type": args.fileType,
			},
		})
		require.NoError(t, err)
	}

	// 3 separate edges with distinct types
	for _, args := range []struct {
		rel, src, tgt, conf string
	}{
		{"CALLS", "src::1", "tgt::1", "EXTRACTED"},
		{"REFERENCES", "src::1", "doc::1", "INFERRED"},
		{"RELATES_TO", "tgt::1", "doc::1", "AMBIGUOUS"},
	} {
		_, err := exec.Execute(ctx, fmt.Sprintf(graphifyEdgeUpsertQuery, args.rel), map[string]interface{}{
			"src": args.src, "tgt": args.tgt,
			"props": map[string]interface{}{"confidence": args.conf},
		})
		require.NoError(t, err)
	}

	require.Equal(t, int64(3), mustCountRows(t, exec, ctx, graphifyAllEdgeCountQuery, nil))
	require.Equal(t, int64(1), mustCountRows(t, exec, ctx, "MATCH (a:Code {id:'src::1'})-[e:CALLS]->(b:Code {id:'tgt::1'}) RETURN count(e)", nil))
	require.Equal(t, int64(1), mustCountRows(t, exec, ctx, "MATCH (a:Code {id:'src::1'})-[e:REFERENCES]->(b:Document {id:'doc::1'}) RETURN count(e)", nil))
	require.Equal(t, int64(1), mustCountRows(t, exec, ctx, "MATCH (a:Code {id:'tgt::1'})-[e:RELATES_TO]->(b:Document {id:'doc::1'}) RETURN count(e)", nil))

	// re-run all three edge upserts — must remain at 3 (graphify idempotency)
	for _, args := range []struct {
		rel, src, tgt, conf string
	}{
		{"CALLS", "src::1", "tgt::1", "EXTRACTED"},
		{"REFERENCES", "src::1", "doc::1", "INFERRED"},
		{"RELATES_TO", "tgt::1", "doc::1", "AMBIGUOUS"},
	} {
		_, err := exec.Execute(ctx, fmt.Sprintf(graphifyEdgeUpsertQuery, args.rel), map[string]interface{}{
			"src": args.src, "tgt": args.tgt,
			"props": map[string]interface{}{"confidence": args.conf},
		})
		require.NoError(t, err)
	}
	require.Equal(t, int64(3), mustCountRows(t, exec, ctx, graphifyAllEdgeCountQuery, nil))
}

// TestGraphifyScenarioE2E_ReRunDoesNotProduceWritesForEqualPayload is the
// strict-idempotency contract: pushing the EXACT same payload twice must not
// produce extra storage-level writes beyond the second SET-equal-to-current
// pass. We can't legally assert zero updates (graphify's SET n += $props
// touches every property, and `n += {...}` is an upsert at the storage layer)
// but we CAN assert that the second pass doesn't produce extra CreateNode /
// CreateEdge calls — i.e. graphify never duplicates entities.
func TestGraphifyScenarioE2E_ReRunDoesNotProduceWritesForEqualPayload(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &graphifyCountingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
		"CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	payload := buildGraphifyScenarioPayload(8, 0, 0)

	pushGraphifyPayload(t, exec, ctx, payload)
	nc1 := counting.NodeCreateCount()
	ec1 := counting.EdgeCreateCount()
	require.Equal(t, int64(len(payload.nodes)), nc1, "first push must create exactly one node per payload node")
	require.Equal(t, int64(len(payload.edges)), ec1, "first push must create exactly one edge per payload edge")

	pushGraphifyPayload(t, exec, ctx, payload)
	require.Equal(t, nc1, counting.NodeCreateCount(), "second push of identical payload must not create new nodes")
	require.Equal(t, ec1, counting.EdgeCreateCount(), "second push of identical payload must not create new edges")
}

// mustOneRow is the project-wide-friendly assertion that a query returns
// exactly one row. Keeps property-fidelity assertions in the scenario test
// concise.
func mustOneRow(t testing.TB, exec *StorageExecutor, ctx context.Context, query string, params map[string]interface{}) []interface{} {
	res, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "expected exactly one row for: %s", query)
	return res.Rows[0]
}
