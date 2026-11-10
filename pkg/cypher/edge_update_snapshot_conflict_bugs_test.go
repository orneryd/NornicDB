// Unit tests for the edge-update snapshot conflict classification bug.
//
// BUG: inside an explicit transaction, MERGE resolves an existing
// relationship through a latest-committed lookup (GetEdgeBetween), but the
// subsequent SET-driven store.UpdateEdge resolves the same edge through the
// BEGIN-time snapshot. When a peer transaction MERGEd the same relationship
// with different property values and committed after this transaction began,
// the edge was found but not updatable, and the statement failed hard with
// "not found" — surfaced over Bolt as a non-retryable
// Neo.ClientError.Statement.SyntaxError. Neo4j succeeds on this exact
// interleaving (MERGE blocks on the relationship lock, re-reads, and
// applies the SET), and its only sanctioned conflict failure is a transient
// error that drivers auto-retry. The first fix classified the
// snapshot-invisible but live edge as storage.ErrConflict, which maps to
// Neo.TransientError.Transaction.Outdated so managed transactions
// (session.ExecuteWrite) retry on a fresh snapshot and converge. A write
// that reaches storage without the relationship's write lock still fails
// that way; a Cypher SET now takes the lock and succeeds as in Neo4j
// (NOR-213).
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// edgeConflictTestEngine builds a namespaced engine over the shared
// Badger-backed in-memory store, matching the production topology where
// every database is a namespace over one storage engine.
func edgeConflictTestEngine(t testing.TB) storage.Engine {
	base := newTestMemoryEngine(t)
	return storage.NewNamespacedEngine(base, "neo4j")
}

// unwindMergeRelCypher is the batched UNWIND-MERGE-chain statement shape:
// two lookup MATCHes on different labels, a MERGE of the relationship, and
// SET assignments whose values differ per caller.
const unwindMergeRelCypher = `UNWIND $rows AS row
MATCH (a:A {key: row.aKey})
MATCH (b:B {key: row.bKey})
MERGE (a)-[rel:REL]->(b)
SET rel.x = row.x,
    rel.y = row.y
RETURN count(rel) AS written`

func unwindMergeRelRows(x, y string) map[string]interface{} {
	return map[string]interface{}{
		"rows": []interface{}{
			map[string]interface{}{
				"aKey": "a1",
				"bKey": "b1",
				"x":    x,
				"y":    y,
			},
		},
	}
}

func seedRelEndpoints(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (:A {key: 'a1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
}

// requireSingleEdgeWithX asserts exactly one :REL edge exists between the
// endpoints and that it carries the expected x value.
func requireSingleEdgeWithX(t *testing.T, exec *StorageExecutor, wantX string) {
	t.Helper()
	res, err := exec.Execute(context.Background(),
		`MATCH (:A {key: 'a1'})-[rel:REL]->(:B {key: 'b1'})
		 RETURN count(rel) AS c, collect(rel.x) AS xs`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0], "exactly one REL edge must exist, got xs=%v", res.Rows[0][1])
	require.ElementsMatch(t, []interface{}{wantX}, res.Rows[0][1])
}

// peerCommittedEdgeSchedule is the one owner of the peer-commit schedule the
// three tests below share:
//
//  1. the endpoints are seeded and the writer's explicit transaction begins,
//     pinning its snapshot at BEGIN;
//  2. a peer autocommit statement runs the same relationship statement with
//     peerParams and commits after that begin;
//  3. the writer runs the identical statement with writerParams: the
//     relationship lookup (latest-committed read) finds the peer's edge, whose
//     store.UpdateEdge target is invisible at the writer's snapshot. Since
//     NOR-213 the SET takes the relationship's write lock and is applied to
//     the peer's commit, so the statement and its commit succeed as in Neo4j.
//
// writerMessage describes the writer statement's failure. The returned
// executor is positioned after the writer's commit, so the caller owns the
// outcome assertion for its own SET values.
func peerCommittedEdgeSchedule(t *testing.T, statement string, peerParams, writerParams map[string]interface{}, writerMessage string) *StorageExecutor {
	t.Helper()
	engine := edgeConflictTestEngine(t)
	writerExec := NewStorageExecutor(engine)
	peerExec := NewStorageExecutor(engine)
	ctx := context.Background()

	seedRelEndpoints(t, writerExec)

	_, err := writerExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	committed := false
	defer func() {
		if !committed {
			_, _ = writerExec.Execute(ctx, "ROLLBACK", nil)
		}
	}()

	// Peer commits the same relationship after the writer's begin.
	_, err = peerExec.Execute(ctx, statement, peerParams)
	require.NoError(t, err)

	_, err = writerExec.Execute(ctx, statement, writerParams)
	require.NoError(t, err, writerMessage)
	_, err = writerExec.Execute(ctx, "COMMIT", nil)
	committed = true
	require.NoError(t, err)

	return writerExec
}

// TestBug_UnwindMergeChainPeerCommittedEdgeUpdateAppliesOnPeerCommit is the
// batched reproduction: the writer runs the UNWIND MERGE chain while a peer
// has already committed the same relationship with DIFFERENT property values.
// Before, the statement failed hard with "not found" and was then classified
// as a retryable storage.ErrConflict. Since NOR-213 the SET locks the
// relationship and applies its values on top of the peer's commit, so the
// statement and its commit succeed with exactly one edge carrying the
// writer's values: Neo4j's observable outcome for this interleaving.
func TestBug_UnwindMergeChainPeerCommittedEdgeUpdateAppliesOnPeerCommit(t *testing.T) {
	writerExec := peerCommittedEdgeSchedule(t,
		unwindMergeRelCypher,
		unwindMergeRelRows("x-peer", "y-peer"),
		unwindMergeRelRows("x-writer", "y-writer"),
		"the SET is applied to the peer's committed relationship")

	requireSingleEdgeWithX(t, writerExec, "x-writer")
}

// TestBug_PlainMergeSetPeerCommittedEdgeUpdateAppliesOnPeerCommit is the
// non-batched variant: a plain MERGE ... SET on a single relationship hits
// the same latest-committed-lookup / snapshot-update divergence.
func TestBug_PlainMergeSetPeerCommittedEdgeUpdateAppliesOnPeerCommit(t *testing.T) {
	const plainMergeSet = `MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:REL]->(b)
SET rel.x = $x`

	writerExec := peerCommittedEdgeSchedule(t,
		plainMergeSet,
		map[string]interface{}{"x": "x-peer"},
		map[string]interface{}{"x": "x-writer"},
		"the SET is applied to the peer's committed relationship")

	requireSingleEdgeWithX(t, writerExec, "x-writer")
}

// TestBug_UnwindMergeChainPeerCommitIdenticalSetValuesIsSilentNoOp is the
// success-path control: when the racing writers stamp IDENTICAL property
// values, the assignment diff reports no change, UpdateEdge is never called,
// and the statement succeeds. The conflict reclassification must not change
// this path.
func TestBug_UnwindMergeChainPeerCommitIdenticalSetValuesIsSilentNoOp(t *testing.T) {
	writerExec := peerCommittedEdgeSchedule(t,
		unwindMergeRelCypher,
		unwindMergeRelRows("x-same", "y-same"),
		unwindMergeRelRows("x-same", "y-same"),
		"identical SET values must not attempt an update")

	requireSingleEdgeWithX(t, writerExec, "x-same")
}
