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
// error that drivers auto-retry. The fix classifies the snapshot-invisible
// but live edge as storage.ErrConflict, which maps to
// Neo.TransientError.Transaction.Outdated so managed transactions
// (session.ExecuteWrite) retry on a fresh snapshot and converge.
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

// TestBug_UnwindMergeChainPeerCommittedEdgeUpdateIsRetryableConflict is the
// batched reproduction:
//
//  1. T1 opens an explicit transaction (snapshot pinned at BEGIN).
//  2. A peer autocommit statement MERGEs the same relationship with
//     DIFFERENT property values and commits after T1's begin.
//  3. T1 runs the identical UNWIND MERGE chain: the relationship lookup
//     (latest-committed read) finds the peer's edge, the SET values differ,
//     and store.UpdateEdge previously failed with "not found" because the
//     edge is invisible at T1's snapshot.
//
// The failure must be storage.ErrConflict (retryable), and a retry on a
// fresh snapshot must converge to exactly one edge carrying the retrying
// writer's values — Neo4j's observable outcome for this interleaving.
func TestBug_UnwindMergeChainPeerCommittedEdgeUpdateIsRetryableConflict(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	writerExec := NewStorageExecutor(engine)
	peerExec := NewStorageExecutor(engine)
	ctx := context.Background()

	seedRelEndpoints(t, writerExec)

	_, err := writerExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = writerExec.Execute(ctx, "ROLLBACK", nil)
		}
	}()

	// Peer commits the same relationship after T1's begin.
	_, err = peerExec.Execute(ctx, unwindMergeRelCypher, unwindMergeRelRows("x-peer", "y-peer"))
	require.NoError(t, err)

	// T1 runs the same statement with different SET values.
	_, err = writerExec.Execute(ctx, unwindMergeRelCypher, unwindMergeRelRows("x-writer", "y-writer"))
	require.Error(t, err, "peer commit during open tx must surface a write-write conflict")
	t.Logf("observed error: %v", err)
	require.ErrorIs(t, err, storage.ErrConflict,
		"peer-committed edge update must be a retryable conflict, got: %v", err)
	require.NotErrorIs(t, err, storage.ErrNotFound)
	_, rbErr := writerExec.Execute(ctx, "ROLLBACK", nil)
	rolledBack = true
	require.NoError(t, rbErr)

	// Retry on a fresh snapshot (what a driver's managed transaction does
	// on a TransientError) must merge onto the peer's edge and win.
	_, err = writerExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack = false
	_, err = writerExec.Execute(ctx, unwindMergeRelCypher, unwindMergeRelRows("x-writer", "y-writer"))
	require.NoError(t, err, "retry with a fresh snapshot must merge onto the peer edge")
	_, err = writerExec.Execute(ctx, "COMMIT", nil)
	rolledBack = true
	require.NoError(t, err)

	requireSingleEdgeWithX(t, writerExec, "x-writer")
}

// TestBug_PlainMergeSetPeerCommittedEdgeUpdateIsRetryableConflict is the
// non-batched variant: a plain MERGE ... SET on a single relationship hits
// the same latest-committed-lookup / snapshot-update divergence.
func TestBug_PlainMergeSetPeerCommittedEdgeUpdateIsRetryableConflict(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	writerExec := NewStorageExecutor(engine)
	peerExec := NewStorageExecutor(engine)
	ctx := context.Background()

	seedRelEndpoints(t, writerExec)

	const plainMergeSet = `MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:REL]->(b)
SET rel.x = $x`

	_, err := writerExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack := false
	defer func() {
		if !rolledBack {
			_, _ = writerExec.Execute(ctx, "ROLLBACK", nil)
		}
	}()

	_, err = peerExec.Execute(ctx, plainMergeSet, map[string]interface{}{"x": "x-peer"})
	require.NoError(t, err)

	_, err = writerExec.Execute(ctx, plainMergeSet, map[string]interface{}{"x": "x-writer"})
	require.Error(t, err, "peer commit during open tx must surface a write-write conflict")
	t.Logf("observed error: %v", err)
	require.ErrorIs(t, err, storage.ErrConflict,
		"peer-committed edge update must be a retryable conflict, got: %v", err)
	require.NotErrorIs(t, err, storage.ErrNotFound)
	_, rbErr := writerExec.Execute(ctx, "ROLLBACK", nil)
	rolledBack = true
	require.NoError(t, rbErr)

	// Retry on a fresh snapshot converges to one edge with the writer's value.
	_, err = writerExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	rolledBack = false
	_, err = writerExec.Execute(ctx, plainMergeSet, map[string]interface{}{"x": "x-writer"})
	require.NoError(t, err, "retry with a fresh snapshot must merge onto the peer edge")
	_, err = writerExec.Execute(ctx, "COMMIT", nil)
	rolledBack = true
	require.NoError(t, err)

	requireSingleEdgeWithX(t, writerExec, "x-writer")
}

// TestBug_UnwindMergeChainPeerCommitIdenticalSetValuesIsSilentNoOp is the
// success-path control: when the racing writers stamp IDENTICAL property
// values, the assignment diff reports no change, UpdateEdge is never called,
// and the statement succeeds. The conflict reclassification must not change
// this path.
func TestBug_UnwindMergeChainPeerCommitIdenticalSetValuesIsSilentNoOp(t *testing.T) {
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

	_, err = peerExec.Execute(ctx, unwindMergeRelCypher, unwindMergeRelRows("x-same", "y-same"))
	require.NoError(t, err)

	_, err = writerExec.Execute(ctx, unwindMergeRelCypher, unwindMergeRelRows("x-same", "y-same"))
	require.NoError(t, err, "identical SET values must not attempt an update")
	_, err = writerExec.Execute(ctx, "COMMIT", nil)
	committed = true
	require.NoError(t, err)

	requireSingleEdgeWithX(t, writerExec, "x-same")
}
