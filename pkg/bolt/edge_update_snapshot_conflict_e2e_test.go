// Bolt-level end-to-end test for the edge-update snapshot conflict
// classification bug.
//
// BUG: when a peer session MERGEd the same relationship with different
// property values and committed after an explicit transaction began, the
// explicit transaction's MERGE ... SET failed with "not found" and surfaced
// over the wire as Neo.ClientError.Statement.SyntaxError — a non-retryable
// client error. Neo4j succeeds on this exact interleaving, and its only
// sanctioned conflict failure is a transient code that every driver's
// managed-transaction API (session.ExecuteWrite) auto-retries. The fix
// classifies the failure as storage.ErrConflict, which maps to
// Neo.TransientError.Transaction.Outdated, so a driver retry on a fresh
// snapshot converges to Neo4j's observable outcome: one relationship
// carrying the retrying writer's values.
package bolt

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestBoltExplicitTxPeerCommittedEdgeUpdateIsTransientOutdated drives the
// full wire path: handshake, HELLO, BEGIN, RUN, PULL over real Bolt
// connections against the session-scoped explicit-transaction executor.
func TestBoltExplicitTxPeerCommittedEdgeUpdateIsTransientOutdated(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = baseStore.Close() })
	store := storage.NewNamespacedEngine(baseStore, "neo4j")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	const mergeSetWriter = "MATCH (a:A {key: 'a1'}) MATCH (b:B {key: 'b1'}) MERGE (a)-[rel:REL]->(b) SET rel.x = 'x-writer'"
	const mergeSetPeer = "MATCH (a:A {key: 'a1'}) MATCH (b:B {key: 'b1'}) MERGE (a)-[rel:REL]->(b) SET rel.x = 'x-peer'"

	// Seed the endpoints before any contended transaction begins.
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup, "CREATE (:A {key: 'a1'})")
	runBoltQueryAndCollectRecords(t, setup, "CREATE (:B {key: 'b1'})")

	// Writer session: BEGIN pins the snapshot before the peer's commit.
	writer := openBoltTestConn(t, port)
	requireNoError(t, SendBegin(t, writer, nil))
	requireNoError(t, ReadSuccess(t, writer))

	// Peer session: autocommit MERGE of the same relationship with
	// different property values, committed after the writer's BEGIN.
	peer := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, peer, mergeSetPeer)

	// Writer runs the same MERGE ... SET with different values. The
	// relationship lookup sees the peer's edge (latest-committed read);
	// the property update previously failed with "not found" and
	// surfaced as a non-retryable SyntaxError. The wire code MUST be the
	// transient, driver-retryable Neo.TransientError.Transaction.Outdated.
	requireNoError(t, SendRun(t, writer, mergeSetWriter, nil, nil))
	code, message, err := AssertFailure(t, writer)
	requireNoError(t, err)
	t.Logf("observed failure: code=%s message=%s", code, message)
	if code != "Neo.TransientError.Transaction.Outdated" {
		t.Fatalf("expected Neo.TransientError.Transaction.Outdated, got code=%q message=%q", code, message)
	}
	if !strings.Contains(message, "changed after transaction start") {
		t.Errorf("expected pinned conflict substring in message, got %q", message)
	}

	// Retry on a fresh connection (a driver retries transient failures on
	// a fresh transaction with a fresh snapshot) must succeed.
	retry := openBoltTestConn(t, port)
	requireNoError(t, SendBegin(t, retry, nil))
	requireNoError(t, ReadSuccess(t, retry))
	runBoltQueryAndCollectRecords(t, retry, mergeSetWriter)
	requireNoError(t, SendCommit(t, retry))
	requireNoError(t, ReadSuccess(t, retry))

	// Converged state: exactly one relationship with the retrying
	// writer's value — Neo4j's observable outcome for this interleaving.
	verify := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, verify,
		"MATCH (:A {key: 'a1'})-[rel:REL]->(:B {key: 'b1'}) RETURN count(rel), collect(rel.x)")
	if len(records) != 1 {
		t.Fatalf("expected one row, got %d (%v)", len(records), records)
	}
	row := records[0]
	if count, ok := row[0].(int64); !ok || count != 1 {
		t.Fatalf("expected exactly one REL edge after retry, got %v (row=%v)", row[0], row)
	}
	xs, ok := row[1].([]any)
	if !ok || len(xs) != 1 || xs[0] != "x-writer" {
		t.Fatalf("expected the retrying writer's value to win, got %v", row[1])
	}
}
