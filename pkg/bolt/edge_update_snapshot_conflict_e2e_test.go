// A relationship writer whose BEGIN predates a peer's commit applies its
// update under the entity write lock, without a client retry. This follows
// the same latest-committed writer contract as the overlapping lock-wait tests.
package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestBoltExplicitWriterAppliesToPeerCommittedRelationship drives the
// full wire path: handshake, HELLO, BEGIN, RUN, PULL over real Bolt
// connections against the session-scoped explicit-transaction executor.
func TestBoltExplicitWriterAppliesToPeerCommittedRelationship(t *testing.T) {
	baseStore := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = baseStore.Close() })
	store := storage.NewNamespacedEngine(baseStore, "neo4j")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

	const mergeSetWriter = "MATCH (a:A {key: 'a1'}) MATCH (b:B {key: 'b1'}) MERGE (a)-[rel:REL]->(b) SET rel.x = 'x-writer'"
	const mergeSetPeer = "MATCH (a:A {key: 'a1'}) MATCH (b:B {key: 'b1'}) MERGE (a)-[rel:REL]->(b) SET rel.x = 'x-peer'"

	// Seed the endpoints before any contended transaction begins.
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "CREATE (:A {key: 'a1'})", nil)
	runBoltQueryWithParams(t, setup, "CREATE (:B {key: 'b1'})", nil)

	// Writer session: BEGIN pins the snapshot before the peer's commit.
	writer := openBoltTestConn(t, port)
	requireNoError(t, SendBegin(t, writer, nil))
	requireNoError(t, ReadSuccess(t, writer))

	// Peer session: autocommit MERGE of the same relationship with
	// different property values, committed after the writer's BEGIN.
	peer := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, peer, mergeSetPeer, nil)

	// The peer has finished: the writer takes the relationship's write lock
	// and applies its SET to that committed image despite its older BEGIN.
	runBoltQueryWithParams(t, writer, mergeSetWriter, nil)
	requireNoError(t, SendCommit(t, writer))
	requireNoError(t, ReadSuccess(t, writer))

	// Converged state: exactly one relationship with the
	// writer's value — Neo4j's observable outcome for this interleaving.
	verify := openBoltTestConn(t, port)
	records := runBoltQueryWithParams(t, verify,
		"MATCH (:A {key: 'a1'})-[rel:REL]->(:B {key: 'b1'}) RETURN count(rel), collect(rel.x)", nil)
	if len(records) != 1 {
		t.Fatalf("expected one row, got %d (%v)", len(records), records)
	}
	row := records[0]
	if count, ok := row[0].(int64); !ok || count != 1 {
		t.Fatalf("expected exactly one REL edge after both commits, got %v (row=%v)", row[0], row)
	}
	xs, ok := row[1].([]any)
	if !ok || len(xs) != 1 || xs[0] != "x-writer" {
		t.Fatalf("expected the writer's value to win, got %v", row[1])
	}
}
