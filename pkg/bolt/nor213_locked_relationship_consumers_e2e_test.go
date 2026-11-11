// Bolt wire end-to-end coverage for NOR-213: everything that consumes a
// relationship, or the node at its other end, returned by a locked node's
// last committed relationship read. Every consumer of that row sees the same
// last committed entity, in the same statement and in later ones, and every
// stored structure the transaction maintains from it agrees with the data
// after commit.
package bolt

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nor213FreshPeer is one way a peer changes x's relationship or its other end
// after the reader's snapshot.
type nor213FreshPeer struct {
	name string
	// seed creates the graph; warm is the reader's read before the peer
	// commits (it fills the reader's snapshot caches); change is the peer's
	// statement, run after x's lock in the waiting case.
	seed, warm, change string
}

func nor213FreshPeers() []nor213FreshPeer {
	return []nor213FreshPeer{
		{
			name:   "new-other-end",
			seed:   "CREATE (:Probe {id: $x})",
			warm:   "MATCH (x:Probe {id: $x}) RETURN x.id",
			change: "MATCH (x:Probe {id: $x}) SET x.touch = 1 WITH x CREATE (x)-[:LINK {p: 7}]->(:Held:After {id: $y, v: 9})",
		},
		{
			name:   "cached-other-end-relabelled",
			seed:   "CREATE (:Probe {id: $x}), (:Anchor {id: $x})-[:WARM]->(:Held {id: $y, v: 1})",
			warm:   "MATCH (:Anchor {id: $x})-[:WARM]->(y:Held) RETURN y.v, labels(y)",
			change: "MATCH (x:Probe {id: $x}), (y:Held {id: $y}) SET x.touch = 1, y:After, y.v = 9 CREATE (x)-[:LINK {p: 7}]->(y)",
		},
		{
			name:   "existing-relationship-changed",
			seed:   "CREATE (:Probe {id: $x})-[:LINK {p: 1}]->(:Held {id: $y, v: 1})",
			warm:   "MATCH (:Probe {id: $x})-[r:LINK]->(y) RETURN r.p, y.v, labels(y)",
			change: "MATCH (x:Probe {id: $x})-[r:LINK]->(y) SET x.touch = 1, r.p = 7, y.v = 9, y:After",
		},
	}
}

const nor213FreshReads = "MATCH p = (x)-[r:LINK]->(y) RETURN startNode(r).id, endNode(r).v, type(r), r.p, properties(r).p, " +
	"y.v, y {.v}.v, 'After' IN labels(y), elementId(endNode(r)) = elementId(y), nodes(p)[1].v, relationships(p)[0].p"

// nor213FreshRun runs the reader's statement after the peer's change: either
// after the peer committed, or while the peer holds x's lock (the reader
// waits, then the peer commits).
func nor213FreshRun(t *testing.T, port int, waits <-chan struct{}, peer nor213FreshPeer, timing string, params map[string]any, statement string) (net.Conn, [][]any, error) {
	t.Helper()
	setup := openBoltTestConn(t, port)
	defer setup.Close()
	runBoltQueryWithParams(t, setup, peer.seed, params)
	reader := openBoltTestConn(t, port)
	nor213BoltBegin(t, reader)
	runBoltQueryWithParams(t, reader, peer.warm, params)
	writer := openBoltTestConn(t, port)
	defer writer.Close()
	nor213BoltBegin(t, writer)
	runBoltQueryWithParams(t, writer, peer.change, params)
	if timing == "committed-before" {
		nor213BoltCommit(t, writer)
		rows, err := runBoltQueryRows(t, reader, statement, params)
		return reader, rows, err
	}
	nor213DrainEntityWriteWaits(waits)
	type outcome struct {
		rows [][]any
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		rows, err := runBoltQueryRows(t, reader, statement, params)
		done <- outcome{rows, err}
	}()
	select {
	case <-waits:
	case got := <-done:
		t.Fatalf("the reader did not wait for x's lock: %v %v", got.rows, got.err)
	case <-time.After(nor213WireWaitLimit):
		t.Fatal("the reader neither waited nor finished")
	}
	nor213BoltCommit(t, writer)
	select {
	case got := <-done:
		return reader, got.rows, got.err
	case <-time.After(nor213WireWaitLimit):
		_ = reader.Close()
		t.Fatal("the waiting reader did not resume")
	}
	return reader, nil, nil
}

// nor213RequireStoredAgreement checks in a fresh session that the stored
// relationship counts, the label counts and the property index give the same
// answers as traversing and scanning the rows.
func nor213RequireStoredAgreement(t *testing.T, port int, what string) {
	t.Helper()
	conn := openBoltTestConn(t, port)
	defer conn.Close()
	one := func(query string) any {
		rows := runBoltQueryWithParams(t, conn, query, nil)
		require.Len(t, rows, 1, query)
		return rows[0][0]
	}
	for _, relType := range []string{"LINK", "OTHER", "OTHER2"} {
		for _, label := range []string{"After", "Held", "Extra"} {
			stored := one(fmt.Sprintf("MATCH ()-[r:%s]->(:%s) RETURN count(r)", relType, label))
			traversed := one(fmt.Sprintf("MATCH (a)-[r:%s]->(b) WHERE b:%s RETURN size(collect(r))", relType, label))
			require.Equal(t, traversed, stored, "%s: stored count of (:*)-[:%s]->(:%s) agrees with the rows", what, relType, label)
			storedStart := one(fmt.Sprintf("MATCH (:Probe)-[r:%s]->(:%s) RETURN count(r)", relType, label))
			traversedStart := one(fmt.Sprintf("MATCH (a)-[r:%s]->(b) WHERE a:Probe AND b:%s RETURN size(collect(r))", relType, label))
			require.Equal(t, traversedStart, storedStart, "%s: stored count of (:Probe)-[:%s]->(:%s) agrees with the rows", what, relType, label)
		}
	}
	for _, label := range []string{"After", "Held", "Extra"} {
		require.Equal(t, one(fmt.Sprintf("MATCH (n) WHERE n:%s RETURN size(collect(n))", label)), one(fmt.Sprintf("MATCH (n:%s) RETURN count(n)", label)), "%s: label count of %s", what, label)
	}
	for _, value := range []int64{1, 9, 10} {
		indexed := one(fmt.Sprintf("MATCH (n:Held {v: %d}) RETURN count(n)", value))
		scanned := one(fmt.Sprintf("MATCH (n:Held) WHERE n.v + 0 = %d RETURN count(n)", value))
		require.Equal(t, scanned, indexed, "%s: index lookup of Held.v = %d", what, value)
	}
}

// TestBoltLockedNodeFreshEntitiesAreConsistentForEveryConsumer: after a peer
// created or changed x's relationship and its other end y, the reader takes
// x's lock and expands to y. Every read consumer returns the last committed
// relationship and y, in the same statement and in a later one.
func TestBoltLockedNodeFreshEntitiesAreConsistentForEveryConsumer(t *testing.T) {
	port, waits := nor213RelationshipStore(t, false)
	for _, peer := range nor213FreshPeers() {
		for _, timing := range []string{"committed-before", "waited"} {
			key := peer.name + "-" + timing
			params := map[string]any{"x": key + "-x", "y": key + "-y"}
			reader, rows, err := nor213FreshRun(t, port, waits, peer, timing, params, "MATCH (x:Probe {id: $x}) SET x += {} WITH x "+nor213FreshReads)
			require.NoError(t, err, key)
			want := []any{params["x"], int64(9), "LINK", int64(7), int64(7), int64(9), int64(9), true, true, int64(9), int64(7)}
			require.Equal(t, [][]any{want}, rows, "%s: the locking statement", key)
			require.Equal(t, [][]any{want}, runBoltQueryWithParams(t, reader, "MATCH (x:Probe {id: $x}) WITH x "+nor213FreshReads, params), "%s: a later statement", key)
			nor213BoltCommit(t, reader)
			_ = reader.Close()
		}
	}
}

// TestBoltLockedNodeFreshEntityWritesKeepStoredStructuresConsistent: the
// reader writes through the relationship or its other end returned by x's
// last committed read and commits; a fresh session then finds the stored
// relationship counts, label counts and property index in agreement with the
// rows.
func TestBoltLockedNodeFreshEntityWritesKeepStoredStructuresConsistent(t *testing.T) {
	writes := []struct {
		name, clause string
		// outcome is checked in a fresh session after commit.
		verify string
		want   []any
	}{
		{"delete-relationship", "DELETE r", "MATCH (:Probe {id: $x})-[r:LINK]->() RETURN count(r)", []any{int64(0)}},
		{"set-relationship-property", "SET r.p = 8", "MATCH (:Probe {id: $x})-[r:LINK]->() RETURN r.p", []any{int64(8)}},
		{"set-other-end-property", "SET y.v = 10", "MATCH (y:Held {id: $y}) RETURN y.v, 'After' IN labels(y)", []any{int64(10), true}},
		{"add-other-end-label", "SET y:Extra", "MATCH (y:Held {id: $y}) RETURN y.v, 'Extra' IN labels(y)", []any{int64(9), true}},
		{"remove-other-end-label", "REMOVE y:After", "MATCH (y:Held {id: $y}) RETURN y.v, 'After' IN labels(y)", []any{int64(9), false}},
		{"create-relationship-to-other-end", "CREATE (x)-[:OTHER]->(y)", "MATCH (:Probe {id: $x})-[r:OTHER]->(y) RETURN count(r), y.v", []any{int64(1), int64(9)}},
		{"merge-relationship-to-other-end", "MERGE (x)-[:OTHER2]->(y)", "MATCH (:Probe {id: $x})-[r:OTHER2]->(y) RETURN count(r), y.v", []any{int64(1), int64(9)}},
	}
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "CREATE INDEX held_v FOR (n:Held) ON (n.v)", nil)
	for _, peer := range nor213FreshPeers() {
		for _, write := range writes {
			for _, timing := range []string{"committed-before", "waited"} {
				key := strings.Join([]string{peer.name, write.name, timing}, "-")
				params := map[string]any{"x": key + "-x", "y": key + "-y"}
				reader, _, err := nor213FreshRun(t, port, waits, peer, timing, params, "MATCH (x:Probe {id: $x}) SET x += {} WITH x MATCH (x)-[r:LINK]->(y) "+write.clause+" RETURN count(*)")
				require.NoError(t, err, key)
				nor213BoltCommit(t, reader)
				_ = reader.Close()
				got := runBoltQueryWithParams(t, setup, write.verify, params)
				require.Equal(t, [][]any{write.want}, got, "%s: the committed write", key)
				nor213RequireStoredAgreement(t, port, key)
			}
		}
	}
}

// TestBoltLockedNodeFreshOtherEndDetachDeleteIsRejectedWithoutDamage: a
// DETACH DELETE of the other end enumerates its relationships from its
// snapshot, which does not hold the peer's new relationship. The commit is
// rejected with a retryable conflict (as before this change), and nothing of
// the transaction is stored.
func TestBoltLockedNodeFreshOtherEndDetachDeleteIsRejectedWithoutDamage(t *testing.T) {
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	for _, peer := range nor213FreshPeers() {
		for _, timing := range []string{"committed-before", "waited"} {
			key := peer.name + "-detach-" + timing
			params := map[string]any{"x": key + "-x", "y": key + "-y"}
			reader, _, err := nor213FreshRun(t, port, waits, peer, timing, params, "MATCH (x:Probe {id: $x}) SET x += {} WITH x MATCH (x)-[r:LINK]->(y) DETACH DELETE y RETURN count(*)")
			if err == nil {
				requireNoError(t, SendCommit(t, reader))
				msgType, msgData, readErr := ReadMessage(reader)
				requireNoError(t, readErr)
				if msgType != MsgSuccess {
					err = nor213BoltOutcome(msgType, msgData)
				}
			}
			_ = reader.Close()
			if err != nil {
				require.Contains(t, err.Error(), "Neo.TransientError.Transaction.Outdated", key)
				require.Equal(t, [][]any{{int64(1), int64(9)}}, runBoltQueryWithParams(t, setup, "MATCH (:Probe {id: $x})-[r:LINK]->(y:Held {id: $y}) RETURN count(r), y.v", params), "%s: nothing of the rejected transaction is stored", key)
			} else {
				require.Equal(t, [][]any{{int64(0)}}, runBoltQueryWithParams(t, setup, "MATCH (y:Held {id: $y}) RETURN count(y)", params), "%s: the committed delete", key)
			}
			nor213RequireStoredAgreement(t, port, key)
		}
	}
}
