// Bolt wire end-to-end coverage for NOR-213's stored relationship counts: a
// transaction that holds a node's write lock reads that node's relationships
// and their other ends as last committed, so the labels it reads for an end
// node can change between two of its own statements, and between a statement
// and its commit, when another transaction relabels, deletes or recreates
// that node. A label change that stays open while another transaction
// creates or deletes a relationship at the node and commits first is the same
// question in the other direction. Whatever each committed transaction adds
// to and removes from the stored relationship counts must match what it
// leaves in the stored graph.
package bolt

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nor213CountSetup has stored relationship counts for both labels the
// sequences move between, as in the reported reproduction.
const nor213CountSetup = "CREATE (x:Probe {id:'x'})-[:LINK]->(:Held {id:'y'}), " +
	"(x)-[:LINK]->(:Held {id:'z'}), " +
	"(:Probe)-[:OTHER]->(:Held), (:Probe)-[:OTHER]->(:After), (:Held)-[:OTHER]->(:Probe), (:After)-[:OTHER]->(:Probe)"

// nor213CountTypes and nor213CountLabels are the relationship types and labels
// the sequences touch.
var (
	nor213CountTypes  = []string{"OTHER", "LINK", "NEW"}
	nor213CountLabels = []string{"Held", "After", "Probe", "Third"}
)

// nor213RequireCountsMatchGraph compares, on a fresh connection, every stored
// relationship count the engine keeps for the touched labels and types (by
// end label and by start label) and the label counts with a count by
// traversal.
func nor213RequireCountsMatchGraph(t *testing.T, port int, what string) {
	t.Helper()
	conn := openBoltTestConn(t, port)
	defer conn.Close()
	nor213RequireCountsMatchGraphOn(t, conn, what)
}

// nor213RequireCountsMatchGraphOn is nor213RequireCountsMatchGraph on conn,
// which may hold an open transaction: its counts must then match the graph
// that transaction reads.
func nor213RequireCountsMatchGraphOn(t *testing.T, conn net.Conn, what string) {
	t.Helper()
	one := func(query string) any {
		rows := runBoltQueryWithParams(t, conn, query, nil)
		require.Len(t, rows, 1, query)
		return rows[0][0]
	}
	for _, relType := range nor213CountTypes {
		require.Equal(t, one(fmt.Sprintf("MATCH (a)-[r:%s]->(b) RETURN size(collect(r))", relType)),
			one(fmt.Sprintf("MATCH ()-[r:%s]->() RETURN count(r)", relType)), "%s: stored count of :%s", what, relType)
		for _, label := range nor213CountLabels {
			require.Equal(t, one(fmt.Sprintf("MATCH (a)-[r:%s]->(b) WHERE b:%s RETURN size(collect(r))", relType, label)),
				one(fmt.Sprintf("MATCH ()-[r:%s]->(:%s) RETURN count(r)", relType, label)), "%s: stored count of ()-[:%s]->(:%s)", what, relType, label)
			require.Equal(t, one(fmt.Sprintf("MATCH (a)-[r:%s]->(b) WHERE a:%s RETURN size(collect(r))", relType, label)),
				one(fmt.Sprintf("MATCH (:%s)-[r:%s]->() RETURN count(r)", label, relType)), "%s: stored count of (:%s)-[:%s]->()", what, label, relType)
		}
	}
	for _, label := range nor213CountLabels {
		require.Equal(t, one(fmt.Sprintf("MATCH (n) WHERE n:%s RETURN size(collect(n))", label)),
			one(fmt.Sprintf("MATCH (n:%s) RETURN count(n)", label)), "%s: label count of %s", what, label)
	}
}

// nor213CountStep is one statement of the sequence, run by the transaction
// under test (reader) or committed by another connection (peer).
type nor213CountStep struct {
	peer      bool
	statement string
	// mayFail allows the statement, or the transaction's commit, to be
	// refused; a refused transaction persists nothing.
	mayFail bool
	// peerMayWait runs a peer statement that can wait for a lock the
	// transaction under test holds on its own connection, and collects its
	// outcome after that transaction ends.
	peerMayWait bool
}

const (
	nor213LockAndCreate   = "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[:LINK]->(y {id:'y'}) CREATE (x)-[:OTHER {id:'own'}]->(y)"
	nor213DeleteOwn       = "MATCH ()-[r:OTHER {id:'own'}]->() DELETE r"
	nor213RelabelY        = "MATCH (y {id:'y'}) REMOVE y:Held SET y:After"
	nor213OwnRelabelY     = "MATCH (y {id:'y'}) REMOVE y:After SET y:Third"
	nor213DeleteLinkToY   = "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[r:LINK]->({id:'y'}) DELETE r"
	nor213DetachDeleteY   = "MATCH (y {id:'y'}) DETACH DELETE y"
	nor213DetachDeleteX   = "MATCH (x:Probe {id:'x'}) DETACH DELETE x"
	nor213PeerRecreateY   = "CREATE (:Third {id:'y'})"
	nor213PeerRelabelYTwo = "MATCH (y {id:'y'}) REMOVE y:After SET y:Third"
)

// TestBoltLockedNodeCountsMatchTheStoredGraphAcrossPeerRelabels: after each
// sequence commits, the stored relationship counts equal the counts by
// traversal, durable and with write-behind commits.
func TestBoltLockedNodeCountsMatchTheStoredGraphAcrossPeerRelabels(t *testing.T) {
	sequences := []struct {
		name  string
		steps []nor213CountStep
	}{
		{
			// The reported sequence: create, peer relabel, delete own relationship.
			name: "create-then-delete-own-across-relabel",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DeleteOwn},
			},
		},
		{
			name: "create-only-then-relabel",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
			},
		},
		{
			name: "relabel-then-create",
			steps: []nor213CountStep{
				{peer: true, statement: nor213RelabelY},
				{statement: nor213LockAndCreate},
			},
		},
		{
			name: "relabel-then-delete-existing",
			steps: []nor213CountStep{
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DeleteLinkToY},
			},
		},
		{
			name: "delete-existing-then-relabel",
			steps: []nor213CountStep{
				{statement: nor213DeleteLinkToY},
				{peer: true, statement: nor213RelabelY},
			},
		},
		{
			name: "read-then-relabel-then-delete-existing",
			steps: []nor213CountStep{
				{statement: "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[:LINK]->(y {id:'y'}) RETURN labels(y)"},
				{peer: true, statement: nor213RelabelY},
				{statement: "MATCH (x:Probe {id:'x'})-[r:LINK]->({id:'y'}) DELETE r"},
			},
		},
		{
			name: "both-ends-relabelled-create-and-delete",
			steps: []nor213CountStep{
				{statement: "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[:LINK]->(y {id:'y'}), (x)-[:LINK]->(z {id:'z'}) CREATE (y)-[:OTHER {id:'own'}]->(z)"},
				{peer: true, statement: "MATCH (n) WHERE n.id IN ['y', 'z'] REMOVE n:Held SET n:After"},
				{statement: nor213DeleteOwn},
			},
		},
		{
			name: "both-ends-relabelled-create-only",
			steps: []nor213CountStep{
				{statement: "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[:LINK]->(y {id:'y'}), (x)-[:LINK]->(z {id:'z'}) CREATE (y)-[:OTHER {id:'own'}]->(z)"},
				{peer: true, statement: "MATCH (n) WHERE n.id IN ['y', 'z'] REMOVE n:Held SET n:After"},
			},
		},
		{
			name: "peer-deletes-other-end-after-create",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate, mayFail: true},
				{peer: true, statement: nor213DetachDeleteY},
			},
		},
		{
			name: "peer-deletes-other-end-after-create-then-delete-own",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate, mayFail: true},
				{peer: true, statement: nor213DetachDeleteY},
				{statement: nor213DeleteOwn, mayFail: true},
			},
		},
		{
			// The transaction's own relationship is gone before the other end
			// is: created and deleted, it contributes nothing.
			name: "create-delete-own-then-peer-deletes-other-end",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DeleteOwn},
				{peer: true, statement: nor213DetachDeleteY},
			},
		},
		{
			name: "create-delete-own-then-peer-recreates-other-end",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DeleteOwn},
				{peer: true, statement: nor213DetachDeleteY},
				{peer: true, statement: nor213PeerRecreateY},
			},
		},
		{
			name: "create-relabelled-twice-delete-own-then-peer-deletes-other-end",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{peer: true, statement: nor213PeerRelabelYTwo},
				{statement: nor213DeleteOwn},
				{peer: true, statement: nor213DetachDeleteY},
			},
		},
		{
			// A node delete removes a relationship whose other end another
			// transaction relabels before the delete commits.
			name: "cascade-then-peer-relabels-other-end",
			steps: []nor213CountStep{
				{statement: nor213DetachDeleteX},
				{peer: true, statement: nor213RelabelY},
			},
		},
		{
			name: "peer-relabel-then-cascade",
			steps: []nor213CountStep{
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DetachDeleteX},
			},
		},
		{
			// The transaction deletes an existing relationship; another
			// transaction deletes its other end, waiting for the
			// relationship or refused.
			name: "delete-existing-then-peer-deletes-other-end",
			steps: []nor213CountStep{
				{statement: nor213DeleteLinkToY},
				{peer: true, statement: nor213DetachDeleteY, peerMayWait: true, mayFail: true},
			},
		},
		{
			name: "delete-existing-and-create-then-peer-deletes-other-end",
			steps: []nor213CountStep{
				{statement: nor213DeleteLinkToY},
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213DetachDeleteY, peerMayWait: true, mayFail: true},
			},
		},
		{
			// The transaction comes to write an end node it first only reached
			// as the other end of its relationship.
			name: "create-peer-relabel-then-own-relabel",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213OwnRelabelY},
			},
		},
		{
			name: "delete-existing-peer-relabel-then-own-relabel",
			steps: []nor213CountStep{
				{statement: nor213DeleteLinkToY},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213OwnRelabelY},
			},
		},
		{
			name: "create-peer-relabel-own-property-then-own-relabel",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: "MATCH (y {id:'y'}) SET y.v = 7"},
				{statement: nor213OwnRelabelY},
			},
		},
		{
			name: "create-peer-relabel-then-own-detach-delete-other-end",
			steps: []nor213CountStep{
				{statement: nor213LockAndCreate},
				{peer: true, statement: nor213RelabelY},
				{statement: nor213DetachDeleteY},
			},
		},
	}
	for _, writeBehind := range []bool{false, true} {
		for _, sequence := range sequences {
			t.Run(fmt.Sprintf("writeBehind=%v/%s", writeBehind, sequence.name), func(t *testing.T) {
				port, waits := nor213RelationshipStore(t, writeBehind)
				setup := openBoltTestConn(t, port)
				defer setup.Close()
				runBoltQueryWithParams(t, setup, nor213CountSetup, nil)
				nor213RequireCountsMatchGraph(t, port, sequence.name+": before")

				reader := openBoltTestConn(t, port)
				defer reader.Close()
				nor213BoltBegin(t, reader)
				runBoltQueryWithParams(t, reader, "MATCH (x:Probe {id:'x'}) RETURN x.id", nil)
				runBoltQueryWithParams(t, setup, "CREATE (:Tick)", nil)

				type waitingPeer struct {
					conn net.Conn
					done <-chan error
					step nor213CountStep
				}
				var peers []waitingPeer
				mayFail := false
				var failed error
				for _, step := range sequence.steps {
					if step.peer && step.peerMayWait {
						conn := openBoltTestConn(t, port)
						defer conn.Close()
						nor213DrainEntityWriteWaits(waits)
						done := make(chan error, 1)
						go func(statement string) { done <- nor213RunBoltStatement(t, conn, statement, nil) }(step.statement)
						peer := waitingPeer{conn: conn, done: done, step: step}
						select {
						case <-waits:
						case err := <-done:
							ended := make(chan error, 1)
							ended <- err
							peer.done = ended
						case <-time.After(nor213WireWaitLimit):
							t.Fatalf("%s: peer statement neither waited nor finished: %s", sequence.name, step.statement)
						}
						peers = append(peers, peer)
						continue
					}
					if step.peer {
						runBoltQueryWithParams(t, setup, step.statement, nil)
						continue
					}
					mayFail = mayFail || step.mayFail
					if failed != nil {
						break
					}
					if _, err := runBoltQueryRows(t, reader, step.statement, nil); err != nil {
						require.True(t, step.mayFail, "%s: %s: %v", sequence.name, step.statement, err)
						failed = err
					}
				}
				if failed == nil {
					failed = nor213BoltCommitResult(t, reader)
					require.True(t, failed == nil || mayFail, "%s: commit: %v", sequence.name, failed)
				} else {
					// A failed statement ends the transaction: RESET rolls it
					// back and releases its locks for a waiting peer.
					requireNoError(t, SendReset(t, reader))
					_, _, _ = ReadMessage(reader)
				}
				for _, peer := range peers {
					err := nor213AwaitBoltStatement(t, peer.conn, peer.done, sequence.name+": peer "+peer.step.statement)
					require.True(t, err == nil || peer.step.mayFail, "%s: peer %s: %v", sequence.name, peer.step.statement, err)
				}
				nor213RequireCountsMatchGraph(t, port, sequence.name+": after commit")
			})
		}
	}
}

// TestBoltRelabelAndRelationshipCountsMatchTheStoredGraphInEitherCommitOrder:
// one transaction changes a node's labels and stays open while another
// creates or deletes a relationship at that node, at either end of it. The
// stored counts must match the stored graph whichever commits first,
// including a relationship change committed by an auto-commit statement
// while the label change is still open.
func TestBoltRelabelAndRelationshipCountsMatchTheStoredGraphInEitherCommitOrder(t *testing.T) {
	const relabel = "MATCH (y {id:'y'}) REMOVE y:Held SET y:After"
	for _, writeBehind := range []bool{false, true} {
		for _, change := range []string{"create", "delete"} {
			for _, position := range []string{"end", "start"} {
				for _, order := range []string{"relabel-last", "relabel-first", "autocommit-relationship-then-relabel"} {
					t.Run(fmt.Sprintf("writeBehind=%v/%s/y-at-%s/%s", writeBehind, change, position, order), func(t *testing.T) {
						port, _ := nor213RelationshipStore(t, writeBehind)
						peer := openBoltTestConn(t, port)
						defer peer.Close()
						runBoltQueryWithParams(t, peer, nor213CountSetup, nil)
						pattern := "(x)-[:OTHER {id:'own'}]->(y)"
						if position == "start" {
							pattern = "(y)-[:OTHER {id:'own'}]->(x)"
						}
						statement := "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[:LINK]->(y {id:'y'}) CREATE " + pattern
						if change == "delete" {
							runBoltQueryWithParams(t, peer, "MATCH (x:Probe {id:'x'})-[:LINK]->(y {id:'y'}) CREATE "+pattern, nil)
							statement = "MATCH (x:Probe {id:'x'}) SET x += {} WITH x MATCH (x)-[r:OTHER {id:'own'}]-() DELETE r"
						}
						nor213RequireCountsMatchGraph(t, port, "before")

						labeller := openBoltTestConn(t, port)
						defer labeller.Close()
						nor213BoltBegin(t, labeller)
						runBoltQueryWithParams(t, labeller, "MATCH (y {id:'y'}) RETURN y.id", nil)
						writer := openBoltTestConn(t, port)
						defer writer.Close()
						if order != "autocommit-relationship-then-relabel" {
							nor213BoltBegin(t, writer)
							runBoltQueryWithParams(t, writer, "MATCH (x:Probe {id:'x'}) RETURN x.id", nil)
						}
						runBoltQueryWithParams(t, labeller, relabel, nil)
						runBoltQueryWithParams(t, writer, statement, nil)
						switch order {
						case "relabel-last":
							requireNoError(t, nor213BoltCommitResult(t, writer))
							nor213RequireCountsMatchGraph(t, port, "after the relationship commit")
							// A durable label change may be refused after a
							// relationship delete at its node; it then persists
							// nothing.
							err := nor213BoltCommitResult(t, labeller)
							require.True(t, err == nil || change == "delete", "relabel commit: %v", err)
						case "relabel-first":
							requireNoError(t, nor213BoltCommitResult(t, labeller))
							nor213RequireCountsMatchGraph(t, port, "after the relabel commit")
							requireNoError(t, nor213BoltCommitResult(t, writer))
						default:
							nor213RequireCountsMatchGraph(t, port, "after the auto-commit relationship change")
							err := nor213BoltCommitResult(t, labeller)
							require.True(t, err == nil || change == "delete", "relabel commit: %v", err)
						}
						nor213RequireCountsMatchGraph(t, port, "after both commits")
					})
				}
			}
		}
	}
}

// TestBoltPositionalCountsInsideATransactionMatchItsGraph: a count by
// relationship type and end or start label inside a transaction includes the
// transaction's own changes, also when they leave every per-type and
// per-label count unchanged.
func TestBoltPositionalCountsInsideATransactionMatchItsGraph(t *testing.T) {
	cases := []struct {
		name       string
		statements []string
	}{
		{
			// One OTHER relationship ends at Held instead of After: the per-type
			// count and the label counts are unchanged.
			name: "same-type-moved-between-end-labels",
			statements: []string{
				"MATCH (:Probe)-[r:OTHER]->(:Held) DELETE r",
				"MATCH (x:Probe {id:'x'}) MATCH ()-[:OTHER]->(a:After) CREATE (x)-[:OTHER]->(a)",
			},
		},
		{
			// Two nodes trade labels: the label counts are unchanged.
			name: "balanced-relabel",
			statements: []string{
				nor213RelabelY,
				"MATCH ()-[:OTHER]->(a:After) REMOVE a:After SET a:Held",
			},
		},
		{
			name: "create-then-own-relabel",
			statements: []string{
				nor213LockAndCreate,
				"MATCH (y {id:'y'}) REMOVE y:Held SET y:Third",
			},
		},
	}
	for _, writeBehind := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("writeBehind=%v/%s", writeBehind, tc.name), func(t *testing.T) {
				port, _ := nor213RelationshipStore(t, writeBehind)
				setup := openBoltTestConn(t, port)
				defer setup.Close()
				runBoltQueryWithParams(t, setup, nor213CountSetup, nil)
				tx := openBoltTestConn(t, port)
				defer tx.Close()
				nor213BoltBegin(t, tx)
				for _, statement := range tc.statements {
					runBoltQueryWithParams(t, tx, statement, nil)
					nor213RequireCountsMatchGraphOn(t, tx, tc.name+": inside the transaction after "+statement)
				}
				requireNoError(t, nor213BoltCommitResult(t, tx))
				nor213RequireCountsMatchGraph(t, port, tc.name+": after commit")
			})
		}
	}
}

// nor213BoltCommitResult commits the explicit transaction conn holds and
// returns the server's outcome instead of failing the test.
func nor213BoltCommitResult(t *testing.T, conn net.Conn) error {
	t.Helper()
	if err := SendCommit(t, conn); err != nil {
		return err
	}
	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return err
	}
	if msgType != MsgSuccess {
		return nor213BoltOutcome(msgType, msgData)
	}
	return nil
}
