// Bolt wire reproductions of NOR-213's relationship absence guard on the
// public protocol: a transaction whose snapshot predates another
// transaction's committed relationship can still create a duplicate of it
// and count it a second time.
//
// Each case is one of the report's forms and timings, with the outcome
// Neo4j 5.26.30 gave for the same schedule:
//
//   - form 2, the guard alone, after the first transaction committed: the
//     second statement returns no row and the graph keeps one relationship
//     and a count of one;
//   - form 8, the end node's write lock taken before the guard, both
//     timings: no row, one relationship, a count of one;
//   - form 10, the guard evaluated before both end nodes' write locks,
//     committed before: no row, one relationship, a count of one; waiting:
//     Neo4j reports DeadlockDetected; NornicDB reports its genuine retryable
//     Outdated conflict (approved compatibility contract), and the second
//     transaction rolled back, one relationship, a count of one.
//
// Both transactions begin and read the pair before the first one writes, as
// the reported schedule does, so both snapshots predate the change. The
// waiting timings are ordered by the storage layer's entity lock wait hook,
// never by a sleep. Each case records the rows and the error of the guarded
// statement, whether it waited, its commit outcome, and the committed graph
// read on a fresh connection, so a disagreement reports the whole schedule.
package bolt

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The statements of the reported schedule: the pair both transactions start
// from, the absence guard, the first transaction's create and count, and the
// forms the second transaction runs. Form 8 takes the end node's write lock
// first (SET x += {}), form 10 takes the guard first and then both end nodes'
// write locks (SET d += {}, x += {}).
const (
	nor213AbsenceSeed     = "CREATE (:Holder {id: $d}), (:Asset {id: $x, holders: 0})"
	nor213AbsencePair     = "MATCH (d:Holder {id: $d}), (x:Asset {id: $x}) "
	nor213AbsenceGuard    = "WHERE NOT EXISTS { (d)-[:HAS]->(x) } "
	nor213AbsenceCreate   = "CREATE (d)-[:HAS]->(x) SET x.holders = x.holders + 1 RETURN x.holders"
	nor213AbsencePeer     = nor213AbsencePair + nor213AbsenceCreate
	nor213AbsenceForm2    = nor213AbsencePair + nor213AbsenceGuard + nor213AbsenceCreate
	nor213AbsenceForm8    = nor213AbsencePair + "SET x += {} WITH d, x " + nor213AbsenceGuard + nor213AbsenceCreate
	nor213AbsenceForm10   = nor213AbsencePair + nor213AbsenceGuard + "SET d += {}, x += {} WITH d, x " + nor213AbsenceCreate
	nor213AbsenceSnapshot = nor213AbsencePair + "OPTIONAL MATCH (d)-[r:HAS]->(x) RETURN count(r), x.holders"
	nor213AbsenceVerify   = "MATCH (x:Asset {id: $x}) OPTIONAL MATCH (:Holder {id: $d})-[r:HAS]->(x) RETURN count(r), x.holders"
)

// The two timings of the schedule: the first transaction commits before the
// second sends its statement, or the second sends it while the first is open.
const (
	nor213AbsenceCommittedBefore = "committed-before"
	nor213AbsenceWaited          = "waited"
)

// nor213AbsenceForm is one form of the report and one timing of it: the
// statement the second transaction runs, when the first commits relative to
// it, and the FAILURE code its statement must report, if any.
type nor213AbsenceForm struct {
	name      string
	statement string
	timing    string
	// failure is the FAILURE code the guarded statement must report, and
	// whose transaction must then be rolled back instead of committed.
	failure        string
	extraSeed      string
	allowDuplicate bool
}

// TestBoltRelationshipAbsenceGuardReproductions runs the report's absence
// guard forms over two explicit transactions on the Bolt wire and requires the
// outcome Neo4j 5.26.30 gave for each: the guard sees the committed
// relationship, so it creates nothing, and the graph keeps one relationship
// and a count of one.
func TestBoltRelationshipAbsenceGuardReproductions(t *testing.T) {
	forms := []nor213AbsenceForm{
		{name: "form2-guard-committed-before", statement: nor213AbsenceForm2, timing: nor213AbsenceCommittedBefore},
		{name: "form8-end-lock-then-guard-committed-before", statement: nor213AbsenceForm8, timing: nor213AbsenceCommittedBefore},
		{name: "form8-end-lock-then-guard-waited", statement: nor213AbsenceForm8, timing: nor213AbsenceWaited},
		{name: "form10-guard-then-both-locks-committed-before", statement: nor213AbsenceForm10, timing: nor213AbsenceCommittedBefore},
		{
			name:      "form10-guard-then-both-locks-waited",
			statement: nor213AbsenceForm10,
			timing:    nor213AbsenceWaited,
			failure:   "Neo.TransientError.Transaction.Outdated",
		},
		{
			name:      "guard-both-locks-with-unrelated-same-type-edge",
			statement: nor213AbsenceForm10,
			timing:    nor213AbsenceWaited,
			failure:   "Neo.TransientError.Transaction.Outdated",
			extraSeed: "MATCH (d:Holder {id:$d}) CREATE (d)-[:HAS]->(:Asset {id:'unrelated'})",
		},
		{name: "incoming-guard-committed-before", statement: nor213AbsencePair + "WHERE NOT EXISTS { (x)<-[:HAS]-(d) } " + nor213AbsenceCreate, timing: nor213AbsenceCommittedBefore},
		{name: "incoming-guard-both-locks-waited", statement: nor213AbsencePair + "WHERE NOT EXISTS { (x)<-[:HAS]-(d) } SET d += {}, x += {} WITH d,x " + nor213AbsenceCreate, timing: nor213AbsenceWaited, failure: "Neo.TransientError.Transaction.Outdated"},
		{name: "undirected-guard-both-locks-waited", statement: nor213AbsencePair + "WHERE NOT EXISTS { (d)-[:HAS]-(x) } SET d += {}, x += {} WITH d,x " + nor213AbsenceCreate, timing: nor213AbsenceWaited, failure: "Neo.TransientError.Transaction.Outdated"},
		// The reference permits both creates when this guard runs before the
		// peer commits and never write-locks the guard's anchor.
		{name: "unowned-guard-wait-retains-read-committed-behavior", statement: nor213AbsenceForm2, timing: nor213AbsenceWaited, allowDuplicate: true},
	}
	port, waits := nor213RelationshipStore(t, false)
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			nor213RunAbsenceForm(t, port, waits, form)
		})
	}
}

func TestMutationGuardResolvesAnEndpointCreatedAfterBegin(t *testing.T) {
	port, _ := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	t.Cleanup(func() { _ = setup.Close() })
	runBoltQueryWithParams(t, setup, "CREATE (:Holder {id:'observed-holder'})", nil)
	reader := openBoltTestConn(t, port)
	t.Cleanup(func() { _ = reader.Close() })
	nor213BoltBegin(t, reader)
	require.Len(t, runBoltQueryWithParams(t, reader, "MATCH (d:Holder {id:'observed-holder'}) RETURN d.id", nil), 1)
	runBoltQueryWithParams(t, setup, "MATCH (d:Holder {id:'observed-holder'}) CREATE (d)-[:HAS]->(:Asset {id:'new-asset'})", nil)
	rows, err := runBoltQueryRows(t, reader, "MATCH (d:Holder {id:'observed-holder'}) WHERE NOT EXISTS { (d)-[:HAS]->(:Asset) } SET d.claimed=true RETURN d.id", nil)
	require.NoError(t, err)
	require.Empty(t, rows, "the mutation must resolve the current relationship's new endpoint before deciding absence")
	nor213BoltCommit(t, reader)
}

// nor213RunAbsenceForm runs one form and timing: it seeds the pair, opens both
// explicit transactions and reads the pair in each of them, has the first
// transaction create the relationship and count it, then runs the second
// transaction's statement according to the timing. It records the guarded
// statement's rows, its error and its commit outcome, and then reads the
// committed graph on a fresh connection before requiring the schedule's
// outcome.
func nor213RunAbsenceForm(t *testing.T, port int, waits <-chan struct{}, form nor213AbsenceForm) {
	t.Helper()
	params := map[string]any{"d": form.name + "-d", "x": form.name + "-x"}
	setup := openBoltTestConn(t, port)
	defer func() { _ = setup.Close() }()
	runBoltQueryWithParams(t, setup, nor213AbsenceSeed, params)
	if form.extraSeed != "" {
		runBoltQueryWithParams(t, setup, form.extraSeed, params)
	}

	first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
	defer func() { _ = first.Close() }()
	defer func() { _ = second.Close() }()
	nor213BoltBegin(t, first)
	nor213BoltBegin(t, second)
	// Both transactions read the pair before the first writes, so both
	// snapshots predate the relationship.
	require.Equal(t, [][]any{{int64(0), int64(0)}}, runBoltQueryWithParams(t, first, nor213AbsenceSnapshot, params),
		form.name+": the first transaction's snapshot")
	require.Equal(t, [][]any{{int64(0), int64(0)}}, runBoltQueryWithParams(t, second, nor213AbsenceSnapshot, params),
		form.name+": the second transaction's snapshot")
	require.Equal(t, [][]any{{int64(1)}}, runBoltQueryWithParams(t, first, nor213AbsencePeer, params),
		form.name+": the first transaction creates the relationship and counts it")

	var (
		rows         [][]any
		statementErr error
		waited       bool
	)
	if form.timing == nor213AbsenceCommittedBefore {
		nor213BoltCommit(t, first)
		rows, statementErr = runBoltQueryRows(t, second, form.statement, params)
	} else {
		rows, statementErr, waited = nor213RunAbsenceWhileHeld(t, first, second, waits, form, params)
		t.Logf("%s: the guarded statement waited at the entity lock: %v", form.name, waited)
	}
	t.Logf("%s: the guarded statement returned rows %v and error %v", form.name, rows, statementErr)

	var commitErr error
	if statementErr == nil {
		commitErr = nor213BoltCommitResult(t, second)
	} else {
		// A failed statement ends the transaction: RESET rolls it back.
		requireNoError(t, SendReset(t, second))
		_, _, _ = ReadMessage(second)
	}
	t.Logf("%s: the second transaction's commit outcome: %v", form.name, commitErr)

	verify := openBoltTestConn(t, port)
	final := runBoltQueryWithParams(t, verify, nor213AbsenceVerify, params)
	_ = verify.Close()
	t.Logf("%s: the committed graph after both transactions: %v", form.name, final)

	expected := int64(1)
	if form.allowDuplicate {
		expected = 2
	}
	require.Equal(t, [][]any{{expected, expected}}, final,
		"%s: approved committed relationship and counter outcome", form.name)
	if form.timing == nor213AbsenceWaited {
		require.True(t, waited, "%s: the guarded statement must reach the entity lock wait", form.name)
	}
	if form.failure != "" {
		require.Error(t, statementErr, "%s: the guarded statement must fail with %s", form.name, form.failure)
		require.Contains(t, statementErr.Error(), form.failure, "%s: the guarded statement's FAILURE", form.name)
		return
	}
	require.NoError(t, statementErr, "%s: the guarded statement", form.name)
	if form.allowDuplicate {
		require.Equal(t, [][]any{{int64(2)}}, rows, "a guard without anchor write ownership retains the reference's concurrent-create behavior")
	} else {
		require.Empty(t, rows, "%s: the guard must see the committed relationship and create nothing", form.name)
	}
	require.NoError(t, commitErr, "%s: the second transaction's commit", form.name)
}

// nor213RunAbsenceWhileHeld sends the second transaction's statement while the
// first transaction still holds the pair's write locks, waits for the storage
// layer's entity lock wait - not for a duration - then commits the first
// transaction and returns what the guarded statement produced, and whether it
// waited at all.
func nor213RunAbsenceWhileHeld(t *testing.T, first, second net.Conn, waits <-chan struct{}, form nor213AbsenceForm, params map[string]any) ([][]any, error, bool) {
	t.Helper()
	type outcome struct {
		rows [][]any
		err  error
	}
	nor213DrainEntityWriteWaits(waits)
	done := make(chan outcome, 1)
	go func() {
		got, gotErr := runBoltQueryRows(t, second, form.statement, params)
		done <- outcome{got, gotErr}
	}()
	select {
	case <-waits:
	case got := <-done:
		// The statement ended before it reached the lock wait: the first
		// transaction is still open, so commit it for the schedule to
		// complete and let the caller report the missing wait.
		nor213BoltCommit(t, first)
		return got.rows, got.err, false
	case <-time.After(nor213WireWaitLimit):
		_ = second.Close()
		t.Fatalf("%s: the guarded statement neither waited nor finished within %s", form.name, nor213WireWaitLimit)
		return nil, nil, false
	}
	nor213BoltCommit(t, first)
	select {
	case got := <-done:
		return got.rows, got.err, true
	case <-time.After(nor213WireWaitLimit):
		_ = second.Close()
		t.Fatalf("%s: the guarded statement did not resume after the first transaction committed", form.name)
		return nil, nil, true
	}
}
