// Bolt wire end-to-end coverage for NOR-213's relationship reads under a
// node's write lock: once a transaction holds a node's write lock, its reads
// of that node's relationships see what other transactions committed, in the
// rest of the statement and in later statements, as its reads of the node's
// properties already do. A peer that only creates or deletes a relationship
// on the node, without writing the node itself, is seen too.
package bolt

import (
	"fmt"
	"net"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor213RelationshipReads reads node a's relationships in every form the
// report covers, after the statement's lock prefix: expand, both ends bound,
// directed typed, and (in nor213RelationshipSubqueryReads) EXISTS, COUNT and
// pattern comprehension.
const nor213RelationshipReads = "MATCH (a:Probe {id: $a}) %s WITH a MATCH (b:Probe {id: $b}) WITH a, b " +
	"OPTIONAL MATCH (a)-[r1]-() WITH a, b, count(r1) AS expand " +
	"OPTIONAL MATCH (a)-[r2]-(b) WITH a, b, expand, count(r2) AS bothBound " +
	"OPTIONAL MATCH (a)-[r3:LINK]->(b) WITH a, b, expand, bothBound, count(r3) AS directedTyped " +
	"RETURN expand, bothBound, directedTyped, a.touch"

const nor213RelationshipSubqueryReads = "MATCH (a:Probe {id: $a}), (b:Probe {id: $b}) " +
	"RETURN EXISTS { (a)--(b) }, COUNT { (a)--() }, size([(a)--() | 1])"

func nor213LockedReads(lock string) string {
	return fmt.Sprintf(nor213RelationshipReads, lock)
}

// nor213RequireRelationshipReads requires every read form of a to report
// want relationships to b (want is 0 or 1).
func nor213RequireRelationshipReads(t *testing.T, conn net.Conn, params map[string]any, lock string, want int64, what string) {
	t.Helper()
	rows := runBoltQueryWithParams(t, conn, nor213LockedReads(lock), params)
	require.Len(t, rows, 1, what)
	require.Equal(t, []any{want, want, want}, rows[0][:3], "%s: expand, both ends bound, directed typed", what)
	sub := runBoltQueryWithParams(t, conn, nor213RelationshipSubqueryReads, params)
	require.Equal(t, [][]any{{want == 1, want, want}}, sub, "%s: EXISTS, COUNT, pattern comprehension", what)
}

func nor213RelationshipStore(t *testing.T, writeBehind bool) (int, <-chan struct{}) {
	t.Helper()
	store := nor213BoltStore(t, writeBehind)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	waits := fixtureowner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 64)
	return port, waits
}

// TestBoltLockedNodeReadsCommittedRelationships: a transaction pins its
// snapshot, a peer commits a relationship change on a, and the transaction
// then takes a's write lock. Every read form of a's relationships after the
// lock, in the same statement and in a later one, sees the peer's commit.
func TestBoltLockedNodeReadsCommittedRelationships(t *testing.T) {
	locks := map[string]string{
		"plus-empty": "SET a += {}",
		"dummy":      "SET a._lock = true REMOVE a._lock",
		"property":   "SET a.mine = 1",
	}
	peers := []struct {
		name string
		seed string
		peer string
		want int64
	}{
		{"peer-creates-only", "", "MATCH (a:Probe {id: $a}), (b:Probe {id: $b}) CREATE (a)-[:LINK]->(b)", 1},
		{"peer-creates-and-sets-end-nodes", "", "MATCH (a:Probe {id: $a}), (b:Probe {id: $b}) SET a.touch = 1, b.touch = 1 CREATE (a)-[:LINK]->(b)", 1},
		{"peer-deletes-and-counts-other-end", "MATCH (a:Probe {id: $a}), (b:Probe {id: $b}) CREATE (a)-[:LINK]->(b)", "MATCH (:Probe {id: $a})-[r:LINK]->(b) DELETE r SET b.holders = coalesce(b.holders, 0) - 1", 0},
	}
	for _, engineShape := range []struct {
		name        string
		writeBehind bool
	}{{"durable", false}, {"write-behind", true}} {
		t.Run(engineShape.name, func(t *testing.T) {
			port, _ := nor213RelationshipStore(t, engineShape.writeBehind)
			setup := openBoltTestConn(t, port)
			for _, peer := range peers {
				for lockName, lock := range locks {
					key := peer.name + "-" + lockName
					params := map[string]any{"a": key + "-a", "b": key + "-b"}
					runBoltQueryWithParams(t, setup, "CREATE (:Probe {id: $a}), (:Probe {id: $b})", params)
					if peer.seed != "" {
						runBoltQueryWithParams(t, setup, peer.seed, params)
					}
					before := 1 - peer.want

					reader := openBoltTestConn(t, port)
					nor213BoltBegin(t, reader)
					nor213RequireRelationshipReads(t, reader, params, "", before, key+": the snapshot before the peer")

					control := openBoltTestConn(t, port)
					nor213BoltBegin(t, control)
					nor213RequireRelationshipReads(t, control, params, "", before, key+": the control's snapshot")

					runBoltQueryWithParams(t, setup, peer.peer, params)

					nor213RequireRelationshipReads(t, reader, params, lock, peer.want, key+": the locking statement")
					nor213RequireRelationshipReads(t, reader, params, "", peer.want, key+": a later statement")
					if !engineShape.writeBehind {
						// A transaction that never locks a keeps its begin snapshot.
						nor213RequireRelationshipReads(t, control, params, "", before, key+": a transaction without the lock")
					}
					nor213BoltCommit(t, reader)
					nor213BoltCommit(t, control)
					_ = reader.Close()
					_ = control.Close()
				}
			}
		})
	}
}

// TestBoltLockedNodeReadsRelationshipsCommittedWhileItWaited: the peer holds
// a's write lock while it creates the relationship; the reader's locking
// statement waits for it, and after the peer commits, every read form sees
// the relationship and the peer's property write.
func TestBoltLockedNodeReadsRelationshipsCommittedWhileItWaited(t *testing.T) {
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	for _, lock := range []string{"SET a += {}", "SET a._lock = true REMOVE a._lock"} {
		params := map[string]any{"a": lock + "-a", "b": lock + "-b"}
		runBoltQueryWithParams(t, setup, "CREATE (:Probe {id: $a}), (:Probe {id: $b})", params)

		reader := openBoltTestConn(t, port)
		nor213BoltBegin(t, reader)
		nor213RequireRelationshipReads(t, reader, params, "", 0, lock+": the snapshot")

		peer := openBoltTestConn(t, port)
		nor213BoltBegin(t, peer)
		runBoltQueryWithParams(t, peer, "MATCH (a:Probe {id: $a}), (b:Probe {id: $b}) SET a.touch = 1 CREATE (a)-[:LINK]->(b)", params)

		nor213DrainEntityWriteWaits(waits)
		type outcome struct {
			rows [][]any
			err  error
		}
		done := make(chan outcome, 1)
		go func() {
			rows, err := runBoltQueryRows(t, reader, nor213LockedReads(lock), params)
			done <- outcome{rows, err}
		}()
		select {
		case <-waits:
		case got := <-done:
			t.Fatalf("%s: the locking statement did not wait for the peer: %v %v", lock, got.rows, got.err)
		case <-time.After(nor213WireWaitLimit):
			t.Fatalf("%s: the locking statement neither waited nor finished", lock)
		}
		nor213BoltCommit(t, peer)
		var got outcome
		select {
		case got = <-done:
		case <-time.After(nor213WireWaitLimit):
			_ = reader.Close()
			t.Fatalf("%s: the waiting statement did not resume", lock)
		}
		require.NoError(t, got.err, lock)
		require.Equal(t, [][]any{{int64(1), int64(1), int64(1), int64(1)}}, got.rows, "%s: relationships and the refreshed property after the wait", lock)
		nor213RequireRelationshipReads(t, reader, params, "", 1, lock+": a later statement after the wait")
		nor213BoltCommit(t, reader)
		_ = reader.Close()
		_ = peer.Close()
	}
}

// TestBoltLockFirstGuardsOverRelationshipsApplyOnce: two transactions run the
// same guarded statement, each taking the write locks of both end nodes
// first. The second waits for the first, then its guard sees the first's
// relationship change, so the change and its counter apply once.
func TestBoltLockFirstGuardsOverRelationshipsApplyOnce(t *testing.T) {
	shapes := []struct {
		name   string
		seed   string
		guard  string
		verify string
		want   []any
	}{
		{
			name:   "holder-creates-and-counts",
			seed:   "CREATE (:Holder {id: $d}), (:Asset {id: $x, holders: 0})",
			guard:  "MATCH (d:Holder {id: $d}), (x:Asset {id: $x}) SET d += {}, x += {} WITH d, x WHERE NOT EXISTS { (d)-[:HAS]->(x) } CREATE (d)-[:HAS]->(x) SET x.holders = x.holders + 1 RETURN x.holders",
			verify: "MATCH (x:Asset {id: $x}) OPTIONAL MATCH (:Holder {id: $d})-[r:HAS]->(x) RETURN count(r), x.holders",
			want:   []any{int64(1), int64(1)},
		},
		{
			name:   "holder-deletes-and-counts",
			seed:   "CREATE (:Holder {id: $d})-[:HAS]->(:Asset {id: $x, holders: 1})",
			guard:  "MATCH (d:Holder {id: $d}), (x:Asset {id: $x}) SET d += {}, x += {} WITH d, x MATCH (d)-[r:HAS]->(x) DELETE r SET x.holders = x.holders - 1 RETURN x.holders",
			verify: "MATCH (x:Asset {id: $x}) OPTIONAL MATCH (:Holder {id: $d})-[r:HAS]->(x) RETURN count(r), x.holders",
			want:   []any{int64(0), int64(0)},
		},
		{
			name:   "creator-sets-end-nodes",
			seed:   "CREATE (:Holder {id: $d}), (:Asset {id: $x, holders: 0})",
			guard:  "MATCH (d:Holder {id: $d}), (x:Asset {id: $x}) SET d.seen = coalesce(d.seen, 0) + 1, x.seen = coalesce(x.seen, 0) + 1 WITH d, x WHERE NOT EXISTS { (d)-[:HAS]->(x) } CREATE (d)-[:HAS]->(x) SET x.holders = x.holders + 1 RETURN x.holders",
			verify: "MATCH (x:Asset {id: $x}) OPTIONAL MATCH (:Holder {id: $d})-[r:HAS]->(x) RETURN count(r), x.holders",
			want:   []any{int64(1), int64(1)},
		},
	}
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	for _, shape := range shapes {
		for _, timing := range []string{"committed-before", "waited"} {
			key := shape.name + "-" + timing
			params := map[string]any{"d": key + "-d", "x": key + "-x"}
			runBoltQueryWithParams(t, setup, shape.seed, params)

			first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
			nor213BoltBegin(t, first)
			nor213BoltBegin(t, second)
			// Both transactions read first, so both snapshots predate the change.
			runBoltQueryWithParams(t, first, shape.verify, params)
			runBoltQueryWithParams(t, second, shape.verify, params)
			runBoltQueryWithParams(t, first, shape.guard, params)

			var secondErr error
			if timing == "committed-before" {
				nor213BoltCommit(t, first)
				secondErr = nor213RunBoltStatement(t, second, shape.guard, params)
			} else {
				nor213DrainEntityWriteWaits(waits)
				done := make(chan error, 1)
				go func() { done <- nor213RunBoltStatement(t, second, shape.guard, params) }()
				select {
				case <-waits:
				case err := <-done:
					t.Fatalf("%s: the second guard did not wait: %v", key, err)
				case <-time.After(nor213WireWaitLimit):
					t.Fatalf("%s: the second guard neither waited nor finished", key)
				}
				nor213BoltCommit(t, first)
				secondErr = nor213AwaitBoltStatement(t, second, done, key+": the second guard")
			}
			require.NoError(t, secondErr, key)
			nor213BoltCommit(t, second)
			verify := openBoltTestConn(t, port)
			require.Equal(t, [][]any{shape.want}, runBoltQueryWithParams(t, verify, shape.verify, params), "%s: the change and its counter apply once", key)
			_ = verify.Close()
			_ = first.Close()
			_ = second.Close()
		}
	}
}

// runBoltQueryRows runs query on conn and returns its records without failing
// the test, so it can run off the test goroutine.
func runBoltQueryRows(t *testing.T, conn net.Conn, query string, params map[string]any) ([][]any, error) {
	t.Helper()
	if err := SendRun(t, conn, query, params, nil); err != nil {
		return nil, err
	}
	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return nil, err
	}
	if msgType != MsgSuccess {
		return nil, nor213BoltOutcome(msgType, msgData)
	}
	if err := SendPull(t, conn, nil); err != nil {
		return nil, err
	}
	var rows [][]any
	for {
		msgType, msgData, err = ReadMessage(conn)
		if err != nil {
			return nil, err
		}
		switch msgType {
		case MsgRecord:
			fields, _, err := decodePackStreamList(msgData, 0)
			if err != nil {
				return nil, err
			}
			rows = append(rows, fields)
		case MsgSuccess:
			return rows, nil
		default:
			return nil, nor213BoltOutcome(msgType, msgData)
		}
	}
}

// TestBoltLockedNodeReadsTheOtherEndOfANewRelationship: a peer creates a new
// node y and x's relationship to it, with properties on both, and commits.
// After the reader takes x's write lock, the relationship and the node at its
// other end are returned, with label and property predicates on y applied to
// y as committed. A peer that deleted the other end with its relationship
// leaves no row and no error.
func TestBoltLockedNodeReadsTheOtherEndOfANewRelationship(t *testing.T) {
	const (
		directed   = "MATCH (x:Probe {id: $a}) SET x += {} WITH x OPTIONAL MATCH (x)-[r:HAS]->(y:Held {key: $k}) RETURN y.key, y.holders, r.prop"
		undirected = "MATCH (x:Probe {id: $a}) SET x += {} WITH x OPTIONAL MATCH (x)-[r:HAS]-(y:Held) WHERE y.holders > 0 RETURN y.key, y.holders, r.prop"
		subqueries = "MATCH (x:Probe {id: $a}) RETURN EXISTS { (x)-[:HAS]->(:Held {key: $k}) }, COUNT { (x)-[:HAS]->(:Held) }"
		// snapshot reads without taking x's lock, so the peer can take it.
		snapshot = "MATCH (x:Probe {id: $a}) OPTIONAL MATCH (x)-[r:HAS]->(y:Held {key: $k}) RETURN y.key, y.holders, r.prop"
	)
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	for _, timing := range []string{"committed-before", "waited"} {
		params := map[string]any{"a": timing + "-x", "k": timing + "-held"}
		runBoltQueryWithParams(t, setup, "CREATE (:Probe {id: $a})", params)

		reader := openBoltTestConn(t, port)
		nor213BoltBegin(t, reader)
		require.Equal(t, [][]any{{nil, nil, nil}}, runBoltQueryWithParams(t, reader, snapshot, params), timing+": the snapshot")

		peer := openBoltTestConn(t, port)
		nor213BoltBegin(t, peer)
		runBoltQueryWithParams(t, peer, "MATCH (x:Probe {id: $a}) SET x += {} WITH x MERGE (y:Held {key: $k}) ON CREATE SET y.holders = 1 CREATE (x)-[:HAS {prop: 7}]->(y)", params)

		var rows [][]any
		if timing == "committed-before" {
			nor213BoltCommit(t, peer)
			rows = runBoltQueryWithParams(t, reader, directed, params)
		} else {
			nor213DrainEntityWriteWaits(waits)
			type outcome struct {
				rows [][]any
				err  error
			}
			done := make(chan outcome, 1)
			go func() {
				got, err := runBoltQueryRows(t, reader, directed, params)
				done <- outcome{got, err}
			}()
			select {
			case <-waits:
			case got := <-done:
				t.Fatalf("%s: the reader did not wait for the lock: %v %v", timing, got.rows, got.err)
			case <-time.After(nor213WireWaitLimit):
				t.Fatalf("%s: the reader neither waited nor finished", timing)
			}
			nor213BoltCommit(t, peer)
			select {
			case got := <-done:
				require.NoError(t, got.err, timing)
				rows = got.rows
			case <-time.After(nor213WireWaitLimit):
				_ = reader.Close()
				t.Fatalf("%s: the waiting reader did not resume", timing)
			}
		}
		want := [][]any{{params["k"], int64(1), int64(7)}}
		require.Equal(t, want, rows, "%s: the directed read returns the relationship and its new other end", timing)
		require.Equal(t, want, runBoltQueryWithParams(t, reader, undirected, params), timing+": the undirected read")
		require.Equal(t, [][]any{{true, int64(1)}}, runBoltQueryWithParams(t, reader, subqueries, params), timing+": EXISTS and COUNT with a label on the other end")
		nor213BoltCommit(t, reader)
		_ = reader.Close()
		_ = peer.Close()
	}

	t.Run("peer deleted the other end", func(t *testing.T) {
		params := map[string]any{"a": "deleted-x", "k": "deleted-held"}
		runBoltQueryWithParams(t, setup, "CREATE (:Probe {id: $a})-[:HAS {prop: 7}]->(:Held {key: $k, holders: 1})", params)
		reader := openBoltTestConn(t, port)
		nor213BoltBegin(t, reader)
		require.Equal(t, [][]any{{params["k"], int64(1), int64(7)}}, runBoltQueryWithParams(t, reader, snapshot, params), "the snapshot")
		runBoltQueryWithParams(t, setup, "MATCH (y:Held {key: $k}) DETACH DELETE y", params)
		require.Equal(t, [][]any{{nil, nil, nil}}, runBoltQueryWithParams(t, reader, directed, params), "the deleted other end leaves no row")
		require.Equal(t, [][]any{{false, int64(0)}}, runBoltQueryWithParams(t, reader, subqueries, params))
		nor213BoltCommit(t, reader)
		_ = reader.Close()
	})
}

// TestBoltLockFirstGuardOverANewOtherEndAppliesOnce: two transactions each
// lock x, then create x's relationship to the node with a key only when x
// has none yet. The first creates the node and the relationship; the second,
// waiting or after the commit, sees both and creates nothing.
func TestBoltLockFirstGuardOverANewOtherEndAppliesOnce(t *testing.T) {
	const guard = "MATCH (x:Probe {id: $a}) SET x += {} WITH x OPTIONAL MATCH (x)-[has:HAS]->(held:Held {key: $k}) " +
		"WITH x, count(held) AS holding WHERE holding = 0 " +
		"MERGE (h:Held {key: $k}) CREATE (x)-[:HAS]->(h) SET h.holders = coalesce(h.holders, 0) + 1 RETURN h.holders"
	const verify = "MATCH (h:Held {key: $k}) OPTIONAL MATCH (:Probe {id: $a})-[r:HAS]->(h) RETURN count(DISTINCT h), count(r), max(h.holders)"
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	for _, timing := range []string{"committed-before", "waited"} {
		params := map[string]any{"a": timing + "-x", "k": timing + "-held"}
		runBoltQueryWithParams(t, setup, "CREATE (:Probe {id: $a})", params)
		first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
		nor213BoltBegin(t, first)
		nor213BoltBegin(t, second)
		runBoltQueryWithParams(t, first, "MATCH (x:Probe {id: $a}) RETURN x.id", params)
		runBoltQueryWithParams(t, second, "MATCH (x:Probe {id: $a}) RETURN x.id", params)
		require.Equal(t, [][]any{{int64(1)}}, runBoltQueryWithParams(t, first, guard, params))
		var secondErr error
		if timing == "committed-before" {
			nor213BoltCommit(t, first)
			secondErr = nor213RunBoltStatement(t, second, guard, params)
		} else {
			nor213DrainEntityWriteWaits(waits)
			done := make(chan error, 1)
			go func() { done <- nor213RunBoltStatement(t, second, guard, params) }()
			select {
			case <-waits:
			case err := <-done:
				t.Fatalf("%s: the second guard did not wait: %v", timing, err)
			case <-time.After(nor213WireWaitLimit):
				t.Fatalf("%s: the second guard neither waited nor finished", timing)
			}
			nor213BoltCommit(t, first)
			secondErr = nor213AwaitBoltStatement(t, second, done, timing+": the second guard")
		}
		require.NoError(t, secondErr, timing)
		nor213BoltCommit(t, second)
		require.Equal(t, [][]any{{int64(1), int64(1), int64(1)}}, runBoltQueryWithParams(t, setup, verify, params), "%s: one node, one relationship, one count", timing)
		_ = first.Close()
		_ = second.Close()
	}
}

// TestBoltRelationshipMergeBetweenLockedNodesMatchesTheCommittedOne: two
// writers add the same pair. Each first takes both end nodes' write locks
// (MERGE of the keyed node and SET of its counter), then MERGEs the
// relationship between them, either in a later clause of the same statement
// or in a later statement. The second waits for the first's locks, and its
// relationship MERGE then matches the first's committed relationship: one
// relationship, counted twice.
func TestBoltRelationshipMergeBetweenLockedNodesMatchesTheCommittedOne(t *testing.T) {
	const (
		lockBoth = "UNWIND [$i, $t] AS k MERGE (h:Held {key: k}) SET h.holders = coalesce(h.holders, 0) + $delta"
		mergeRel = "MATCH (hi:Held {key: $i}) MATCH (ht:Held {key: $t}) MERGE (hi)-[co:COHELD]->(ht) SET co.n = coalesce(co.n, 0) + $delta RETURN co.n"
		oneStmt  = lockBoth + " WITH count(*) AS locked " + mergeRel
		verify   = "MATCH (hi:Held {key: $i})-[co:COHELD]->(ht:Held {key: $t}) RETURN count(co), sum(co.n), hi.holders, ht.holders"
	)
	port, waits := nor213RelationshipStore(t, false)
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT held_key FOR (h:Held) REQUIRE h.key IS UNIQUE", nil)
	for _, shape := range []struct {
		name       string
		statements []string
	}{
		{"same-statement", []string{oneStmt}},
		{"earlier-statement", []string{lockBoth, mergeRel}},
	} {
		for _, existing := range []bool{false, true} {
			for _, timing := range []string{"committed-before", "waited"} {
				key := fmt.Sprintf("%s-%v-%s", shape.name, existing, timing)
				params := map[string]any{"i": key + "-i", "t": key + "-t", "delta": int64(1)}
				if existing {
					runBoltQueryWithParams(t, setup, "CREATE (:Held {key: $i}), (:Held {key: $t})", params)
				}
				first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
				nor213BoltBegin(t, first)
				nor213BoltBegin(t, second)
				runBoltQueryWithParams(t, first, "MATCH (n:Held) RETURN count(n)", nil)
				runBoltQueryWithParams(t, second, "MATCH (n:Held) RETURN count(n)", nil)
				for _, statement := range shape.statements {
					runBoltQueryWithParams(t, first, statement, params)
				}
				run := func() error {
					for _, statement := range shape.statements {
						if err := nor213RunBoltStatement(t, second, statement, params); err != nil {
							return err
						}
					}
					return nil
				}
				var secondErr error
				if timing == "committed-before" {
					nor213BoltCommit(t, first)
					secondErr = run()
				} else {
					nor213DrainEntityWriteWaits(waits)
					done := make(chan error, 1)
					go func() { done <- run() }()
					select {
					case <-waits:
					case err := <-done:
						t.Fatalf("%s: the second writer did not wait: %v", key, err)
					case <-time.After(nor213WireWaitLimit):
						t.Fatalf("%s: the second writer neither waited nor finished", key)
					}
					nor213BoltCommit(t, first)
					secondErr = nor213AwaitBoltStatement(t, second, done, key+": the second writer")
				}
				require.NoError(t, secondErr, key)
				nor213BoltCommit(t, second)
				require.Equal(t, [][]any{{int64(1), int64(2), int64(2), int64(2)}}, runBoltQueryWithParams(t, setup, verify, params), "%s: one relationship, counted twice", key)
				_ = first.Close()
				_ = second.Close()
			}
		}
	}
}
