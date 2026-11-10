// SPDX-License-Identifier: MIT
package storage_test

// NOR136 (write-behind acknowledged-write integrity) public acceptance.
//
// These tests are the public-route acceptance slice of NOR136: the unflushed
// peer lost-write schedule, its disjoint-writer control, and concurrent
// increment accounting through an explicit flush and a reopen. They assert
// product behavior only through the real Execute, Bolt and HTTP transaction
// boundaries (pr1655OpenPublicClient) against a real Badger engine under a real
// server. Nothing here changes production code, storage formats or APIs.
//
// The schedule under test (NOR136 test matrix #1/#2/#4, historical review
// reproduction):
//
//  1. writer A stages SET n.note='loser' and is paused before its final commit;
//  2. peer B's autocommit SET n.other='peer' of the same node waits for A's
//     write lock (NOR-213); before, it was acknowledged into the write-behind
//     buffer here and A's commit was rejected as stale;
//  3. A resumes and commits; B resumes on A's commit, which a buffered
//     autocommit A leaves unflushed, and is acknowledged on top of it.
//
// Both acknowledged updates survive the flush. An acknowledged outcome whose
// update the flush then drops is the defect.
//
// Scheduling is explicit, not wall-clock: the explicit writer is paused by
// leaving its transaction open, the autocommit writer is paused by the
// production user-procedure registration seam (cypher.RegisterUserProcedure)
// that blocks the statement after its SET clause and before its commit, and the
// schedule fixture stops the existing write-behind worker so the peer's
// acknowledgement cannot be flushed away by the adaptive timer. time.After
// appears only as a failure guard for a schedule that never arrives, never as
// synchronization.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// nor136OriginalRead reads both properties the schedule writes.
	nor136OriginalRead = "MATCH (n:Doc {id:'original'}) RETURN n.note AS note, n.other AS other"
	// nor136WriterNote is the stale writer's staged update.
	nor136WriterNote = "MATCH (n:Doc {id:'original'}) SET n.note = 'loser'"
	// nor136PeerOther is the peer's autocommit update, acknowledged into the
	// write-behind buffer.
	nor136PeerOther = "MATCH (n:Doc {id:'original'}) SET n.other = 'peer'"
	// nor136AutocommitWriter runs the writer's SET in the same implicit
	// transaction as the pause procedure, so the SET is staged when the
	// statement blocks and its commit happens after the peer's acknowledgement.
	// WITH is the parser's required separator between an updating clause and
	// CALL, and YIELD is required for a procedure called inside a query.
	nor136AutocommitWriter = "MATCH (n:Doc {id:'original'}) SET n.note = 'loser' WITH n CALL nor136.pause(n.note) YIELD note RETURN note AS note"
	// nor136CounterSeed seeds the increment stress target.
	nor136CounterSeed = "CREATE (:Counter {id:'counter', n:0})"
	// nor136CounterIncrement is the stress read-modify-write.
	nor136CounterIncrement = "MATCH (c:Counter {id:'counter'}) SET c.n = c.n + 1"
	// nor136PauseProcedure is the test-only schedule seam name.
	nor136PauseProcedure = "nor136.pause"
	// nor136ScheduleFlushInterval pins the write-behind rotation delay for the
	// schedule tests: acknowledged commits stay buffered until an explicit
	// FlushWriteBehind or Close, so no timer moves the schedule.
	nor136ScheduleFlushInterval = time.Hour
)

// nor136PauseGate is the one-shot schedule seam driven by the registered
// procedure: the statement reports the value it staged and blocks until the
// test releases it.
type nor136PauseGate struct {
	reached chan string
	resume  chan struct{}
	once    sync.Once
}

func newNOR136PauseGate() *nor136PauseGate {
	return &nor136PauseGate{reached: make(chan string, 1), resume: make(chan struct{})}
}

// nor136RegisterPause publishes the pause procedure through the production
// user-procedure registration seam. It declares READ mode so it cannot write,
// and the handler only observes and blocks: no fake wire responses and no
// storage access.
func nor136RegisterPause(t *testing.T, gate *nor136PauseGate) {
	t.Helper()
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name:        nor136PauseProcedure,
		Signature:   "nor136.pause(note :: STRING) :: (note :: STRING)",
		Description: "Test-only NOR136 schedule seam: reports the staged value and blocks the statement before its commit.",
		Mode:        cypher.ProcedureModeRead,
		MinArgs:     1,
		MaxArgs:     1,
		Returns:     []cypher.ProcedureColumn{{Name: "note", Type: "STRING"}},
	}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, args []interface{}) (*cypher.ExecuteResult, error) {
		note, _ := args[0].(string)
		gate.once.Do(func() { gate.reached <- note })
		select {
		case <-gate.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &cypher.ExecuteResult{Columns: []string{"note"}, Rows: [][]interface{}{{note}}}, nil
	}))
	t.Cleanup(cypher.ClearUserProcedures)
}

// nor136ProtocolRoutes are the public transaction boundaries the acceptance
// matrix covers.
func nor136ProtocolRoutes() []string { return []string{"Execute", "Bolt", "HTTP"} }

// nor136Parsers logs the parser this process actually runs. The parser is
// process-wide (config.init), so both parsers are covered by separate
// NORNICDB_PARSER=<nornic|antlr> runs of the same tests.
func nor136Parsers(t *testing.T) {
	t.Helper()
	t.Logf("NOR136 parser=%s", config.GetParserType())
}

// nor136ReadOriginal reads both schedule properties through the public client.
func nor136ReadOriginal(t *testing.T, client *pr1655PublicClient) (note, other interface{}) {
	t.Helper()
	rows, err := client.run(nor136OriginalRead)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 2)
	return rows[0][0], rows[0][1]
}

// nor136RequireDurableOutcome is the NOR136 invariant, checked on the durable
// state after the peer's acknowledgement has been flushed: the peer's
// acknowledged update always survives, and the writer's staged update survives
// exactly when the writer was acknowledged. A writer that was rejected published
// nothing.
func nor136RequireDurableOutcome(t *testing.T, f *protocolServerFixture, id storage.NodeID, writerErr error, message string) {
	t.Helper()
	require.NoError(t, f.engine.FlushWriteBehind(context.Background()))
	props := storage.ReadPhysicalNodeForTest(t, f.engine, id).Properties
	require.Equal(t, "peer", props["other"], "%s: the peer's acknowledged autocommit update was lost after the flush: %v", message, props)
	if writerErr == nil {
		require.Equal(t, "loser", props["note"], "%s: the writer acknowledged its commit but its own update was lost after the flush: %v", message, props)
	} else {
		require.Equal(t, "seed", props["note"], "%s: a rejected writer published its staged update: %v", message, props)
	}
}

// TestNOR136UnflushedPeerKeepsAcknowledgedWrites is NOR136 matrix rows 1 and 2:
// the unflushed-peer schedule over Execute, Bolt and HTTP, for an explicit and
// an autocommit stale writer.
func TestNOR136UnflushedPeerKeepsAcknowledgedWrites(t *testing.T) {
	nor136Parsers(t)
	for _, route := range nor136ProtocolRoutes() {
		for _, producer := range []string{"explicit", "autocommit"} {
			t.Run(fmt.Sprintf("%s/%s", route, producer), func(t *testing.T) {
				f := openProtocolServerFixture(t, protocolServerOptions{writeBehind: true, flushInterval: nor136ScheduleFlushInterval, manualDrain: true})
				f.seed(t, "CREATE (:Doc {id:'original', note:'seed', other:'seed'})")
				originalID, idErr := pr1655SingleDocID(f.engine)
				require.NoError(t, idErr)

				writer := pr1655OpenPublicClient(t, f.server, route)
				peer := pr1655OpenPublicClient(t, f.server, route)

				gate := newNOR136PauseGate()
				var writerErr error
				var writerDone chan error
				switch producer {
				case "explicit":
					// The explicit transaction is its own pause: SET is staged
					// and nothing is published until COMMIT.
					require.NoError(t, writer.begin())
					_, err := writer.run(nor136WriterNote)
					require.NoError(t, err, "the writer must stage its update before the peer is acknowledged")
				case "autocommit":
					nor136RegisterPause(t, gate)
					writerDone = make(chan error, 1)
					go func() {
						_, err := writer.run(nor136AutocommitWriter)
						writerDone <- err
					}()
					select {
					case note := <-gate.reached:
						require.Equal(t, "loser", note, "the SET clause must run before the statement pauses")
					case err := <-writerDone:
						t.Fatalf("the autocommit writer finished before its commit boundary: %v", err)
					case <-time.After(30 * time.Second):
						t.Fatal("the autocommit writer never reached its commit boundary")
					}
				default:
					t.Fatalf("unknown producer %q", producer)
				}

				// The peer writes the node the writer still holds: it waits for
				// the writer's transaction to end (NOR-213), so nothing of it
				// is acknowledged or stored before then.
				waits := make(chan struct{}, 8)
				t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
					select {
					case waits <- struct{}{}:
					default:
					}
				}))
				peerDone := make(chan error, 1)
				go func() {
					_, err := peer.run(nor136PeerOther)
					peerDone <- err
				}()
				select {
				case <-waits:
				case err := <-peerDone:
					t.Fatalf("the peer did not wait for the uncommitted writer: %v", err)
				case <-time.After(30 * time.Second):
					t.Fatal("the peer never reached the writer's lock")
				}
				require.Equal(t, "seed", storage.ReadPhysicalNodeForTest(t, f.engine, originalID).Properties["other"],
					"the waiting peer's update was stored before the writer ended")

				switch producer {
				case "explicit":
					writerErr = writer.commit()
				case "autocommit":
					close(gate.resume)
					writerErr = <-writerDone
				}
				require.NoError(t, writerErr, "the writer holds the node: its commit is not a conflict")
				// The peer resumes on the writer's commit, still unflushed for
				// the buffered autocommit writer, and writes on top of it.
				require.NoError(t, <-peerDone, "the peer's autocommit update must be acknowledged")
				_, unflushed := nor136ReadOriginal(t, peer)
				require.Equal(t, "peer", unflushed,
					"the peer's acknowledged update must be served by the acknowledged read path")

				// Both acceptable outcomes are checked on the durable state, so
				// an acknowledged write may never be dropped by the flush.
				nor136RequireDurableOutcome(t, f, originalID, writerErr, "unflushed peer")

				// The public read surface must agree with the durable state.
				note, other := nor136ReadOriginal(t, peer)
				require.Equal(t, "peer", other)
				if writerErr == nil {
					require.Equal(t, "loser", note)
				} else {
					require.Equal(t, "seed", note)
				}
			})
		}
	}
}

// TestNOR136DisjointPeerKeepsBothAcknowledgedWrites is NOR136 matrix row 4: a
// peer's acknowledged buffered write to a different node must not conflict the
// writer and must survive alongside it.
func TestNOR136DisjointPeerKeepsBothAcknowledgedWrites(t *testing.T) {
	nor136Parsers(t)
	for _, route := range nor136ProtocolRoutes() {
		t.Run(route, func(t *testing.T) {
			f := openProtocolServerFixture(t, protocolServerOptions{writeBehind: true, flushInterval: nor136ScheduleFlushInterval, manualDrain: true})
			f.seed(t,
				"CREATE (:Doc {id:'original', note:'seed'})",
				"CREATE (:Doc {id:'peer-target', other:'seed'})",
			)
			writer := pr1655OpenPublicClient(t, f.server, route)
			peer := pr1655OpenPublicClient(t, f.server, route)

			require.NoError(t, writer.begin())
			_, err := writer.run(nor136WriterNote)
			require.NoError(t, err)
			peerID := nor136NodeIDByProperty(t, f.engine, "Doc", "id", "peer-target")
			_, err = peer.run("MATCH (n:Doc {id:'peer-target'}) SET n.other = 'peer'")
			require.NoError(t, err, "the disjoint peer write must be acknowledged")
			require.Equal(t, "seed", storage.ReadPhysicalNodeForTest(t, f.engine, peerID).Properties["other"],
				"the disjoint peer's acknowledgement was already flushed: the control was not established")
			rows, err := peer.run("MATCH (n:Doc {id:'peer-target'}) RETURN n.other AS other")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"peer"}}, rows,
				"the disjoint peer's acknowledged update must be served from the unflushed write-behind overlay")

			require.NoError(t, writer.commit(), "a disjoint acknowledged peer write must not conflict the writer")
			require.NoError(t, f.engine.FlushWriteBehind(context.Background()))

			rows, err = peer.run("MATCH (n:Doc {id:'original'}) RETURN n.note AS note")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"loser"}}, rows)
			rows, err = peer.run("MATCH (n:Doc {id:'peer-target'}) RETURN n.other AS other")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"peer"}}, rows)
		})
	}
}

// nor136Producer selects which transaction boundary the stress increments use.
type nor136Producer struct {
	name     string
	explicit func(worker int) bool
}

// nor136StressProducers are the increment producers the stress covers: the
// buffered autocommit path, the native explicit path, and both mixed.
func nor136StressProducers() []nor136Producer {
	return []nor136Producer{
		{name: "autocommit", explicit: func(int) bool { return false }},
		{name: "explicit", explicit: func(int) bool { return true }},
		{name: "mixed", explicit: func(worker int) bool { return worker%2 == 1 }},
	}
}

// TestNOR136ConcurrentIncrementStressKeepsAcknowledgedIncrements is NOR136
// matrix row 3: concurrent increments must retain every acknowledged increment,
// exactly, after an explicit flush and after a reopen. The write-behind-disabled
// run is the control that separates a write-behind schedule loss from any
// unrelated increment semantics, and a producer that cannot make progress inside
// its retry budget is reported as such (no silent truncation of the workload).
func TestNOR136ConcurrentIncrementStressKeepsAcknowledgedIncrements(t *testing.T) {
	nor136Parsers(t)
	for _, writeBehind := range []bool{false, true} {
		for _, producer := range nor136StressProducers() {
			name := fmt.Sprintf("writebehind=%t/%s", writeBehind, producer.name)
			t.Run(name, func(t *testing.T) {
				f := openProtocolServerFixture(t, protocolServerOptions{writeBehind: writeBehind})
				f.seed(t, nor136CounterSeed)

				const workers, increments = 6, 20
				clients := make([]*pr1655PublicClient, workers)
				for i := range clients {
					clients[i] = pr1655OpenPublicClient(t, f.server, "Execute")
				}

				var acknowledged atomic.Int64
				var firstConflict atomic.Value // error
				failures := make(chan error, workers)
				var wg sync.WaitGroup
				for i := 0; i < workers; i++ {
					explicit := producer.explicit(i)
					wg.Add(1)
					go func(client *pr1655PublicClient) {
						defer wg.Done()
						const attempts = 2000
						for success, tried := 0, 0; success < increments; tried++ {
							if tried >= attempts {
								failures <- fmt.Errorf("increment budget exhausted after %d attempts with %d/%d acknowledged (first conflict: %v)",
									tried, success, increments, firstConflict.Load())
								return
							}
							var err error
							if explicit {
								if err = client.begin(); err == nil {
									if _, err = client.run(nor136CounterIncrement); err == nil {
										err = client.commit()
									} else {
										client.rollback()
									}
								}
							} else {
								_, err = client.run(nor136CounterIncrement)
							}
							if err != nil {
								if errors.Is(err, storage.ErrConflict) {
									firstConflict.CompareAndSwap(nil, err)
									continue
								}
								failures <- fmt.Errorf("increment failed for a non-conflict reason: %w", err)
								return
							}
							// Acknowledged exactly once per successful outcome.
							acknowledged.Add(1)
							success++
						}
					}(clients[i])
				}
				wg.Wait()
				close(failures)
				for err := range failures {
					require.NoError(t, err)
				}

				expected := int64(workers * increments)
				require.Equal(t, expected, acknowledged.Load(), "every worker must report exactly its acknowledged increments")
				t.Logf("NOR136 %s: acknowledged=%d increments, no conflict loss", name, acknowledged.Load())

				require.NoError(t, f.engine.FlushWriteBehind(context.Background()))
				durable := nor136CounterValue(t, f.engine)
				t.Logf("NOR136 %s: acknowledged=%d durable_after_flush=%d", name, acknowledged.Load(), durable)
				require.Equal(t, expected, durable,
					"the durable counter lost an acknowledged increment after the flush")

				// Reopen: the acknowledged total must survive the store restart.
				f.close(t)
				f.start(t)
				reopened := nor136CounterValue(t, f.engine)
				t.Logf("NOR136 %s: durable_after_reopen=%d", name, reopened)
				require.Equal(t, expected, reopened,
					"the reopened store lost an acknowledged increment")
			})
		}
	}
}

// nor136NodeIDByProperty resolves a fixture identity through a label and property.
func nor136NodeIDByProperty(t *testing.T, engine *storage.BadgerEngine, label, property, key string) storage.NodeID {
	t.Helper()
	nodes, err := engine.GetNodesByLabel(context.Background(), label)
	require.NoError(t, err)
	for _, node := range nodes {
		if node.Properties[property] == key {
			return node.ID
		}
	}
	t.Fatalf("no %s node carries %s=%q", label, property, key)
	return ""
}

// nor136CounterValue reads the stored counter value from the native engine.
func nor136CounterValue(t *testing.T, engine *storage.BadgerEngine) int64 {
	t.Helper()
	nodes, err := engine.GetNodesByLabel(context.Background(), "Counter")
	require.NoError(t, err)
	require.Len(t, nodes, 1, "exactly one Counter node is expected")
	value, ok := nodes[0].Properties["n"].(int64)
	require.True(t, ok, "counter value must be an integer, got %T", nodes[0].Properties["n"])
	return value
}
