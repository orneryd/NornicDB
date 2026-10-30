// SPDX-License-Identifier: MIT
package storage

// PR1655 current-release acceptance reproductions (fail-first).
//
// A writer stages a write against a target that exists when it stages it. A
// peer then deletes that target and the deletion is acknowledged (write-behind
// buffered) or published (synchronous engine write / peer transaction) before
// the writer commits. At commit the writer must:
//
//   - fail with the retryable storage.ErrConflict,
//   - publish nothing of its own (its marker node, a staged relationship),
//   - revive no relationship the peer deleted, and
//   - leave no relationship whose endpoint the peer deleted.
//
// The controls below pin the surrounding semantics so a reproduction cannot
// pass by changing the product's meaning: staging against a target deleted
// before the transaction began, or one that never existed, keeps the ordinary
// missing-target behavior (ErrNotFound at statement time, or the commit's
// concurrent-modification conflict when the absence is only visible at
// commit), and the writer's own deletion keeps its existing semantics.
//
// Every engine is built with the canonical test-fixture owner and under an
// isolated t.TempDir; no global state, no wall-clock races.

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// pr1655AcceptanceScenario names one staged-target shape.
type pr1655AcceptanceScenario struct {
	name string
	// nodes are the seeded endpoints; the peer deletion removes a target the
	// writer's staged write depends on.
	nodes [2]NodeID
	// peerDelete removes the vanished target as the peer: the relationship for
	// the relationship shape, the start node for the endpoint shapes.
	peerDelete func(t *testing.T, b *BadgerEngine, peer *BadgerTransaction)
	// stage stages the writer's write against the still-visible target.
	stage func(t *testing.T, writer *BadgerTransaction)
}

// pr1655AcceptanceScenarios are the shapes PR1655 is about: a relationship
// update whose relationship the peer deletes, a node update whose node the
// peer deletes, and a new relationship whose endpoint the peer deletes.
func pr1655AcceptanceScenarios() []pr1655AcceptanceScenario {
	return []pr1655AcceptanceScenario{
		{
			name:  "relationship-update/relationship-deleted",
			nodes: [2]NodeID{"test:pr1655-a", "test:pr1655-b"},
			peerDelete: func(t *testing.T, b *BadgerEngine, peer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, peer.DeleteEdge("test:pr1655-r"))
			},
			stage: func(t *testing.T, writer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, writer.UpdateEdge(EdgeUpdateRequest{After: &Edge{
					ID:         "test:pr1655-r",
					StartNode:  "test:pr1655-a",
					EndNode:    "test:pr1655-b",
					Type:       "R",
					Properties: map[string]any{"note": "writer"},
				}}))
			},
		},
		{
			name:  "node-update/node-deleted",
			nodes: [2]NodeID{"test:pr1655-a", "test:pr1655-b"},
			peerDelete: func(t *testing.T, b *BadgerEngine, peer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, peer.DeleteGraphNode("test:pr1655-a", CascadeEdges))
			},
			stage: func(t *testing.T, writer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, writer.UpdateGraphNode(NodeUpdateRequest{After: &Node{
					ID:         "test:pr1655-a",
					Labels:     []string{"Node"},
					Properties: map[string]any{"note": "writer"},
				}, Policy: ClientNodeEdits}))
			},
		},
		{
			name:  "new-edge/endpoint-deleted",
			nodes: [2]NodeID{"test:pr1655-a", "test:pr1655-b"},
			peerDelete: func(t *testing.T, b *BadgerEngine, peer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, peer.DeleteGraphNode("test:pr1655-a", CascadeEdges))
			},
			stage: func(t *testing.T, writer *BadgerTransaction) {
				t.Helper()
				require.NoError(t, writer.CreateEdge(&Edge{
					ID:        "test:pr1655-new",
					StartNode: "test:pr1655-b",
					EndNode:   "test:pr1655-a",
					Type:      "R",
				}))
			},
		},
	}
}

// pr1655AcceptancePeer names how the peer's deletion reaches the writer.
type pr1655AcceptancePeer struct {
	name string
	// implicit marks the peer as an implicit (autocommit) transaction, which
	// is the only shape the write-behind buffer takes.
	implicit bool
	// direct deletes through the engine itself instead of a transaction.
	direct bool
}

func pr1655AcceptancePeers() []pr1655AcceptancePeer {
	return []pr1655AcceptancePeer{
		{name: "direct", direct: true},
		{name: "implicit", implicit: true},
		{name: "explicit"},
	}
}

// pr1655AcceptanceFlushes are the points the peer's acknowledgement is made
// durable before the writer commits: never (left in the write-behind buffer)
// and after an explicit flush.
func pr1655AcceptanceFlushes() []bool { return []bool{false, true} }

// openPR1655AcceptanceEngine builds the canonical write-behind fixture: an
// isolated on-disk engine whose background flusher runs hourly, so only an
// explicit FlushWriteBehind (or a Close) lands an acknowledged buffered
// commit, and no timer can move the schedule.
func openPR1655AcceptanceEngine(t *testing.T, writeBehind bool) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		LowMemory:           true,
		WriteBehind:         writeBehind,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	return engine
}

func TestPR1655StagedTargetRevalidatedAfterMetadataCallback(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			t.Run("writebehind="+boolString(writeBehind)+"/implicit="+boolString(implicit), func(t *testing.T) {
				barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
				b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
					DataDir: t.TempDir(), LowMemory: true, WriteBehind: writeBehind, WriteBehindInterval: time.Hour, Logger: slog.New(barrier),
				}))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				scenario := pr1655AcceptanceScenarios()[2]
				seedPR1655AcceptanceGraph(t, b, scenario.nodes)
				writer := beginPR1655AcceptanceWriter(t, b)
				require.NoError(t, writer.SetImplicit(implicit))
				marker := NodeID("test:pr1655-marker")
				_, err = writer.CreateNode(&Node{ID: marker, Labels: []string{"Marker"}})
				require.NoError(t, err)
				scenario.stage(t, writer)
				require.NoError(t, writer.SetMetadata(map[string]interface{}{"validation_publication_control": true}))
				barrier.target = writer.ID
				var once sync.Once
				release := func() { once.Do(func() { close(barrier.resume) }) }
				t.Cleanup(release)
				done := make(chan error, 1)
				go func() { done <- writer.Commit() }()
				select {
				case <-barrier.reached:
				case err := <-done:
					t.Fatalf("writer completed before the metadata boundary: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("writer did not reach the metadata boundary")
				}
				require.NoError(t, b.DeleteNode("test:pr1655-a"))
				release()
				select {
				case err := <-done:
					require.ErrorIs(t, err, ErrConflict, "final validation must not reuse an endpoint head from before the callback")
				case <-time.After(5 * time.Second):
					t.Fatal("writer did not finish after the callback")
				}
				require.NoError(t, b.FlushWriteBehind())
				assertPR1655AcceptanceGraph(t, b, scenario, marker, "after metadata interposed deletion")
			})
		}
	}
}

// seedPR1655AcceptanceGraph writes the scenario's endpoints and the
// relationship the relationship shape updates, as the latest truth.
func seedPR1655AcceptanceGraph(t *testing.T, b *BadgerEngine, nodes [2]NodeID) {
	t.Helper()
	for _, id := range nodes {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Node"}, Properties: map[string]any{"note": "old"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{
		ID:         "test:pr1655-r",
		StartNode:  nodes[0],
		EndNode:    nodes[1],
		Type:       "R",
		Properties: map[string]any{"note": "old"},
	}))
}

// beginPR1655AcceptanceWriter opens the writer transaction pinned to the
// "test" namespace before any staging, so its read version is the begin-time
// snapshot in every producer path.
func beginPR1655AcceptanceWriter(t *testing.T, b *BadgerEngine) *BadgerTransaction {
	t.Helper()
	writer, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback() })
	require.NoError(t, writer.SetNamespace("test"))
	return writer
}

// peerPR1655Acceptance applies the scenario's deletion through the named peer
// producer and returns the error that producer reported.
func peerPR1655Acceptance(t *testing.T, b *BadgerEngine, scenario pr1655AcceptanceScenario, peer pr1655AcceptancePeer) error {
	t.Helper()
	if peer.direct {
		switch scenario.name {
		case "relationship-update/relationship-deleted":
			return b.DeleteEdge("test:pr1655-r")
		default:
			return b.DeleteNode("test:pr1655-a")
		}
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(peer.implicit))
	scenario.peerDelete(t, b, tx)
	return tx.Commit()
}

// pr1655AcceptanceStoredEdge reads a relationship body from storage, ignoring
// the write-behind overlay, without filling an engine-wide read view.
func pr1655AcceptanceStoredEdge(t *testing.T, b *BadgerEngine, id EdgeID) (*Edge, error) {
	t.Helper()
	var edge *Edge
	var readErr error
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		var found bool
		edge, found, readErr = b.readEdgeBodyInTxn(txn, id, storedRead)
		if readErr == nil && !found {
			readErr = ErrNotFound
		}
		return nil
	}))
	return edge, readErr
}

// pr1655AcceptanceStoredNodeExists reports whether storage holds a live body
// for a node, ignoring the write-behind overlay.
func pr1655AcceptanceStoredNodeExists(t *testing.T, b *BadgerEngine, id NodeID) (bool, error) {
	t.Helper()
	var found bool
	var readErr error
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			readErr = err
			return nil
		}
		found = item.Value(func([]byte) error { return nil }) == nil
		return nil
	}))
	return found, readErr
}

// assertPR1655AcceptanceGraph asserts the peer's deletion is the only truth:
// the vanished target stays deleted, the writer's marker never appears, and
// no relationship references a deleted endpoint.
func assertPR1655AcceptanceGraph(t *testing.T, b *BadgerEngine, scenario pr1655AcceptanceScenario, marker NodeID, message string) {
	t.Helper()

	// The writer's own staged relationship, if any, was not created.
	_, err := pr1655AcceptanceStoredEdge(t, b, "test:pr1655-new")
	require.ErrorIs(t, err, ErrNotFound, "%s: the failed commit created test:pr1655-new", message)

	// No relationship survives the peer's cascade or refers to a deleted
	// endpoint: scanning every stored relationship is the dangling-edge check.
	edges, err := b.GetStoredEdgesByType("")
	require.NoError(t, err)
	for _, edge := range edges {
		for _, endpoint := range []NodeID{edge.StartNode, edge.EndNode} {
			exists, err := pr1655AcceptanceStoredNodeExists(t, b, endpoint)
			require.NoError(t, err)
			require.True(t, exists, "%s: relationship %s (%s -> %s) is dangling on %s",
				message, edge.ID, edge.StartNode, edge.EndNode, endpoint)
		}
		if edge.ID == "test:pr1655-r" && scenario.name == "relationship-update/relationship-deleted" {
			t.Fatalf("%s: the deleted relationship test:pr1655-r was revived", message)
		}
	}

	// The vanished node was not resurrected by the writer's staged body.
	if scenario.name != "relationship-update/relationship-deleted" {
		exists, err := pr1655AcceptanceStoredNodeExists(t, b, "test:pr1655-a")
		require.NoError(t, err)
		require.False(t, exists, "%s: the deleted node test:pr1655-a was resurrected", message)
	}

	// The writer published nothing of its own.
	exists, err := pr1655AcceptanceStoredNodeExists(t, b, marker)
	require.NoError(t, err)
	require.False(t, exists, "%s: the failed commit published earlier marker %s", message, marker)
	_, err = b.GetNode(marker)
	require.ErrorIs(t, err, ErrNotFound, "%s: the failed commit published earlier marker %s", message, marker)
}

// TestPR1655StagedTargetVanishedBeforeCommit is the reproduction: for every
// producer (write-behind on and off), peer kind, flush point and shape, a
// target vanished after staging fails the writer's commit as a retryable
// conflict and publishes nothing.
func TestPR1655StagedTargetVanishedBeforeCommit(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, peer := range pr1655AcceptancePeers() {
			if peer.implicit && !writeBehind {
				// An implicit commit without the write-behind buffer is the
				// synchronous autocommit path the "direct" peer already covers.
				continue
			}
			for _, flush := range pr1655AcceptanceFlushes() {
				if flush && !writeBehind {
					// Nothing is buffered without the write-behind buffer.
					continue
				}
				for _, scenario := range pr1655AcceptanceScenarios() {
					name := scenario.name + "/writebehind=" + boolString(writeBehind) + "/peer=" + peer.name + "/flush=" + boolString(flush)
					t.Run(name, func(t *testing.T) {
						b := openPR1655AcceptanceEngine(t, writeBehind)
						seedPR1655AcceptanceGraph(t, b, scenario.nodes)
						if !writeBehind {
							// The writer's begin snapshot is the seeded graph.
							require.NoError(t, b.FlushWriteBehind())
						}

						writer := beginPR1655AcceptanceWriter(t, b)
						marker := NodeID("test:pr1655-marker")
						_, err := writer.CreateNode(&Node{ID: marker, Labels: []string{"Marker"}})
						require.NoError(t, err)
						scenario.stage(t, writer)

						require.NoError(t, peerPR1655Acceptance(t, b, scenario, peer),
							"the peer's deletion must be acknowledged")
						if flush {
							require.NoError(t, b.FlushWriteBehind())
						}

						err = writer.Commit()
						require.Error(t, err, "a staged target vanished after staging")
						require.ErrorIs(t, err, ErrConflict,
							"the commit must fail as a retryable concurrent modification")

						// The failed commit's own writes never reach storage,
						// buffered or not.
						_, err = b.GetNode(marker)
						require.ErrorIs(t, err, ErrNotFound, "the failed commit published its marker")
						_, err = b.GetEdge("test:pr1655-new")
						require.ErrorIs(t, err, ErrNotFound, "the failed commit created a relationship")

						// Draining the buffer replays the peer's deletion alone.
						require.NoError(t, b.FlushWriteBehind())
						assertPR1655AcceptanceGraph(t, b, scenario, marker,
							"after draining the write-behind buffer")
					})
				}
			}
		}
	}
}

// TestPR1655StagedTargetDeletePublishedAfterCommitAllocation reproduces the
// other half of the window: the peer's deletion has taken its MVCC version
// before the writer begins, but publishes only after the writer staged its
// write. The writer's commit must still fail as a retryable conflict and
// revive nothing.
func TestPR1655StagedTargetDeletePublishedAfterCommitAllocation(t *testing.T) {
	for _, writeBehind := range []bool{true, false} {
		for _, scenario := range pr1655AcceptanceScenarios() {
			t.Run(scenario.name+"/writebehind="+boolString(writeBehind), func(t *testing.T) {
				b := openPR1655AcceptanceEngine(t, writeBehind)
				seedPR1655AcceptanceGraph(t, b, scenario.nodes)
				state, err := b.namespaceMVCC("test")
				require.NoError(t, err)
				before := state.seq.Load()

				// The peer's deletion takes its version, then waits on the
				// commit gate before publishing.
				b.db.commitGate.Lock()
				unlocked := false
				t.Cleanup(func() {
					if !unlocked {
						b.db.commitGate.Unlock()
					}
				})
				done := make(chan error, 1)
				go func() {
					switch scenario.name {
					case "relationship-update/relationship-deleted":
						done <- b.DeleteEdge("test:pr1655-r")
					default:
						done <- b.DeleteNode("test:pr1655-a")
					}
				}()
				deadline := time.Now().Add(10 * time.Second)
				for state.seq.Load() == before && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				require.Greater(t, state.seq.Load(), before, "the peer's deletion allocated its version")

				writer := beginPR1655AcceptanceWriter(t, b)
				marker := NodeID("test:pr1655-marker")
				_, err = writer.CreateNode(&Node{ID: marker, Labels: []string{"Marker"}})
				require.NoError(t, err)
				scenario.stage(t, writer)

				b.db.commitGate.Unlock()
				unlocked = true
				require.NoError(t, <-done, "the peer's deletion must publish")

				err = writer.Commit()
				require.Error(t, err, "the peer's deletion published after the writer staged")
				require.ErrorIs(t, err, ErrConflict,
					"the commit must fail as a retryable concurrent modification")

				_, err = b.GetNode(marker)
				require.ErrorIs(t, err, ErrNotFound, "the failed commit published its marker")
				_, err = b.GetEdge("test:pr1655-new")
				require.ErrorIs(t, err, ErrNotFound, "the failed commit created a relationship")

				require.NoError(t, b.FlushWriteBehind())
				assertPR1655AcceptanceGraph(t, b, scenario, marker,
					"after the peer's deletion publishes first")
			})
		}
	}
}

// TestPR1655StagedTargetControls pins the semantics a reproduction must not
// change: a target deleted before the writer begins, or one that never
// existed, behaves as it does today, and the writer's own deletion does too.
func TestPR1655StagedTargetControls(t *testing.T) {
	t.Run("deleted-before-begin", func(t *testing.T) {
		for _, writeBehind := range []bool{true, false} {
			b := openPR1655AcceptanceEngine(t, writeBehind)
			seedPR1655AcceptanceGraph(t, b, [2]NodeID{"test:pr1655-a", "test:pr1655-b"})
			require.NoError(t, b.DeleteEdge("test:pr1655-r"))
			require.NoError(t, b.FlushWriteBehind())

			writer := beginPR1655AcceptanceWriter(t, b)
			err := writer.UpdateEdge(EdgeUpdateRequest{After: &Edge{
				ID:        "test:pr1655-r",
				StartNode: "test:pr1655-a",
				EndNode:   "test:pr1655-b",
				Type:      "R",
			}})
			require.ErrorIs(t, err, ErrNotFound, "staging against a relationship deleted before begin")
			require.NoError(t, writer.Rollback())

			require.NoError(t, b.DeleteNode("test:pr1655-b"))
			require.NoError(t, b.FlushWriteBehind())
			nodeWriter := beginPR1655AcceptanceWriter(t, b)
			err = nodeWriter.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:pr1655-b", Labels: []string{"Node"}}, Policy: ClientNodeEdits})
			require.ErrorIs(t, err, ErrNotFound, "staging against a node deleted before begin")
		}
	})

	t.Run("never-existed", func(t *testing.T) {
		for _, writeBehind := range []bool{true, false} {
			b := openPR1655AcceptanceEngine(t, writeBehind)
			seedPR1655AcceptanceGraph(t, b, [2]NodeID{"test:pr1655-a", "test:pr1655-b"})

			writer := beginPR1655AcceptanceWriter(t, b)
			err := writer.UpdateEdge(EdgeUpdateRequest{After: &Edge{
				ID:        "test:pr1655-absent",
				StartNode: "test:pr1655-a",
				EndNode:   "test:pr1655-b",
				Type:      "R",
			}})
			require.ErrorIs(t, err, ErrNotFound, "staging against a relationship that never existed")

			nodeWriter := beginPR1655AcceptanceWriter(t, b)
			err = nodeWriter.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:pr1655-absent", Labels: []string{"Node"}}, Policy: ClientNodeEdits})
			require.ErrorIs(t, err, ErrNotFound, "staging against a node that never existed")

			// A new relationship to a missing endpoint is refused when staged
			// (statement time) or, when the absence is only visible at
			// commit, as the same concurrent-modification conflict.
			edgeWriter := beginPR1655AcceptanceWriter(t, b)
			err = edgeWriter.CreateEdge(&Edge{
				ID:        "test:pr1655-orphan",
				StartNode: "test:pr1655-b",
				EndNode:   "test:pr1655-absent",
				Type:      "R",
			})
			if err == nil {
				err = edgeWriter.Commit()
			} else {
				require.NoError(t, edgeWriter.Rollback())
			}
			require.Error(t, err, "a relationship to a missing endpoint is refused")
			require.True(t, errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict),
				"the refusal is a missing-target shape, got %v", err)
			_, err = b.GetEdge("test:pr1655-orphan")
			require.ErrorIs(t, err, ErrNotFound, "no relationship to a missing endpoint is stored")
		}
	})

	t.Run("own-deletion-existing-semantics", func(t *testing.T) {
		for _, writeBehind := range []bool{true, false} {
			b := openPR1655AcceptanceEngine(t, writeBehind)
			seedPR1655AcceptanceGraph(t, b, [2]NodeID{"test:pr1655-a", "test:pr1655-b"})

			// The writer updates a target and then deletes it itself: its own
			// delete wins, the commit succeeds and the update is not stored.
			writer := beginPR1655AcceptanceWriter(t, b)
			require.NoError(t, writer.UpdateEdge(EdgeUpdateRequest{After: &Edge{
				ID:         "test:pr1655-r",
				StartNode:  "test:pr1655-a",
				EndNode:    "test:pr1655-b",
				Type:       "R",
				Properties: map[string]any{"note": "writer"},
			}}))
			require.NoError(t, writer.DeleteEdge("test:pr1655-r"), "a transaction may delete a relationship it updated")
			require.NoError(t, writer.Commit(), "its own deletion is not a concurrent modification")
			require.NoError(t, b.FlushWriteBehind())
			_, err := b.GetEdge("test:pr1655-r")
			require.ErrorIs(t, err, ErrNotFound, "the writer's own deletion keeps its effect")

			// Deleting an already-deleted node inside one transaction reports
			// the existing missing-target shape and cascades its relationships.
			deleter := beginPR1655AcceptanceWriter(t, b)
			require.NoError(t, deleter.DeleteGraphNode("test:pr1655-b", CascadeEdges))
			require.ErrorIs(t, deleter.DeleteGraphNode("test:pr1655-b", CascadeEdges), ErrNotFound,
				"a second delete of the same node reports ErrNotFound")
			require.NoError(t, deleter.Commit())
			require.NoError(t, b.FlushWriteBehind())
			_, err = b.GetNode("test:pr1655-b")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetEdge("test:pr1655-r")
			require.ErrorIs(t, err, ErrNotFound)
		}
	})
}

func TestPR1655VanishedTargetAtStaging(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, bufferedSeed := range []bool{false, true} {
			if bufferedSeed && !writeBehind {
				continue // This population specifically requires an acknowledged buffer.
			}
			for _, flush := range []bool{false, true} {
				if flush && !writeBehind {
					continue
				}
				for _, scenario := range pr1655AcceptanceScenarios() {
					name := scenario.name + "/writebehind=" + boolString(writeBehind) + "/buffered-seed=" + boolString(bufferedSeed) + "/flush=" + boolString(flush)
					t.Run(name, func(t *testing.T) {
						b := openPR1655AcceptanceEngine(t, writeBehind)
						if bufferedSeed {
							seed := beginPR1655AcceptanceWriter(t, b)
							require.NoError(t, seed.SetImplicit(true))
							for _, id := range scenario.nodes {
								_, err := seed.CreateNode(&Node{ID: id, Labels: []string{"Node"}})
								require.NoError(t, err)
							}
							require.NoError(t, seed.CreateEdge(&Edge{ID: "test:pr1655-r", StartNode: scenario.nodes[0], EndNode: scenario.nodes[1], Type: "R"}))
							require.NoError(t, seed.Commit())
						} else {
							seedPR1655AcceptanceGraph(t, b, scenario.nodes)
						}
						writer := beginPR1655AcceptanceWriter(t, b)
						// The original statement has matched its mutation target.
						_, err := writer.GetNode(scenario.nodes[0])
						require.NoError(t, err)
						if scenario.name == "relationship-update/relationship-deleted" {
							_, err = writer.GetEdge("test:pr1655-r")
							require.NoError(t, err)
						}
						require.NoError(t, peerPR1655Acceptance(t, b, scenario, pr1655AcceptancePeer{implicit: true}))
						if flush {
							require.NoError(t, b.FlushWriteBehind())
						}
						switch scenario.name {
						case "node-update/node-deleted":
							err = writer.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: scenario.nodes[0], Labels: []string{"Node"}, Properties: map[string]interface{}{"note": "writer"}}, Policy: ClientNodeEdits})
						case "relationship-update/relationship-deleted":
							err = writer.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "test:pr1655-r", StartNode: scenario.nodes[0], EndNode: scenario.nodes[1], Type: "R", Properties: map[string]interface{}{"note": "writer"}}})
						default:
							err = writer.CreateEdge(&Edge{ID: "test:pr1655-new", StartNode: scenario.nodes[1], EndNode: scenario.nodes[0], Type: "R"})
						}
						if err == nil {
							require.False(t, writeBehind, "a live buffered read must classify the missing target while staging")
							err = writer.Commit()
						}
						require.ErrorIs(t, err, ErrConflict, "a peer deletion after the target was matched must remain retryable")
						require.NotErrorIs(t, err, ErrNotFound)
					})
				}
			}
		}
	}
}

func TestPR1655TargetObservedAfterBeginThenDeleted(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			b := openPR1655AcceptanceEngine(t, true)
			writer := beginPR1655AcceptanceWriter(t, b)
			seedPR1655AcceptanceGraph(t, b, [2]NodeID{"test:pr1655-a", "test:pr1655-b"})
			if kind == "node" {
				node, err := writer.GetNode("test:pr1655-a")
				require.NoError(t, err, "write-behind exposes this committed target after begin")
				require.NoError(t, b.DeleteNode(node.ID))
				after := *node
				after.Properties = map[string]any{"note": "writer"}
				require.ErrorIs(t, writer.UpdateGraphNode(NodeUpdateRequest{Before: node, After: &after, Policy: ClientNodeEdits}), ErrConflict, "the writer actually observed the target before the peer deleted it")
			} else {
				edge, err := writer.GetEdge("test:pr1655-r")
				require.NoError(t, err, "write-behind exposes this committed target after begin")
				require.NoError(t, b.DeleteEdge(edge.ID))
				after := *edge
				after.Properties = map[string]any{"note": "writer"}
				require.ErrorIs(t, writer.UpdateEdge(EdgeUpdateRequest{Before: edge, After: &after}), ErrConflict, "the writer actually observed the target before the peer deleted it")
			}
		})
	}
}

// boolString names one boolean case in a subtest so the fail-first log shows
// exactly which producer combination failed.
func boolString(v bool) string {
	if v {
		return "on"
	}
	return "off"
}
