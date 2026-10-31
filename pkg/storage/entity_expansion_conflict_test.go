package storage

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeCompleteFootprintWaitsBeforePublication(t *testing.T) {
	for _, operation := range []string{"delete", "delete-pair", "move", "growth"} {
		t.Run(operation, func(t *testing.T) {
			b := newTestEngine(t)
			for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"value": "seed"}}))
			heldID, isEdge := "test:e", true
			if operation == "move" {
				heldID, isEdge = "test:a", false
			}
			_, release, err := b.beginEntityWrite(context.Background(), heldID, isEdge, nil)
			require.NoError(t, err)
			var publications atomic.Int32
			b.OnEdgeUpdated(func(*Edge) { publications.Add(1) })
			b.OnNodeDeleted(func(NodeID) { publications.Add(1) })
			b.OnEdgeDeleted(func(EdgeID) { publications.Add(1) })
			waiting := make(chan struct{})
			var waits atomic.Int32
			secondWait := make(chan struct{})
			restore := SetUniqueKeyLockWaitHookForTesting(func() {
				switch waits.Add(1) {
				case 1:
					close(waiting)
				case 2:
					close(secondWait)
				}
			})
			done := make(chan error, 1)
			joined := make(chan struct{})
			peerDone := make(chan error, 1)
			peerJoined := make(chan struct{})
			peerStarted := false
			t.Cleanup(func() {
				release()
				restore()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("footprint worker did not join")
				}
				if peerStarted {
					select {
					case <-peerJoined:
					case <-time.After(5 * time.Second):
						t.Error("peer delete did not join")
					}
				}
			})
			go func() {
				defer close(joined)
				if operation == "move" {
					done <- b.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "test:e", StartNode: "test:c", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"value": "moved"}}})
				} else {
					done <- b.DeleteNode("test:a")
				}
			}()
			select {
			case <-waiting:
			case err := <-done:
				t.Fatalf("write completed before complete footprint became available: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("write did not wait for complete footprint")
			}
			select {
			case err := <-done:
				t.Fatalf("early publication: %v", err)
			default:
			}
			require.Zero(t, publications.Load())
			stored, err := b.GetStoredEdge("test:e")
			require.NoError(t, err)
			require.Equal(t, NodeID("test:a"), stored.StartNode)
			require.Equal(t, "seed", stored.Properties["value"])
			if operation == "delete-pair" {
				peerStarted = true
				go func() { defer close(peerJoined); peerDone <- b.DeleteEdge("test:e") }()
				select {
				case <-secondWait:
				case err := <-peerDone:
					t.Fatalf("peer delete completed before footprint release: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("peer delete did not wait")
				}
			}
			if operation == "growth" {
				// The blocker models a peer's lease. The growth writer must not
				// inherit that manually held test lease when delivering listeners.
				growthDone, growthJoined := make(chan error, 1), make(chan struct{})
				t.Cleanup(func() {
					release()
					select {
					case <-growthJoined:
					case <-time.After(5 * time.Second):
						t.Error("growth writer did not join")
					}
				})
				go func() {
					defer close(growthJoined)
					growthDone <- b.CreateEdge(&Edge{ID: "test:f", StartNode: "test:a", EndNode: "test:c", Type: "NEW"})
				}()
				select {
				case err := <-growthDone:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("growth writer did not complete")
				}
			}
			release()
			select {
			case err := <-done:
				if operation == "growth" {
					require.ErrorIs(t, err, ErrConflict)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("write did not complete after release")
			}
			if peerStarted {
				select {
				case err := <-peerDone:
					require.True(t, err == nil || errors.Is(err, ErrNotFound), "serialized edge delete: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("peer delete did not complete")
				}
			}
			nodes, err := b.NodeCount()
			require.NoError(t, err)
			edges, err := b.EdgeCount()
			require.NoError(t, err)
			switch operation {
			case "delete", "delete-pair":
				require.EqualValues(t, 2, publications.Load(), "one node and one edge deletion publish once each")
				require.EqualValues(t, 2, nodes)
				require.Zero(t, edges)
				_, err = b.GetStoredEdge("test:e")
				require.ErrorIs(t, err, ErrNotFound)
			case "move":
				require.EqualValues(t, 3, nodes)
				require.EqualValues(t, 1, edges)
				stored, err = b.GetStoredEdge("test:e")
				require.NoError(t, err)
				require.Equal(t, NodeID("test:c"), stored.StartNode)
				require.Equal(t, "moved", stored.Properties["value"])
				require.EqualValues(t, 1, publications.Load())
			case "growth":
				require.EqualValues(t, 3, nodes)
				require.EqualValues(t, 2, edges)
				_, err = b.GetStoredNode("test:a")
				require.NoError(t, err)
				stored, err = b.GetStoredEdge("test:e")
				require.NoError(t, err)
				require.Equal(t, NodeID("test:a"), stored.StartNode)
				_, err = b.GetStoredEdge("test:f")
				require.NoError(t, err)
				require.Zero(t, publications.Load())
			}
		})
	}
}

// suppressedEdgeForCleanup persists an edge, suppresses it, and deletes it while
// the next worker pass is already ending, so a same-ID recreation must wait for
// the cleanup that retires the old suppression. It restores the ending flag when
// the test finishes, after the test's own cleanup.
func suppressedEdgeForCleanup(t *testing.T, b *BadgerEngine) *Edge {
	t.Helper()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
	require.NoError(t, b.CreateEdge(edge))
	require.NoError(t, b.SetManualSuppression(StoredEntityEdgeKind, string(edge.ID), true))
	t.Cleanup(func() { b.manualSuppressionMu.Lock(); b.suppressionEnding = false; b.manualSuppressionMu.Unlock() })
	b.manualSuppressionMu.Lock()
	b.suppressionEnding = true
	b.manualSuppressionMu.Unlock()
	require.NoError(t, b.DeleteEdge(edge.ID))
	return edge
}

func TestNativeCompleteFootprintWaitsForSuppressionCleanup(t *testing.T) {
	b := newTestEngine(t)
	edge := suppressedEdgeForCleanup(t, b)
	_, release, err := b.beginEntityWrite(context.Background(), string(edge.ID), true, nil)
	require.NoError(t, err)
	waiting := make(chan struct{})
	resume := make(chan struct{})
	foreground := make(chan struct{})
	var calls atomic.Int32
	var resumeOnce, foregroundOnce sync.Once
	schema := b.GetSchemaForNamespace("test")
	owner := make(chan string, 1)
	restore := SetUniqueKeyLockWaitHookForTesting(func() {
		if calls.Add(1) == 1 {
			schema.uniqueConstraintCommitLocksMu.Lock()
			for id := range schema.uniqueConstraintLockWaits {
				owner <- id
				break
			}
			schema.uniqueConstraintCommitLocksMu.Unlock()
			close(waiting)
			<-resume
		} else {
			foregroundOnce.Do(func() { close(foreground) })
		}
	})
	bgJoined := make(chan struct{})
	joined := make(chan struct{})
	done := make(chan error, 1)
	mutexHeld := false
	workerStarted := false
	t.Cleanup(func() {
		release()
		resumeOnce.Do(func() { close(resume) })
		if mutexHeld {
			b.suppressionWriteMu.Unlock()
		}
		restore()
		select {
		case <-bgJoined:
		case <-time.After(5 * time.Second):
			t.Error("suppression worker did not join")
		}
		if workerStarted {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("foreground delete did not join")
			}
		}
	})
	go func() { defer close(bgJoined); b.endSuppressions(nil) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("suppression worker did not wait")
	}
	var bgOwner string
	select {
	case bgOwner = <-owner:
	case <-time.After(5 * time.Second):
		t.Fatal("suppression waiter owner was not enrolled")
	}
	release()
	require.NoError(t, b.CreateEdge(edge))
	b.suppressionWriteMu.Lock()
	mutexHeld = true
	resumeOnce.Do(func() { close(resume) })
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		schema.uniqueConstraintCommitLocksMu.Lock()
		lock := schema.uniqueConstraintCommitLocks[uniqueConstraintLockKey{entityKind: 2, entityID: string(edge.ID)}]
		owned := lock != nil && lock.owner == bgOwner
		schema.uniqueConstraintCommitLocksMu.Unlock()
		if owned {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("worker did not own cleanup entity")
		default:
		}
		runtime.Gosched()
	}
	var publications atomic.Int32
	b.OnNodeDeleted(func(NodeID) { publications.Add(1) })
	workerStarted = true
	go func() { defer close(joined); done <- b.DeleteNode("test:a") }()
	select {
	case <-foreground:
	case err := <-done:
		t.Fatalf("foreground did not wait for cleanup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("foreground did not reach cleanup authority")
	}
	require.Zero(t, publications.Load())
	stored, err := b.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.StartNode, stored.StartNode)
	b.suppressionWriteMu.Unlock()
	mutexHeld = false
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("foreground did not complete after cleanup")
	}
	select {
	case <-bgJoined:
	case <-time.After(5 * time.Second):
		t.Fatal("suppression worker did not finish after cancellation recheck")
	}
	fresh := &BadgerEngine{}
	require.NoError(t, fresh.loadManualSuppressions(b.db))
	require.False(t, fresh.HasManualSuppressions(), "recreated/deleted edge does not retain its old persisted suppression")
	require.EqualValues(t, 1, publications.Load())
	nodes, err := b.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, nodes)
	edges, err := b.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, edges)
}

func TestNativeCompleteFootprintCloseDrainsQueuedSuppression(t *testing.T) {
	dir := t.TempDir()
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if b != nil {
		fixtureowner.Own(t, b)
	}
	require.NoError(t, err)
	edge := suppressedEdgeForCleanup(t, b)
	require.NoError(t, b.Close(), "Close must borrow its admitted lifecycle after closing normal admission")
	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if reopened != nil {
		fixtureowner.Own(t, reopened)
	}
	require.NoError(t, err)
	require.False(t, reopened.HasManualSuppressions())
	_, err = reopened.GetStoredEdge(edge.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, reopened.CreateEdge(edge))
	_, err = reopened.GetEdge(edge.ID)
	require.NoError(t, err, "same-ID recreation must not regain retired suppression")
}
