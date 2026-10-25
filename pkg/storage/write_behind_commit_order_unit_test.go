package storage

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A generation that retires between a buffered commit's Badger validation and
// its reservation sends the commit back through Badger validation, where the
// retired peer is now visible as a newer head.
func TestWriteBehindReservationRevalidatesAfterRetirement(t *testing.T) {
	b := openWriteBehindOrderEngine(t, t.TempDir(), true)
	defer b.Close()
	seedWriteBehindOrderNode(t, b, "test:n", []string{"Doc"}, map[string]any{"note": "seed"})
	loser, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, loser.SetImplicit(true))
	node, err := loser.GetNode("test:n")
	require.NoError(t, err)
	node.Properties["note"] = "loser"
	require.NoError(t, loser.UpdateNode(node))
	epoch := b.writeBehind.retireEpoch.Load()

	peer, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, peer.SetImplicit(true))
	node, err = peer.GetNode("test:n")
	require.NoError(t, err)
	node.Properties["note"] = "peer"
	require.NoError(t, peer.UpdateNode(node))
	require.NoError(t, peer.Commit())
	require.NoError(t, b.FlushWriteBehind())
	require.Greater(t, b.writeBehind.retireEpoch.Load(), epoch)

	loser.mu.Lock()
	loser.bufferedRetireEpoch = epoch
	reservation, err := loser.reserveBufferedCommitLocked()
	loser.mu.Unlock()
	require.Empty(t, reservation.targets)
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, loser.Rollback())
}

// In-flight reservations: one writer per entity, endpoint users share, and a
// node delete excludes endpoint users.
func TestWriteBehindReservationHolds(t *testing.T) {
	w := NewWriteBehindBuffer(time.Hour, 0, func(*CommitBuffer) error { return nil })
	defer w.Close()
	readTS := MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: 1}
	newer := func(v MVCCVersion) bool { return v.Compare(readTS) > 0 }
	var sequence uint64 = 10
	allocate := func() (MVCCVersion, error) {
		sequence++
		return MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: sequence}, nil
	}
	node := func(id string, use bufferedTargetUse) bufferedTarget {
		return bufferedTarget{bufferedTargetKey: bufferedTargetKey{kind: bufferedNodeEntity, id: id}, use: use}
	}
	reserve := func(owner string, targets ...bufferedTarget) (bufferedReservation, *bufferedConflict) {
		r, conflict, err := w.admitBufferedCommit("test", newer, w.retireEpoch.Load(), targets, nil, allocate)
		require.NoError(t, err)
		return r, conflict
	}
	first, conflict := reserve("a", node("n", bufferedTargetWrite))
	require.Nil(t, conflict)
	_, conflict = reserve("b", node("n", bufferedTargetWrite))
	require.NotNil(t, conflict)
	endpoint, conflict := reserve("c", node("n", bufferedTargetEndpoint))
	require.Nil(t, conflict, "an update does not exclude endpoint users")
	w.releaseReservation(first)
	_, conflict = reserve("d", node("n", bufferedTargetDelete))
	require.NotNil(t, conflict, "a delete excludes endpoint users")
	w.releaseReservation(endpoint)
	deleting, conflict := reserve("d", node("n", bufferedTargetDelete))
	require.Nil(t, conflict)
	_, conflict = reserve("e", node("n", bufferedTargetEndpoint))
	require.NotNil(t, conflict, "an endpoint user waits out a delete")
	w.releaseReservation(deleting)
	w.mu.RLock()
	require.Empty(t, w.holds)
	w.mu.RUnlock()

	_, _, err := w.admitBufferedCommit("test", newer, w.retireEpoch.Load()+1, []bufferedTarget{node("n", bufferedTargetWrite)}, nil, allocate)
	require.ErrorIs(t, err, errBufferedValidationStale)
}

// A snapshot taken while a buffered commit is admitted but not yet appended
// treats that commit as newer, and only that one: commits that became
// visible later, with higher sequences, are not.
func TestWriteBehindSnapshotRecordsInflightVersions(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:n", Labels: []string{"N"}})
	require.NoError(t, err)
	w := b.writeBehind
	slow, conflict, err := w.admitBufferedCommit("test", func(MVCCVersion) bool { return false }, w.retireEpoch.Load(), nil, nil, func() (MVCCVersion, error) {
		return b.allocateMVCCVersionRaw(nil, "test", time.Now())
	})
	require.NoError(t, err)
	require.Nil(t, conflict)
	visible, err := b.allocateMVCCVersionRaw(nil, "test", time.Now())
	require.NoError(t, err)

	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.GetNode("test:n")
	require.NoError(t, err)
	require.Len(t, tx.readInflight, 1)
	require.True(t, tx.snapshotIsolationConflict(slow.version), "the in-flight commit is newer than the snapshot")
	require.False(t, tx.snapshotIsolationConflict(visible), "a commit visible at the snapshot is not")
	after, err := b.allocateMVCCVersionRaw(nil, "test", time.Now())
	require.NoError(t, err)
	require.True(t, tx.snapshotIsolationConflict(after))

	w.releaseReservation(slow)
	require.Nil(t, w.inflightVersions())
}

func writeBehindOrderEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []NodeID{"test:x", "test:y"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind())
	return b
}

func writeBehindOrderImplicit(t *testing.T, b *BadgerEngine, stage func(tx *BadgerTransaction)) *BadgerTransaction {
	t.Helper()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	stage(tx)
	return tx
}

// Two buffered writers of one relationship: the later one loses.
func TestWriteBehindBufferedRelationshipWriteConflict(t *testing.T) {
	b := writeBehindOrderEngine(t)
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:x", EndNode: "test:y", Type: "R", Properties: map[string]any{"v": int64(0)}}))
	require.NoError(t, b.FlushWriteBehind())
	update := func(tx *BadgerTransaction, v int64) {
		edge, err := tx.GetEdge("test:e")
		require.NoError(t, err)
		edge.Properties["v"] = v
		require.NoError(t, tx.UpdateEdge(edge))
	}
	loser := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) { update(tx, 1) })
	peer := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) { update(tx, 2) })
	require.NoError(t, peer.Commit())
	require.True(t, peer.bufferedAck.Load())
	require.ErrorIs(t, loser.Commit(), ErrConflict)
	require.NoError(t, b.FlushWriteBehind())
	edge, err := b.GetEdge("test:e")
	require.NoError(t, err)
	require.Equal(t, int64(2), edge.Properties["v"])
}

// A DETACH DELETE removing a buffered relationship itself is not raced by it,
// also while that relationship's generation waits to drain.
func TestWriteBehindDetachDeleteOfBufferedRelationship(t *testing.T) {
	b := writeBehindOrderEngine(t)
	creator := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:x", EndNode: "test:y", Type: "R"}))
	})
	require.NoError(t, creator.Commit())
	b.writeBehind.rotate()
	deleter := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		require.NoError(t, tx.DeleteNode("test:x"))
	})
	require.NoError(t, deleter.Commit())
	require.NoError(t, b.FlushWriteBehind())
	_, err := b.GetNode("test:x")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.GetEdge("test:e")
	require.ErrorIs(t, err, ErrNotFound)
}

// A newer buffered relationship in a draining generation still blocks a
// delete of its endpoint that could not see it.
func TestWriteBehindDeleteRacesDrainingRelationship(t *testing.T) {
	b := writeBehindOrderEngine(t)
	deleter := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		require.NoError(t, tx.DeleteNode("test:x"))
	})
	creator := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:x", EndNode: "test:y", Type: "R"}))
	})
	require.NoError(t, creator.Commit())
	b.writeBehind.rotate()
	b.writeBehind.mu.RLock()
	require.Len(t, b.writeBehind.draining, 1)
	b.writeBehind.mu.RUnlock()
	require.ErrorIs(t, deleter.Commit(), ErrConflict)
}

// The engine's direct deletes land after the buffered relationships they
// remove: none survives to dangle after the flush.
func TestWriteBehindDirectDeletesLandAfterBufferedRelationships(t *testing.T) {
	b := writeBehindOrderEngine(t)
	for _, id := range []EdgeID{"test:e1", "test:e2"} {
		tx := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
			require.NoError(t, tx.CreateEdge(&Edge{ID: id, StartNode: "test:x", EndNode: "test:y", Type: "R"}))
		})
		require.NoError(t, tx.Commit())
		require.True(t, tx.bufferedAck.Load())
	}
	require.NoError(t, b.DeleteEdge("test:e1"))
	require.NoError(t, b.DeleteNode("test:x"))
	require.NoError(t, b.FlushWriteBehind())
	for _, id := range []EdgeID{"test:e1", "test:e2"} {
		_, err := b.GetEdge(id)
		require.ErrorIs(t, err, ErrNotFound, "%s", id)
	}
	count, err := b.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}

// A bulk commit's claims use an index and are released whole.
func TestWriteBehindBulkCommitReservation(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	const n = bufferedHoldsRetainLimit + 8
	seed, err := b.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		_, err := seed.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, seed.Commit())
	bulk := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		for i := 0; i < n; i++ {
			node, err := tx.GetNode(NodeID(fmt.Sprintf("test:n%d", i)))
			require.NoError(t, err)
			node.Properties = map[string]any{"v": int64(i)}
			require.NoError(t, tx.UpdateNode(node))
		}
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:bulk", StartNode: "test:n0", EndNode: "test:n1", Type: "R"}))
	})
	targets, _ := bulk.bufferedCommitTargetsLocked()
	require.Len(t, targets, n+1, "endpoints written by the commit are covered by their writes")
	require.NoError(t, bulk.Commit())
	require.True(t, bulk.bufferedAck.Load())
	b.writeBehind.mu.RLock()
	require.Nil(t, b.writeBehind.holds)
	require.Zero(t, b.writeBehind.inflightCount.Load())
	for _, versions := range b.writeBehind.inflight {
		require.Empty(t, versions)
	}
	b.writeBehind.mu.RUnlock()
	b.writeBehind.releaseReservation(bufferedReservation{})
}

// A synchronous lander that cannot land the buffered generations it is
// ordered after fails without landing.
func TestWriteBehindSynchronousLandingFailsWhenDrainFails(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	// Installed before anything signals the flusher, so the switch is the
	// only state shared with it.
	failed := errors.New("injected drain failure")
	var failing atomic.Bool
	apply := b.writeBehind.apply
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		if failing.Load() {
			return failed
		}
		return apply(buf)
	}
	for _, id := range []NodeID{"test:x", "test:y"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	ack := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		node, err := tx.GetNode("test:x")
		require.NoError(t, err)
		node.Properties = map[string]any{"note": "ack"}
		require.NoError(t, tx.UpdateNode(node))
	})
	require.NoError(t, ack.Commit())
	failing.Store(true)

	// An explicit commit of the buffered node must land it first and fails
	// with the drain; one of an unrelated node is not ordered after it.
	explicit, err := b.BeginTransaction()
	require.NoError(t, err)
	node, err := explicit.GetNode("test:x")
	require.NoError(t, err)
	node.Properties = map[string]any{"note": "explicit"}
	require.NoError(t, explicit.UpdateNode(node))
	require.ErrorIs(t, explicit.Commit(), failed)
	unrelated, err := b.BeginTransaction()
	require.NoError(t, err)
	node, err = unrelated.GetNode("test:y")
	require.NoError(t, err)
	node.Properties = map[string]any{"note": "unrelated"}
	require.NoError(t, unrelated.UpdateNode(node))
	require.NoError(t, unrelated.Commit())
	direct := &Node{ID: "test:x", Labels: []string{"N"}, Properties: map[string]any{"note": "direct"}}
	require.ErrorIs(t, b.UpdateNode(direct), failed)

	failing.Store(false)
	require.NoError(t, b.FlushWriteBehind())
	x, err := b.GetNode("test:x")
	require.NoError(t, err)
	require.Equal(t, "ack", x.Properties["note"])
	y, err := b.GetNode("test:y")
	require.NoError(t, err)
	require.Equal(t, "unrelated", y.Properties["note"])
}

// While one buffered commit is in flight, another writer's commits stay
// visible to it: updating its own node twice in a row is not a conflict.
func TestWriteBehindInflightCommitDoesNotHideLaterCommits(t *testing.T) {
	b := writeBehindOrderEngine(t)
	w := b.writeBehind
	inflight, conflict, err := w.admitBufferedCommit("test", func(MVCCVersion) bool { return false }, w.retireEpoch.Load(),
		[]bufferedTarget{{bufferedTargetKey: bufferedTargetKey{kind: bufferedNodeEntity, id: "test:x"}, use: bufferedTargetWrite}}, nil,
		func() (MVCCVersion, error) { return b.allocateMVCCVersion(nil, "test", time.Now()) })
	require.NoError(t, err)
	require.Nil(t, conflict)
	defer w.releaseReservation(inflight)
	for i := 0; i < 3; i++ {
		tx := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
			node, err := tx.GetNode("test:y")
			require.NoError(t, err)
			node.Properties = map[string]any{"v": int64(i)}
			require.NoError(t, tx.UpdateNode(node))
		})
		require.NoError(t, tx.Commit(), "update %d", i)
		require.True(t, tx.bufferedAck.Load())
	}
	loser := writeBehindOrderImplicit(t, b, func(tx *BadgerTransaction) {
		node, err := tx.GetNode("test:x")
		require.NoError(t, err)
		node.Properties = map[string]any{"v": int64(9)}
		require.NoError(t, tx.UpdateNode(node))
	})
	require.ErrorIs(t, loser.Commit(), ErrConflict, "the in-flight writer still holds x")
}
