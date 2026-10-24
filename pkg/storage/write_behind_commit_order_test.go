package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openWriteBehindOrderEngine(t *testing.T, dir string, lowMemory bool) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: lowMemory, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	return b
}

func writeBehindOrderNode(t *testing.T, store Engine, id NodeID) *Node {
	t.Helper()
	var found *Node
	require.NoError(t, StreamStoredNodes(context.Background(), store, StreamNodesOptions{}, func(n *Node) error {
		if n.ID == id {
			found = copyNode(n)
		}
		return nil
	}))
	require.NotNil(t, found, "stored node %s", id)
	return found
}

// writeBehindOrderHolders counts the nodes holding label.id=value through the
// full stored reader and through the label index.
func writeBehindOrderHolders(t *testing.T, store Engine, label, value string) (full, indexed int) {
	t.Helper()
	require.NoError(t, StreamStoredNodes(context.Background(), store, StreamNodesOptions{}, func(n *Node) error {
		if hasLabel(n.Labels, label) && n.Properties["id"] == value {
			full++
		}
		return nil
	}))
	require.NoError(t, StreamStoredNodesByLabelProjected(store, label, []string{"id"}, func(n *Node) error {
		if n.Properties["id"] == value {
			indexed++
		}
		return nil
	}))
	return full, indexed
}

// NOR-143: an acknowledged buffered image must never be flushed over a newer
// committed one. The explicit relabel is paused after its Badger commit and
// before its publication while the buffer flushes; the stored node, the label
// index, the derived counts and the cache must then all show the relabel.
func TestWriteBehindFlushKeepsNewerCommittedImage(t *testing.T) {
	for _, lowMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("lowMemory=%v", lowMemory), func(t *testing.T) {
			writeBehindFlushKeepsNewerCommittedImage(t, lowMemory)
		})
	}
}

func writeBehindFlushKeepsNewerCommittedImage(t *testing.T, lowMemory bool) {
	dir := t.TempDir()
	b := openWriteBehindOrderEngine(t, dir, lowMemory)
	closed := false
	defer func() {
		if !closed {
			_ = b.Close()
		}
	}()
	store := NewNamespacedEngine(b, "test")
	schema := store.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(store, schema))
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "held", "note": "seed"}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())

	ack, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, ack.SetImplicit(true))
	node, err := ack.GetNode("test:a")
	require.NoError(t, err)
	node.Properties["note"] = "ack"
	require.NoError(t, ack.UpdateNode(node))
	require.NoError(t, ack.Commit())
	require.True(t, ack.bufferedAck.Load(), "the acknowledged write must still be buffered")

	peer, err := b.BeginTransaction()
	require.NoError(t, err)
	node, err = peer.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "ack", node.Properties["note"])
	node.Labels = []string{"Parked"}
	node.Properties["note"] = "peer"
	require.NoError(t, peer.UpdateNode(node))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	var armed atomic.Bool
	reset := setCommitTailHook(func() {
		if armed.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
	})
	defer reset()
	committed := make(chan error, 1)
	go func() { committed <- peer.CommitContext(ctx) }()
	select {
	case <-entered:
	case err := <-committed:
		t.Fatalf("the relabel never reached its publication: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-ctx.Done():
		release()
		t.Fatal("flush blocked behind the paused relabel")
	}
	release()
	require.NoError(t, <-committed)
	require.NoError(t, b.FlushWriteBehind())

	check := func(stage string, b *BadgerEngine) {
		t.Helper()
		store := NewNamespacedEngine(b, "test")
		stored := writeBehindOrderNode(t, store, "a")
		require.Equal(t, []string{"Parked"}, stored.Labels, "%s: stored labels", stage)
		require.Equal(t, "peer", stored.Properties["note"], "%s: stored body", stage)
		full, indexed := writeBehindOrderHolders(t, store, "Doc", "held")
		require.Equal(t, 0, full, "%s: stored Doc holders", stage)
		require.Equal(t, full, indexed, "%s: the label index must match the stored nodes", stage)
		full, indexed = writeBehindOrderHolders(t, store, "Parked", "held")
		require.Equal(t, 1, full, "%s: stored Parked holders", stage)
		require.Equal(t, full, indexed, "%s: the label index must match the stored nodes", stage)
		cached, err := store.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, stored.Labels, cached.Labels, "%s: cached labels", stage)
		require.Equal(t, stored.Properties["note"], cached.Properties["note"], "%s: cached body", stage)
		docs, err := b.NodeCountByLabelInNamespace("test", "Doc")
		require.NoError(t, err)
		parked, err := b.NodeCountByLabelInNamespace("test", "Parked")
		require.NoError(t, err)
		require.Equal(t, [2]int64{0, 1}, [2]int64{docs, parked}, "%s: derived Doc/Parked counts", stage)
	}
	check("flushed", b)

	// The relabel released the key: a new Doc may take it, and then holds it alone.
	_, err = store.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"id": "held"}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())
	full, indexed := writeBehindOrderHolders(t, store, "Doc", "held")
	require.Equal(t, [2]int{1, 1}, [2]int{full, indexed})

	require.NoError(t, b.Close())
	closed = true
	reopened := openWriteBehindOrderEngine(t, dir, lowMemory)
	defer reopened.Close()
	reopenedStore := NewNamespacedEngine(reopened, "test")
	stored := writeBehindOrderNode(t, reopenedStore, "a")
	require.Equal(t, []string{"Parked"}, stored.Labels, "restarted: stored labels")
	full, indexed = writeBehindOrderHolders(t, reopenedStore, "Doc", "held")
	require.Equal(t, [2]int{1, 1}, [2]int{full, indexed}, "restarted: Doc holders")
	_, err = reopenedStore.CreateNode(&Node{ID: "c", Labels: []string{"Doc"}, Properties: map[string]any{"id": "held"}})
	require.Error(t, err, "restarted: a second Doc holder of the key")
}

func seedWriteBehindOrderNode(t *testing.T, b *BadgerEngine, id NodeID, labels []string, props map[string]any) {
	t.Helper()
	_, err := b.CreateNode(&Node{ID: id, Labels: labels, Properties: props})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())
}

// NOR-136: a write staged against a node a still-buffered peer changed after
// the writer's snapshot must fail with a conflict, for implicit and explicit
// writers alike, and the peer's acknowledged write must survive.
func TestWriteBehindUnflushedPeerConflicts(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			b := openWriteBehindOrderEngine(t, t.TempDir(), true)
			defer b.Close()
			seedWriteBehindOrderNode(t, b, "test:n", []string{"Doc"}, map[string]any{"note": "seed", "other": "seed"})

			loser, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, loser.SetImplicit(!explicit))
			node, err := loser.GetNode("test:n")
			require.NoError(t, err)
			node.Properties["note"] = "loser"
			require.NoError(t, loser.UpdateNode(node))

			peer, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, peer.SetImplicit(true))
			node, err = peer.GetNode("test:n")
			require.NoError(t, err)
			node.Properties["other"] = "peer"
			require.NoError(t, peer.UpdateNode(node))
			require.NoError(t, peer.Commit())
			require.True(t, peer.bufferedAck.Load())

			err = loser.Commit()
			require.ErrorIs(t, err, ErrConflict, "the loser must not overwrite the acknowledged peer")
			require.NoError(t, b.FlushWriteBehind())
			stored := writeBehindOrderNode(t, NewNamespacedEngine(b, "test"), "n")
			require.Equal(t, "peer", stored.Properties["other"])
			require.Equal(t, "seed", stored.Properties["note"])
		})
	}
}

// A direct engine write lands synchronously; the buffered image it replaces
// must land first, never after it.
func TestWriteBehindDirectWriteOutlivesOlderBufferedImage(t *testing.T) {
	b := openWriteBehindOrderEngine(t, t.TempDir(), true)
	defer b.Close()
	seedWriteBehindOrderNode(t, b, "test:n", []string{"Doc"}, map[string]any{"note": "seed"})
	ack, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, ack.SetImplicit(true))
	node, err := ack.GetNode("test:n")
	require.NoError(t, err)
	node.Properties["note"] = "ack"
	node.Labels = []string{"Doc", "Tagged"}
	require.NoError(t, ack.UpdateNode(node))
	require.NoError(t, ack.Commit())
	require.True(t, ack.bufferedAck.Load())

	direct := copyNode(node)
	direct.Labels = []string{"Doc"}
	direct.Properties["note"] = "direct"
	require.NoError(t, b.UpdateNode(direct))
	require.NoError(t, b.FlushWriteBehind())

	store := NewNamespacedEngine(b, "test")
	stored := writeBehindOrderNode(t, store, "n")
	require.Equal(t, "direct", stored.Properties["note"])
	require.Equal(t, []string{"Doc"}, stored.Labels)
	tagged, err := b.NodeCountByLabelInNamespace("test", "Tagged")
	require.NoError(t, err)
	require.Zero(t, tagged)
	read, err := store.GetNode("n")
	require.NoError(t, err)
	require.Equal(t, "direct", read.Properties["note"])
}

// A node delete must not orphan a relationship a buffered peer created after
// the delete's snapshot, and a relationship create must not land on an
// endpoint a buffered peer deleted after its snapshot.
func TestWriteBehindBufferedRelationshipRaces(t *testing.T) {
	setup := func(t *testing.T) *BadgerEngine {
		b := openWriteBehindOrderEngine(t, t.TempDir(), true)
		seedWriteBehindOrderNode(t, b, "test:x", []string{"N"}, map[string]any{"k": "x"})
		seedWriteBehindOrderNode(t, b, "test:y", []string{"N"}, map[string]any{"k": "y"})
		return b
	}
	t.Run("delete after newer relationship", func(t *testing.T) {
		b := setup(t)
		defer b.Close()
		deleter, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, deleter.SetImplicit(true))
		require.NoError(t, deleter.DeleteNode("test:x"))

		peer, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, peer.SetImplicit(true))
		require.NoError(t, peer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:x", EndNode: "test:y", Type: "R"}))
		require.NoError(t, peer.Commit())
		require.True(t, peer.bufferedAck.Load())

		require.ErrorIs(t, deleter.Commit(), ErrConflict)
		require.NoError(t, b.FlushWriteBehind())
		_, err = b.GetNode("test:x")
		require.NoError(t, err)
		_, err = b.GetEdge("test:e")
		require.NoError(t, err)
	})
	t.Run("relationship after newer endpoint delete", func(t *testing.T) {
		b := setup(t)
		defer b.Close()
		creator, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, creator.SetImplicit(true))
		require.NoError(t, creator.CreateEdge(&Edge{ID: "test:e", StartNode: "test:x", EndNode: "test:y", Type: "R"}))

		peer, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, peer.SetImplicit(true))
		require.NoError(t, peer.DeleteNode("test:x"))
		require.NoError(t, peer.Commit())
		require.True(t, peer.bufferedAck.Load())

		require.ErrorIs(t, creator.Commit(), ErrConflict)
		require.NoError(t, b.FlushWriteBehind())
		_, err = b.GetNode("test:x")
		require.ErrorIs(t, err, ErrNotFound)
		_, err = b.GetEdge("test:e")
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// NOR-136 / NOR-138 under concurrency: implicit and explicit increments race
// with label changes on the same nodes while the buffer flushes. Every
// acknowledged increment survives, the derived label counts match a recount,
// and the restart folds them without an underflow.
func TestWriteBehindConcurrentWritersKeepEveryAcknowledgedWrite(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: true, WriteBehindInterval: 2 * time.Millisecond})
	require.NoError(t, err)
	ids := []NodeID{"test:c0", "test:c1", "test:c2"}
	for _, id := range ids {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"cnt": int64(0)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind())

	var successes [3]atomic.Int64
	var conflicts, failures atomic.Int64
	var wg sync.WaitGroup
	const workers, iterations = 8, 150
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker) + time.Now().UnixNano()))
			for i := 0; i < iterations; i++ {
				which := rng.Intn(len(ids))
				tx, err := b.BeginTransaction()
				if err != nil {
					failures.Add(1)
					continue
				}
				_ = tx.SetImplicit(rng.Intn(3) != 0)
				node, err := tx.GetNode(ids[which])
				if err != nil {
					_ = tx.Rollback()
					failures.Add(1)
					continue
				}
				increment := rng.Intn(4) != 0
				if increment {
					node.Properties["cnt"] = node.Properties["cnt"].(int64) + 1
				}
				switch rng.Intn(3) {
				case 0:
					node.Labels = []string{"Doc", "Tagged"}
				case 1:
					node.Labels = []string{"Doc"}
				}
				if err := tx.UpdateNode(node); err != nil {
					_ = tx.Rollback()
					failures.Add(1)
					continue
				}
				switch err := tx.Commit(); {
				case err == nil:
					if increment {
						successes[which].Add(1)
					}
				case errors.Is(err, ErrConflict):
					conflicts.Add(1)
				default:
					failures.Add(1)
					t.Errorf("worker %d: unexpected commit error: %v", worker, err)
				}
			}
		}(worker)
	}
	wg.Wait()
	t.Logf("conflicts=%d failures=%d successes=%v", conflicts.Load(), failures.Load(), []int64{successes[0].Load(), successes[1].Load(), successes[2].Load()})
	require.Zero(t, failures.Load())

	check := func(stage string, b *BadgerEngine) {
		t.Helper()
		store := NewNamespacedEngine(b, "test")
		tagged := int64(0)
		for i, id := range ids {
			stored := writeBehindOrderNode(t, store, NodeID(id[len("test:"):]))
			require.Equal(t, successes[i].Load(), stored.Properties["cnt"], "%s: %s lost acknowledged increments", stage, id)
			if hasLabel(stored.Labels, "Tagged") {
				tagged++
			}
		}
		docs, err := b.NodeCountByLabelInNamespace("test", "Doc")
		require.NoError(t, err)
		taggedCount, err := b.NodeCountByLabelInNamespace("test", "Tagged")
		require.NoError(t, err)
		require.Equal(t, [2]int64{int64(len(ids)), tagged}, [2]int64{docs, taggedCount}, "%s: derived counts must match the stored nodes", stage)
	}
	require.NoError(t, b.FlushWriteBehind())
	check("flushed", b)
	require.NoError(t, b.Close())
	reopened, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: true, WriteBehindInterval: 2 * time.Millisecond})
	require.NoError(t, err, "restart after concurrent write-behind writes")
	defer reopened.Close()
	check("restarted", reopened)
}
