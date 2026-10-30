package storage

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestStoredEntityPublicationExactImages(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "old"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:b"})
	require.NoError(t, err)
	original, err := b.GetNode("test:a")
	require.NoError(t, err)
	original.ChunkEmbeddings = [][]float32{{1, 2}, {3, 4}}
	original.EmbedMeta = map[string]any{"chunk_count": 2}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(original))
	original, err = b.GetNode("test:a")
	require.NoError(t, err)
	var mu sync.Mutex
	var events []StoredEntityPublication
	var initial []StoredEntityChange
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
		err := snapshot.ForEach(func(entry StoredEntitySnapshotEntry) error {
			change, err := entry.Read()
			if err != nil {
				return err
			}
			initial = append(initial, change)
			return nil
		})
		return func(event StoredEntityPublication) (func(), error) {
			return func() { mu.Lock(); events = append(events, event); mu.Unlock() }, nil
		}, err
	}, nil))
	require.Len(t, initial, 2)
	var hydrated *Node
	for _, change := range initial {
		if change.AfterNode.ID == "test:a" {
			hydrated = change.AfterNode
		}
	}
	assertStoredNodeImage(t, original, hydrated, "initial snapshot includes full separate vectors")
	replacement := CopyNode(original)
	replacement.Properties["text"] = "new"
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: replacement}))
	written, err := b.GetNode("test:a")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Len(t, events[0].Changes, 1)
	assertStoredNodeImage(t, original, events[0].Changes[0].BeforeNode)
	assertStoredNodeImage(t, written, events[0].Changes[0].AfterNode)
	require.NotEmpty(t, events[0].LogicalID)
	edge := testEdge("test:r", "test:a", "test:b", "R")
	edge.Properties = map[string]any{"n": []any{int64(1), int64(2)}}
	require.NoError(t, b.CreateEdge(edge))
	storedEdge, err := b.GetEdge("test:r")
	require.NoError(t, err)
	require.Equal(t, storedEdge, events[1].Changes[0].AfterEdge)
	require.NoError(t, b.DeleteNode("test:a"))
	require.Len(t, events, 3)
	require.Len(t, events[2].Changes, 2, "cascade includes each stored edge image")
	var nodeDeleted, edgeDeleted bool
	for _, change := range events[2].Changes {
		if change.BeforeNode != nil {
			nodeDeleted = true
			assertStoredNodeImage(t, written, change.BeforeNode)
			require.Nil(t, change.AfterNode)
		}
		if change.BeforeEdge != nil {
			edgeDeleted = true
			require.Equal(t, storedEdge, change.BeforeEdge)
			require.Nil(t, change.AfterEdge)
		}
	}
	require.True(t, nodeDeleted)
	require.True(t, edgeDeleted)
}

func TestStoredEntityPublicationRejectedBeforePhysical(t *testing.T) {
	b := newTestEngine(t)
	rejected := errors.New("sizing rejected")
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return nil, rejected }, nil
	}, nil))
	_, err := b.CreateNode(&Node{ID: "test:rejected"})
	require.ErrorIs(t, err, rejected)
	_, err = b.GetNode("test:rejected")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStoredEntityPublicationBufferedAckOnce(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	var events []StoredEntityPublication
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(event StoredEntityPublication) (func(), error) {
			return func() { events = append(events, event) }, nil
		}, nil
	}, nil))
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"v": "one"}})
		return err
	})
	require.Len(t, events, 1)
	first, err := b.GetNode("test:a")
	require.NoError(t, err)
	assertStoredNodeImage(t, first, events[0].Changes[0].AfterNode)
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		node, err := tx.GetNode("test:a")
		if err != nil {
			return err
		}
		node.Properties["v"] = "two"
		return tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
	})
	require.Len(t, events, 2)
	assertStoredNodeImage(t, first, events[1].Changes[0].BeforeNode, "prior is previous acknowledged overlay")
	second, err := b.GetNode("test:a")
	require.NoError(t, err)
	assertStoredNodeImage(t, second, events[1].Changes[0].AfterNode)
	require.NoError(t, b.FlushWriteBehind())
	require.Len(t, events, 2, "physical replay does not charge acknowledged commits again")
}

func assertStoredNodeImage(t *testing.T, expected, actual *Node, message ...any) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	clean := func(source *Node) Node {
		node := *source
		node.CopyEmbeddingIdentityFrom(nil)
		return node
	}
	require.Equal(t, clean(expected), clean(actual), message...)
}

// Keep a real physical commit registered at its canonical owner but leave its
// derived publication pending. This proves snapshot upgrade drains old callbacks
// before resetting the ledger, without a scheduler-dependent pause or new hook.
func TestStoredEntityInitializationDrainsEarlierPublication(t *testing.T) {
	b := newTestEngine(t)
	count := 0
	observer := func(event StoredEntityPublication) (func(), error) {
		delta := 0
		for _, change := range event.Changes {
			if change.BeforeNode != nil {
				delta--
			}
			if change.AfterNode != nil {
				delta++
			}
		}
		return func() { count += delta }, nil
	}
	initialize := func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
		next := 0
		err := snapshot.ForEach(func(entry StoredEntitySnapshotEntry) error {
			if entry.Kind == StoredEntityNodeKind {
				next++
			}
			return nil
		})
		count = next
		return observer, err
	}
	require.NoError(t, b.InitializeStoredEntityObserver("test", initialize, nil))
	txn, readTs, err := b.db.beginTxn(true)
	require.NoError(t, err)
	defer b.db.endRead(readTs)
	defer txn.Discard()
	node := &Node{ID: "test:pending"}
	data, _, err := b.encodeNode(txn, node)
	require.NoError(t, err)
	require.NoError(t, txn.Set(nodeKey(node.ID), data))
	cw := b.newCommitWriter(b.db, txn)
	cw.publishKeys = []any{node.ID}
	ts, err := b.db.commitShared(txn, cw.publication())
	require.NoError(t, err)
	require.Equal(t, 0, count, "physical commit has not run its registered publication")
	require.NoError(t, b.InitializeStoredEntityObserver("test", initialize, nil))
	require.Equal(t, 1, count, "snapshot includes the physical commit once")
	b.db.oracle.stage.apply(ts)
	require.Equal(t, 1, count, "old observer cannot add the same commit after snapshot reset")
}

func TestStoredEntityInitializationIncludesInflightSidecarOnce(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:sidecar", Properties: map[string]any{"text": "source"}})
	require.NoError(t, err)
	node, err := b.GetNode("test:sidecar")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.EmbedMeta = map[string]any{"chunk_count": 1}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
	var size atomic.Int64
	length := func(node *Node) int64 {
		var n int64
		if node != nil {
			for _, chunk := range node.ChunkEmbeddings {
				n += int64(len(chunk))
			}
		}
		return n
	}
	observer := func(event StoredEntityPublication) (func(), error) {
		var delta int64
		for _, change := range event.Changes {
			delta += length(change.AfterNode) - length(change.BeforeNode)
		}
		return func() { size.Add(delta) }, nil
	}
	initialize := func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
		var n int64
		err := snapshot.ForEach(func(entry StoredEntitySnapshotEntry) error {
			change, err := entry.Read()
			if err != nil {
				return err
			}
			n += length(change.AfterNode)
			return nil
		})
		size.Store(n)
		return observer, err
	}
	require.NoError(t, b.InitializeStoredEntityObserver("test", initialize, nil))
	require.Equal(t, int64(2), size.Load())
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	restore := setPublicationStageHook(func(uint64) { once.Do(func() { close(entered); <-release }) })
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	sidecarDone := make(chan error, 1)
	initializeDone := make(chan error, 1)
	t.Cleanup(func() {
		finish()
		restore()
		select {
		case <-sidecarDone:
		case <-time.After(5 * time.Second):
			t.Error("sidecar worker did not stop")
		}
		select {
		case <-initializeDone:
		case <-time.After(5 * time.Second):
			t.Error("initializer worker did not stop")
		}
	})
	node, err = b.GetNode("test:sidecar")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{3, 4, 5, 6}}
	go func() { sidecarDone <- b.UpdateNodeEmbeddingSidecar(node) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not reach publication")
	}
	go func() { initializeDone <- b.InitializeStoredEntityObserver("test", initialize, nil) }()
	// Await the actual exclusive lifecycle cut, not elapsed time. The pending
	// old stage keeps this cut open until release, including on a slow host.
	deadline := time.After(5 * time.Second)
	for b.writeBarrier.TryRLock() {
		b.writeBarrier.RUnlock()
		select {
		case <-deadline:
			t.Fatal("initializer did not acquire lifecycle cut")
		default:
			runtime.Gosched()
		}
	}
	finish()
	select {
	case err := <-sidecarDone:
		sidecarDone <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not finish")
	}
	select {
	case err := <-initializeDone:
		initializeDone <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("initializer did not finish")
	}
	require.Equal(t, int64(4), size.Load(), "snapshot upgrade includes inflight raw sidecar exactly once")
}

type storedReadTestBoundary struct {
	read func(StoredEntityReadKind, string, func() error) error
}

func (b storedReadTestBoundary) WithStoredEntityRead(kind StoredEntityReadKind, id string, read func() error) error {
	return b.read(kind, id, read)
}

func TestStoredEntityReadBoundaryUsesActualNamespaceRead(t *testing.T) {
	b := newTestEngine(t)
	var calls []StoredEntityReadKind
	injected := errors.New("snapshot-bound read failed")
	fail := false
	boundary := storedReadTestBoundary{read: func(kind StoredEntityReadKind, id string, read func() error) error {
		calls = append(calls, kind)
		if kind == StoredEntityNodeSnapshotScan || kind == StoredEntityEdgeSnapshotScan {
			require.Equal(t, "test", id)
		} else {
			require.Equal(t, "test:a", id)
		}
		if fail && kind == StoredEntityNodePriorRead {
			return injected
		}
		return read()
	}}
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
		err := snapshot.ForEach(func(StoredEntitySnapshotEntry) error { return nil })
		return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, err
	}, boundary))
	require.Equal(t, []StoredEntityReadKind{StoredEntityNodeSnapshotScan, StoredEntityEdgeSnapshotScan}, calls)
	fail = true
	_, err := b.CreateNode(&Node{ID: "test:a"})
	require.ErrorIs(t, err, injected)
	_, err = b.GetNode("test:a")
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, []StoredEntityReadKind{StoredEntityNodeSnapshotScan, StoredEntityEdgeSnapshotScan, StoredEntityNodePriorRead}, calls)
	fail = false
	_, err = b.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	require.Len(t, calls, 4, "successful continuation runs its actual prior read once")
	_, err = b.CreateNode(&Node{ID: "other:a"})
	require.NoError(t, err)
	require.Len(t, calls, 4, "namespace boundary never intercepts another namespace")
}

func TestStoredEntityPublicationLargeSidecarAndPrecommitAbort(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint("reject=", reject), func(t *testing.T) {
			b := openLargeCommitTestEngine(t, t.TempDir())
			_, err := b.CreateNode(&Node{ID: "test:large", Labels: []string{"Doc"}, Properties: map[string]any{"text": "source"}})
			require.NoError(t, err)
			node, err := b.GetNode("test:large")
			require.NoError(t, err)
			node.ChunkEmbeddings = [][]float32{{1, 2}}
			node.EmbedMeta = map[string]any{"chunk_count": 1}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(node))
			original, err := b.GetNode(node.ID)
			require.NoError(t, err)
			failure := errors.New("prepared size rejected")
			var prepared StoredEntityPublication
			published := 0
			require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
				return func(event StoredEntityPublication) (func(), error) {
					prepared = event
					if reject {
						return nil, failure
					}
					return func() { published++ }, nil
				}, nil
			}, nil))
			batches := 0
			defer setLargeCommitBatchHook(func(int) error { batches++; return nil })()
			node = CopyNode(original)
			node.ChunkEmbeddings = make([][]float32, 20000)
			for index := range node.ChunkEmbeddings {
				node.ChunkEmbeddings[index] = make([]float32, 16)
				seed := uint32(index + 1)
				for coordinate := range node.ChunkEmbeddings[index] {
					seed ^= seed << 13
					seed ^= seed >> 17
					seed ^= seed << 5
					node.ChunkEmbeddings[index][coordinate] = math.Float32frombits(0x3f000000 | (seed & 0x007fffff))
				}
			}
			node.EmbedMeta = map[string]any{"chunk_count": 20000}
			err = b.UpdateNodeEmbeddingSidecar(node)
			require.Positive(t, batches, "public sidecar spans physical batches")
			require.Len(t, prepared.Changes, 1)
			assertStoredNodeImage(t, original, prepared.Changes[0].BeforeNode)
			require.Len(t, prepared.Changes[0].AfterNode.ChunkEmbeddings, 20000, "final capture reads all private earlier batches")
			stored, readErr := b.GetNode(node.ID)
			require.NoError(t, readErr)
			if reject {
				require.ErrorIs(t, err, failure)
				require.Zero(t, published)
				assertStoredNodeImage(t, original, stored)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, published)
				assertStoredNodeImage(t, stored, prepared.Changes[0].AfterNode)
			}
			requireNoLargeCommitIntent(t, b)
		})
	}
}

// A poisoned vector codec is a deterministic read detector: identity enumeration
// must not invoke it, while an explicit complete-image Read must expose it.
func TestStoredEntitySnapshotIdentityDefersVectorRead(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:lazy", ChunkEmbeddings: [][]float32{make([]float32, 16384)}})
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Set(embeddingKey("test:lazy", 0), []byte{0xff}) }))
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
		var entries []StoredEntitySnapshotEntry
		require.NoError(t, snapshot.ForEach(func(entry StoredEntitySnapshotEntry) error { entries = append(entries, entry); return nil }))
		require.Len(t, entries, 1)
		require.Equal(t, StoredEntityNodeKind, entries[0].Kind)
		require.Equal(t, "test:lazy", entries[0].ID)
		_, err := entries[0].Read()
		require.Error(t, err, "complete image read must reach the vector codec")
		return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, nil
	}, nil))
}
