package nornicdb

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// refreshReadBarrier is the search service's storage. It pauses the first
// armed read of a node whose result matches observed: the refresh has reserved
// its generation and read storage, and has not applied yet. No goroutine
// stacks are inspected.
type refreshReadBarrier struct {
	storage.Engine
	observed func(*storage.Node, error) bool
	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	opened   sync.Once
}

func (b *refreshReadBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if b.armed.Load() && b.observed(node, err) && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return node, err
}

func (b *refreshReadBarrier) open() { b.opened.Do(func() { close(b.release) }) }

func (b *refreshReadBarrier) wait(t *testing.T) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		b.open()
		t.Fatal("the refresh did not read storage")
	}
}

func storedContent(word string) func(*storage.Node, error) bool {
	return func(node *storage.Node, err error) bool {
		return err == nil && node != nil && node.Properties["content"] == word
	}
}

func newMutationOrderDB(t *testing.T) (*DB, *dbSearchService, storage.Engine, *refreshReadBarrier) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "test")
	barrier := &refreshReadBarrier{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
	svc := search.NewService(barrier, 2, nil)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	entry := &dbSearchService{dbName: "test", engine: store, svc: svc, pendingFlushDelay: time.Hour}
	db := &DB{config: DefaultConfig(), storage: store, baseStorage: base, searchServices: map[string]*dbSearchService{"test": entry}}
	t.Cleanup(func() {
		barrier.open()
		entry.pendingFlushMu.Lock()
		if entry.pendingFlushTimer != nil {
			entry.pendingFlushTimer.Stop()
		}
		entry.pendingFlushMu.Unlock()
		_ = svc.Close()
		_ = base.Close()
	})
	return db, entry, store, barrier
}

func applyQueued(t *testing.T, entry *dbSearchService) {
	t.Helper()
	nodes, _ := entry.drainPending()
	for _, mutation := range nodes {
		require.NoError(t, entry.svc.RefreshNodeWithMutation(mutation))
	}
}

func fulltextHits(t *testing.T, svc *search.Service, word string) int {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.MinRRFScore = 0
	response, err := svc.Search(context.Background(), word, nil, opts)
	require.NoError(t, err)
	return len(response.Results)
}

// A drained refresh applied after newer event, direct or embedded work, and
// again after a deletion, changes nothing.
func TestSearchMutationDrainsPreserveNewerMutations(t *testing.T) {
	for _, newerPath := range []string{"event", "direct", "embedded"} {
		t.Run("newer="+newerPath, func(t *testing.T) {
			db, entry, store, _ := newMutationOrderDB(t)
			old := &storage.Node{ID: "source", Properties: map[string]any{"content": "obsolete"}, ChunkEmbeddings: [][]float32{{1, 0}}}
			_, err := store.CreateNode(old)
			require.NoError(t, err)
			db.queueSearchRefreshEvent(searchNodeWrite, "test:source", storage.CaptureVectorWorkOrigin())
			stale, _ := entry.drainPending()
			newer := storage.CopyNode(old)
			newer.Properties["content"] = "current"
			newer.ChunkEmbeddings = [][]float32{{0, 1}}
			require.NoError(t, store.UpdateNode(newer))
			switch newerPath {
			case "event":
				db.indexNodeFromEvent(&storage.Node{ID: "test:source"}, storage.CaptureVectorWorkOrigin())
				applyQueued(t, entry)
			case "direct":
				require.NoError(t, entry.svc.RefreshNode("source", storage.CaptureVectorWorkOrigin()))
			case "embedded":
				require.True(t, db.publishEmbeddedNode(&storage.Node{ID: "test:source"}, storage.CaptureVectorWorkOrigin()))
			}
			require.Equal(t, 1, entry.svc.EmbeddingCount())
			require.NoError(t, store.DeleteNode("source"))
			db.queueSearchRefreshEvent(searchNodeDelete, "test:source", storage.CaptureVectorWorkOrigin())
			applyQueued(t, entry)
			require.NoError(t, entry.svc.RefreshNodeWithMutation(stale["source"]))
			require.Zero(t, entry.svc.EmbeddingCount(), "an old drain resurrected a deleted vector")
			require.Zero(t, fulltextHits(t, entry.svc, "obsolete"))
		})
	}
	t.Run("retired entry rejects storage events", func(t *testing.T) {
		db, entry, _, _ := newMutationOrderDB(t)
		entry.retire()
		for _, kind := range []searchRefreshEventKind{searchNodeWrite, searchNodeDelete, searchRelationshipChange} {
			db.queueSearchRefreshEvent(kind, "test:source", storage.CaptureVectorWorkOrigin())
		}
		require.False(t, entry.hasPending())
		entry.pendingFlushMu.Lock()
		defer entry.pendingFlushMu.Unlock()
		require.Nil(t, entry.pendingFlushTimer)
	})
}

// A refresh that read the node before a newer delete and its refresh applied
// must not resurrect it. The barrier pauses the older refresh after its read.
func TestRefreshReadBeforeNewerDeleteCannotResurrect(t *testing.T) {
	db, entry, store, barrier := newMutationOrderDB(t)
	_, err := store.CreateNode(&storage.Node{ID: "source", Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	barrier.observed = storedContent("alpha")
	barrier.armed.Store(true)
	done := make(chan bool, 1)
	go func() {
		done <- db.publishEmbeddedNode(&storage.Node{ID: "test:source"}, storage.CaptureVectorWorkOrigin())
	}()
	barrier.wait(t)
	require.NoError(t, store.DeleteNode("source"))
	db.queueSearchRefreshEvent(searchNodeDelete, "test:source", storage.CaptureVectorWorkOrigin())
	applyQueued(t, entry)
	barrier.open()
	require.True(t, <-done)
	require.Zero(t, entry.svc.EmbeddingCount(), "a refresh that read before the delete resurrected the vector")
}

// A refresh that read older content must not replace a newer write's entry.
func TestRefreshReadBeforeNewerWriteCannotOverwrite(t *testing.T) {
	db, entry, store, barrier := newMutationOrderDB(t)
	node := &storage.Node{ID: "source", Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	barrier.observed = storedContent("alpha")
	barrier.armed.Store(true)
	done := make(chan bool, 1)
	go func() {
		done <- db.publishEmbeddedNode(&storage.Node{ID: "test:source"}, storage.CaptureVectorWorkOrigin())
	}()
	barrier.wait(t)
	newer := storage.CopyNode(node)
	newer.Properties["content"] = "bravo"
	require.NoError(t, store.UpdateNode(newer))
	db.indexNodeFromEvent(&storage.Node{ID: "test:source"}, storage.CaptureVectorWorkOrigin())
	applyQueued(t, entry)
	barrier.open()
	require.True(t, <-done)
	require.Equal(t, 1, fulltextHits(t, entry.svc, "bravo"))
	require.Zero(t, fulltextHits(t, entry.svc, "alpha"), "a refresh that read older content replaced the newer entry")
}

// Concurrent storage writes, events, embedded publication, direct refreshes
// and drains converge on storage; deleted nodes stay out.
func TestSearchMutationConcurrentPublicationAndDrains(t *testing.T) {
	db, entry, store, _ := newMutationOrderDB(t)
	for i := range 8 {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
		require.NoError(t, err)
	}
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range 32 {
				id := storage.NodeID(fmt.Sprintf("n%d", i%8))
				node := &storage.Node{ID: id, Properties: map[string]any{"content": fmt.Sprintf("payload %d %d", worker, i)}, ChunkEmbeddings: [][]float32{{1, 0}}}
				_ = store.UpdateNode(node)
				qualified := &storage.Node{ID: "test:" + id}
				db.indexNodeFromEvent(qualified, storage.CaptureVectorWorkOrigin())
				stale, _ := entry.drainPending()
				_ = db.publishEmbeddedNode(qualified, storage.CaptureVectorWorkOrigin())
				if i%2 == 0 {
					_ = entry.svc.RefreshNode(id, storage.CaptureVectorWorkOrigin())
				}
				for _, mutation := range stale {
					_ = entry.svc.RefreshNodeWithMutation(mutation)
				}
			}
		}()
	}
	workers.Wait()
	stale, _ := entry.drainPending()
	for i := range 8 {
		id := storage.NodeID(fmt.Sprintf("n%d", i))
		require.NoError(t, store.DeleteNode(id))
		db.queueSearchRefreshEvent(searchNodeDelete, "test:"+string(id), storage.CaptureVectorWorkOrigin())
	}
	applyQueued(t, entry)
	for _, mutation := range stale {
		require.NoError(t, entry.svc.RefreshNodeWithMutation(mutation))
	}
	require.Zero(t, entry.svc.EmbeddingCount())
}
