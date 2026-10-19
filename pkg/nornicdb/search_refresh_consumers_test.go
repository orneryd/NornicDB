package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// consumerWriteBarrier pauses the first armed UpdateNode after its commit, or the
// first armed DeleteNode before it, so a concurrent write lands between the
// storage write and the search refresh that follows it. No goroutine stacks are
// inspected.
type consumerWriteBarrier struct {
	storage.Engine
	updateArmed atomic.Bool
	deleteArmed atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	opened      sync.Once
}

func (b *consumerWriteBarrier) open() { b.opened.Do(func() { close(b.release) }) }

func (b *consumerWriteBarrier) UpdateNode(node *storage.Node) error {
	err := b.Engine.UpdateNode(node)
	if err == nil && b.updateArmed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return err
}

func (b *consumerWriteBarrier) DeleteNode(id storage.NodeID) error {
	if b.deleteArmed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return b.Engine.DeleteNode(id)
}

func (b *consumerWriteBarrier) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		b.open()
		t.Fatal(what + " did not reach its barrier")
	}
}

func openConsumerDB(t *testing.T, relationships bool) (*DB, *search.Service) {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.SearchRelationshipsEnabled = relationships
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc, err := db.EnsureSearchIndexesBuilt(context.Background(), db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	require.True(t, svc.IsReady())
	return db, svc
}

// installConsumerBarrier routes the DB's writes through a barrier below its
// namespace until cleanup. A failed test still releases a paused writer.
func installConsumerBarrier(t *testing.T, db *DB) *consumerWriteBarrier {
	t.Helper()
	// Startup's background build reads db.storage; let it finish first.
	db.bgWg.Wait()
	db.mu.Lock()
	original := db.storage
	namespaced := original.(*storage.NamespacedEngine)
	barrier := &consumerWriteBarrier{Engine: namespaced.GetInnerEngine(), entered: make(chan struct{}), release: make(chan struct{})}
	db.storage = storage.NewNamespacedEngine(barrier, namespaced.Namespace())
	db.mu.Unlock()
	t.Cleanup(func() {
		db.mu.Lock()
		db.storage = original
		db.mu.Unlock()
	})
	t.Cleanup(barrier.open)
	return barrier
}

func consumerDBHits(t *testing.T, svc *search.Service, word string) int {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.MinRRFScore = 0
	response, err := svc.Search(context.Background(), word, nil, opts)
	require.NoError(t, err)
	return len(response.Results)
}

// settleQueuedRefreshes outlasts the deferred search flush.
func settleQueuedRefreshes() { time.Sleep(4 * searchMutationDebounceDelay) }

// DB.UpdateNode refreshes search after committing; a delete that commits
// in between must stay deleted in search (#1482 round 2 regression).
func TestDBUpdateIndexCannotResurrectDelete(t *testing.T) {
	ctx := context.Background()
	db, svc := openConsumerDB(t, false)
	_, err := db.CreateNodeWithID(ctx, "source", []string{"Doc"}, map[string]interface{}{"content": "alphaword"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return consumerDBHits(t, svc, "alphaword") == 1 }, 5*time.Second, 20*time.Millisecond)
	barrier := installConsumerBarrier(t, db)
	barrier.updateArmed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := db.UpdateNode(ctx, "source", map[string]interface{}{"content": "bravoword"})
		done <- err
	}()
	barrier.wait(t, "update")
	require.NoError(t, db.DeleteNode(ctx, "source"))
	barrier.open()
	require.NoError(t, <-done)
	settleQueuedRefreshes()
	_, err = db.storage.GetNode("source")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.FulltextDocCount(), "an update's late index call resurrected a deleted node (counted without a search, which would clean the orphan)")
}

// DB.DeleteNode refreshes search after its storage delete; an update that
// commits before that delete must not leave the deleted node indexed.
func TestDBDeleteWithConcurrentUpdateLeavesSearch(t *testing.T) {
	ctx := context.Background()
	db, svc := openConsumerDB(t, false)
	_, err := db.CreateNodeWithID(ctx, "source", []string{"Doc"}, map[string]interface{}{"content": "alphaword"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return consumerDBHits(t, svc, "alphaword") == 1 }, 5*time.Second, 20*time.Millisecond)
	barrier := installConsumerBarrier(t, db)
	barrier.deleteArmed.Store(true)
	done := make(chan error, 1)
	go func() { done <- db.DeleteNode(ctx, "source") }()
	barrier.wait(t, "delete")
	_, err = db.UpdateNode(ctx, "source", map[string]interface{}{"content": "bravoword"})
	require.NoError(t, err)
	barrier.open()
	require.NoError(t, <-done)
	settleQueuedRefreshes()
	_, err = db.storage.GetNode("source")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.FulltextDocCount(), "a node deleted from storage stayed indexed (counted without a search, which would clean the orphan)")
}

// Storage events from concurrent autocommit writers (external writers,
// replication apply) must leave search matching the stored node.
func TestStorageEventsConvergeUnderConcurrentWriters(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	dims := db.config.Memory.EmbeddingDimensions
	vector := make([]float32, dims)
	vector[0] = 1
	_, err := db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "seedword"}, ChunkEmbeddings: [][]float32{vector}})
	require.NoError(t, err)
	for round := range 20 {
		var writers sync.WaitGroup
		for writer := range 8 {
			writers.Add(1)
			go func() {
				defer writers.Done()
				node := &storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": fmt.Sprintf("word%dx%d", round, writer)}, ChunkEmbeddings: [][]float32{vector}}
				_ = db.storage.UpdateNode(node)
			}()
		}
		writers.Wait()
		stored, err := db.storage.GetNode("source")
		require.NoError(t, err)
		word := stored.Properties["content"].(string)
		settleQueuedRefreshes()
		require.Equal(t, 1, consumerDBHits(t, svc, word), "round %d: search does not match the stored node (%s)", round, word)
	}
}

// A late embedding callback for a node deleted meanwhile must not resurrect it.
func TestLateEmbeddingCallbackCannotResurrectDelete(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	dims := db.config.Memory.EmbeddingDimensions
	vector := make([]float32, dims)
	vector[0] = 1
	_, err := db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alphaword"}})
	require.NoError(t, err)
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = [][]float32{vector}
	embedded.ID = storage.NodeID(db.defaultDatabaseName() + ":source")
	require.NoError(t, db.storage.DeleteNode("source"))
	settleQueuedRefreshes()
	db.onNodeEmbedded(embedded)
	settleQueuedRefreshes()
	require.Zero(t, svc.EmbeddingCount(), "a late embedding callback resurrected a deleted vector")
	require.Zero(t, consumerDBHits(t, svc, "alphaword"))
}

// A late embedding callback must not replace a newer update's index entry.
func TestLateEmbeddingCallbackCannotOverwriteNewerUpdate(t *testing.T) {
	db, svc := openConsumerDB(t, false)
	dims := db.config.Memory.EmbeddingDimensions
	vector := make([]float32, dims)
	vector[0] = 1
	_, err := db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alphaword"}})
	require.NoError(t, err)
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = [][]float32{vector}
	embedded.ID = storage.NodeID(db.defaultDatabaseName() + ":source")
	newer := storage.CopyNode(stored)
	newer.Properties["content"] = "bravoword"
	require.NoError(t, db.storage.UpdateNode(newer))
	settleQueuedRefreshes()
	db.onNodeEmbedded(embedded)
	settleQueuedRefreshes()
	require.Equal(t, 1, consumerDBHits(t, svc, "bravoword"), "the stored content is not searchable")
	require.Zero(t, consumerDBHits(t, svc, "alphaword"), "a late embedding callback replaced a newer update")
}

// Deleting a relationship removes its client-supplied vector from search.
func TestDBRelationshipDeleteRemovesVector(t *testing.T) {
	ctx := context.Background()
	db, svc := openConsumerDB(t, true)
	vector := make([]float64, db.config.Memory.EmbeddingDimensions)
	vector[0] = 1
	_, err := db.ExecuteCypher(ctx, "CREATE (:Doc {name: 'a'})-[:CITES {vec: $v}]->(:Doc {name: 'b'})", map[string]interface{}{"v": vector})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return svc.HasRelationshipVectorEntries("CITES", "vec") }, 5*time.Second, 20*time.Millisecond, "the relationship vector is indexed")
	_, err = db.ExecuteCypher(ctx, "MATCH ()-[r:CITES]->() DELETE r", nil)
	require.NoError(t, err)
	settleQueuedRefreshes()
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a deleted relationship's vector stayed indexed")
}
