package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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
	svc, err := db.EnsureSearchIndexes(context.Background(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	require.True(t, svc.IsReady())
	return db, svc
}

// installConsumerBarrier routes the DB's writes through the shared consumer
// barrier below its namespace until cleanup. The update and delete arms stay
// independent because a case pauses only one of them, and a failed test still
// releases a paused writer.
func installConsumerBarrier(t *testing.T, db *DB) *storagefault.ConsumerBarrier {
	t.Helper()
	// Startup's background build reads db.storage; let it finish first.
	db.bgWg.Wait()
	db.mu.Lock()
	original := db.storage
	namespaced := original.(*storage.NamespacedEngine)
	barrier := storagefault.NewConsumerBarrier(namespaced.GetInnerEngine())
	db.storage = storage.NewNamespacedEngine(barrier, namespaced.Namespace())
	db.mu.Unlock()
	t.Cleanup(func() {
		db.mu.Lock()
		db.storage = original
		db.mu.Unlock()
	})
	t.Cleanup(barrier.ReleaseWrites)
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

// A write consumer's refresh must leave search matching storage when a delete
// races the write. DB.UpdateNode refreshes search after committing, so a delete
// that commits in between must stay deleted in search (#1482 round 2
// regression); DB.DeleteNode refreshes search after its storage delete, so an
// update that commits before that delete must not leave the deleted node
// indexed. Both orders hold the concurrent write at the barrier, let the other
// write commit, then release it so its refresh reads storage after both writes.
func TestDBDeleteAndUpdateRaceLeavesSearchMatchingStorage(t *testing.T) {
	cases := []struct {
		name string
		// pauseUpdate arms the paused update first and lets the other write
		// commit while it is held; false pauses the delete instead.
		pauseUpdate bool
		// pausedDone is the barrier failure when the paused write never
		// reaches its commit/store boundary.
		pausedDone string
		// otherWrite runs, and must succeed, while the paused write is held.
		otherWrite func(ctx context.Context, db *DB) error
		// failure is the assertion message for the leftover search index entry.
		failure string
	}{
		{
			name:        "update then delete",
			pauseUpdate: true,
			pausedDone:  "update did not reach its barrier",
			otherWrite:  func(ctx context.Context, db *DB) error { return db.DeleteNode(ctx, "source") },
			failure:     "an update's late index call resurrected a deleted node (counted without a search, which would clean the orphan)",
		},
		{
			name:        "delete then update",
			pauseUpdate: false,
			pausedDone:  "delete did not reach its barrier",
			otherWrite: func(ctx context.Context, db *DB) error {
				_, err := db.UpdateNode(ctx, "source", map[string]interface{}{"content": "bravoword"})
				return err
			},
			failure: "a node deleted from storage stayed indexed (counted without a search, which would clean the orphan)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, svc := openConsumerDB(t, false)
			_, err := db.CreateNodeWithID(ctx, "source", []string{"Doc"}, map[string]interface{}{"content": "alphaword"})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return consumerDBHits(t, svc, "alphaword") == 1 }, 5*time.Second, 20*time.Millisecond)
			barrier := installConsumerBarrier(t, db)
			// The paused write is the update in one case and the delete in the
			// other; the arm and its Entered channel follow that choice.
			var entered chan struct{}
			if tc.pauseUpdate {
				barrier.Armed.Store(true)
				entered = barrier.Entered
			} else {
				barrier.DeleteArm.Store(true)
				entered = barrier.DeleteEntered
			}
			pausedWrite := func(ctx context.Context) error {
				if tc.pauseUpdate {
					_, err := db.UpdateNode(ctx, "source", map[string]interface{}{"content": "bravoword"})
					return err
				}
				return db.DeleteNode(ctx, "source")
			}
			done := make(chan error, 1)
			go func() { done <- pausedWrite(ctx) }()
			runtimeFixtureOwner.WaitForBarrier(t, entered, 5*time.Second, barrier.ReleaseWrites, tc.pausedDone)
			require.NoError(t, tc.otherWrite(ctx, db))
			barrier.ReleaseWrites()
			require.NoError(t, <-done)
			settleQueuedRefreshes()
			_, err = db.storage.GetNode("source")
			require.ErrorIs(t, err, storage.ErrNotFound)
			require.Zero(t, svc.FulltextDocCount(), tc.failure)
		})
	}
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
				_ = db.storage.UpdateNode(storage.NodeUpdateRequest{After: node})
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

// seedStaleEmbeddingCallback creates "source" holding alphaword, reads it back,
// and returns the qualified embedding callback the worker would publish for it
// so a case can make that callback stale before delivering it.
func seedStaleEmbeddingCallback(t *testing.T, db *DB) *storage.Node {
	t.Helper()
	vector := make([]float32, db.config.Memory.EmbeddingDimensions)
	vector[0] = 1
	_, err := db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alphaword"}})
	require.NoError(t, err)
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = [][]float32{vector}
	embedded.ID = storage.NodeID(db.defaultDatabaseName() + ":source")
	return embedded
}

// A late embedding callback must not resurrect a deleted node or replace a
// newer update's index entry. Both cases seed the same stale callback and only
// the newer write committed after it differs, so each case keeps its own
// terminal state assertion.
func TestLateEmbeddingCallbackCannotOverrideNewerWrite(t *testing.T) {
	cases := []struct {
		name string
		// newerWrite commits the write that must win before the stale callback
		// is delivered.
		newerWrite func(ctx context.Context, db *DB) error
		// hits is the declared per-word search hit count after delivery.
		hits map[string]int
	}{
		{
			name:       "deleted after callback snapshot",
			newerWrite: func(ctx context.Context, db *DB) error { return db.storage.DeleteNode("source") },
			hits:       map[string]int{"alphaword": 0},
		},
		{
			name: "updated after callback snapshot",
			newerWrite: func(ctx context.Context, db *DB) error {
				stored, err := db.storage.GetNode("source")
				if err != nil {
					return err
				}
				newer := storage.CopyNode(stored)
				newer.Properties["content"] = "bravoword"
				return db.storage.UpdateNode(storage.NodeUpdateRequest{After: newer})
			},
			hits: map[string]int{"bravoword": 1, "alphaword": 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, svc := openConsumerDB(t, false)
			embedded := seedStaleEmbeddingCallback(t, db)
			require.NoError(t, tc.newerWrite(ctx, db))
			settleQueuedRefreshes()
			db.onNodeEmbedded(embedded)
			settleQueuedRefreshes()
			require.Zero(t, svc.EmbeddingCount(), "a late embedding callback resurrected a vector over a newer write")
			for word, hits := range tc.hits {
				require.Equal(t, hits, consumerDBHits(t, svc, word), "%q hits", word)
			}
		})
	}
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
