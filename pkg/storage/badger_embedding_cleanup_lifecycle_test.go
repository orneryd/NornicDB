package storage

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingCleanup_TransactionInlineReplacement(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:review-inline")
	_, err := engine.CreateNode(separatelyStoredNode(id))
	require.NoError(t, err)
	require.Equal(t, 16, countEmbeddingKeys(t, engine, id))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	require.NoError(t, tx.Commit())
	require.Zero(t, countEmbeddingKeys(t, engine, id), "transactional shrink must delete all old chunks")
	engine.invalidateCachesAfterRestore()
	got, err := engine.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 0, 0}}, got.ChunkEmbeddings)
}

func TestEmbeddingCleanup_TransactionCreateThenDelete(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:review-create-delete")
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(separatelyStoredNode(id))
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode(id))
	require.NoError(t, tx.Commit())
	require.Zero(t, countEmbeddingKeys(t, engine, id), "delete must remove chunks staged by this transaction")
}

func TestEmbeddingCleanup_LateChunkWriter(t *testing.T) {
	for _, writer := range []string{"create", "upsert", "update"} {
		for _, action := range []string{"delete", "inline"} {
			t.Run(writer+"/"+action, func(t *testing.T) {
				engine := newTestEngine(t)
				id := NodeID("test:review-late")
				if writer == "update" {
					_, err := engine.CreateNode(separatelyStoredNode(id))
					require.NoError(t, err)
				}
				bodyCommitted := make(chan struct{})
				resume := make(chan struct{})
				defer func() {
					select {
					case <-resume:
					default:
						close(resume)
					}
				}()
				var paused atomic.Bool
				restore := setCommitTailHook(func() {
					if paused.CompareAndSwap(false, true) {
						close(bodyCommitted)
						<-resume
					}
				})
				defer restore()
				done := make(chan error, 1)
				go func() {
					if writer == "create" {
						_, err := engine.CreateNode(separatelyStoredNode(id))
						done <- err
					} else {
						done <- engine.UpdateNode(separatelyStoredNode(id))
					}
				}()
				select {
				case <-bodyCommitted:
				case <-time.After(10 * time.Second):
					t.Fatal("writer did not commit its body")
				}
				require.Equal(t, separateEmbeddingChunks, countEmbeddingKeys(t, engine, id), "body and chunks must be published together")
				if action == "delete" {
					require.NoError(t, engine.DeleteNode(id))
				} else {
					require.NoError(t, engine.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
				}
				require.Zero(t, countEmbeddingKeys(t, engine, id), "cleanup completed before the old writer resumed")
				close(resume)
				require.NoError(t, <-done)
				require.Zero(t, countEmbeddingKeys(t, engine, id), "earlier body writer must not resurrect separately stored chunks")
			})
		}
	}
}

func TestEmbeddingCleanup_AllKeyShapes(t *testing.T) {
	for _, action := range []string{"delete", "inline", "transaction-delete", "transaction-inline"} {
		t.Run(action, func(t *testing.T) {
			engine := newTestEngine(t)
			id := NodeID("test:review-shards")
			node := separatelyStoredNode(id)
			node.ChunkEmbeddings = [][]float32{make([]float32, 32768)}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			meta, err := encodeValue(map[string]any{"chunk_count": 1})
			require.NoError(t, err)
			require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error { return txn.Set(embeddingMetaKey(id), meta) }))
			require.Greater(t, countEmbeddingKeys(t, engine, id), 3)
			if action == "delete" {
				require.NoError(t, engine.DeleteNode(id))
			} else if action == "inline" {
				require.NoError(t, engine.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "new"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
			} else {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				if action == "transaction-inline" {
					require.NoError(t, tx.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
				} else {
					require.NoError(t, tx.DeleteNode(id))
				}
				require.NoError(t, tx.Commit())
			}
			require.Zero(t, countEmbeddingKeys(t, engine, id))
		})
	}
}

func TestEmbeddingCleanup_TransactionRepeatedReplacement(t *testing.T) {
	for _, sequence := range []string{"create-inline", "create-separate", "delete-recreate", "update-separate"} {
		t.Run(sequence, func(t *testing.T) {
			engine := newTestEngine(t)
			id := NodeID("test:repeated")
			if sequence == "update-separate" {
				_, err := engine.CreateNode(separatelyStoredNode(id))
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			if sequence != "update-separate" {
				_, err := tx.CreateNode(separatelyStoredNode(id))
				require.NoError(t, err)
			}
			final := separatelyStoredNode(id)
			final.ChunkEmbeddings = [][]float32{{7, 8}}
			if sequence == "create-separate" || sequence == "update-separate" {
				final.ChunkEmbeddings = [][]float32{make([]float32, 32768)}
				final.ChunkEmbeddings[0][0] = 7
			}
			if sequence == "delete-recreate" {
				require.NoError(t, tx.DeleteNode(id))
				_, err = tx.CreateNode(final)
				require.NoError(t, err)
			} else {
				require.NoError(t, tx.UpdateNode(final))
			}
			require.NoError(t, tx.Commit())
			engine.invalidateCachesAfterRestore()
			got, err := engine.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, final.ChunkEmbeddings, got.ChunkEmbeddings)
			if sequence == "create-inline" || sequence == "delete-recreate" {
				require.Zero(t, countEmbeddingKeys(t, engine, id))
			} else {
				require.Less(t, countEmbeddingKeys(t, engine, id), separateEmbeddingChunks, "old chunks must not survive a smaller separate replacement")
			}
		})
	}
}

func TestEmbeddingCleanup_DeleteRacesSidecarPublication(t *testing.T) {
	engine := newTestEngine(t)
	for iteration := 0; iteration < 20; iteration++ {
		id := NodeID("test:delete-race")
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
		node, err := engine.GetNode(id)
		require.NoError(t, err)
		node.ChunkEmbeddings = separatelyStoredNode(id).ChunkEmbeddings
		node.EmbedMeta = map[string]any{"chunk_count": separateEmbeddingChunks}
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.DeleteNode(id))
		start := make(chan struct{})
		sidecarDone := make(chan error, 1)
		go func() {
			<-start
			sidecarDone <- engine.UpdateNodeEmbeddingSidecar(node)
		}()
		close(start)
		require.NoError(t, tx.Commit())
		err = <-sidecarDone
		require.True(t, err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrEmbeddingSourceChanged), "%v", err)
		require.Zero(t, countEmbeddingKeys(t, engine, id))
	}
}

func TestEmbeddingCleanup_DirectLargeWriteRollsBackBodyAndChunks(t *testing.T) {
	for _, action := range []string{"create", "update"} {
		t.Run(action, func(t *testing.T) {
			engine := newTestEngine(t)
			id := NodeID("test:atomic-embedding")
			if action == "update" {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "old"}})
				require.NoError(t, err)
			}
			node := separatelyStoredNode(id)
			node.ChunkEmbeddings = [][]float32{make([]float32, 2000000)}
			// Incompressible vectors force several Badger batches even though
			// embedding records are compressed before storage.
			state := uint32(1)
			for i := range node.ChunkEmbeddings[0] {
				state ^= state << 13
				state ^= state >> 17
				state ^= state << 5
				node.ChunkEmbeddings[0][i] = float32(int32(state)) / (1 << 31)
			}
			injected := errors.New("embedding batch failed")
			restore := setLargeCommitBatchHook(func(batch int) error {
				require.Zero(t, countEmbeddingKeys(t, engine, id), "no partial chunks may be visible")
				if batch == 2 {
					return injected
				}
				return nil
			})
			defer restore()
			if action == "create" {
				_, err := engine.CreateNode(node)
				require.ErrorIs(t, err, injected)
			} else {
				require.ErrorIs(t, engine.UpdateNode(node), injected)
			}
			restore()
			require.Zero(t, countEmbeddingKeys(t, engine, id))
			got, err := engine.GetNode(id)
			if action == "create" {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, "old", got.Properties["k"])
			}
			requireNoLargeCommitIntent(t, engine)
		})
	}
}

func TestEmbeddingCleanup_SidecarDrainsBufferedChunks(t *testing.T) {
	engine, wal := newWriteBehindWALTestEngine(t, t.TempDir(), time.Hour)
	defer wal.Close()
	defer engine.Close()
	id := NodeID("test:buffered-embedding")
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(separatelyStoredNode(id))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.Zero(t, countEmbeddingKeys(t, engine, id))
	node, err := engine.GetNode(id)
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{3, 4}}
	node.EmbedMeta = map[string]any{"chunk_count": 1}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(node))
	require.Equal(t, 2, countEmbeddingKeys(t, engine, id))
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, 2, countEmbeddingKeys(t, engine, id))

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.DeleteNode(id))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.ErrorIs(t, engine.UpdateNodeEmbeddingSidecar(node), ErrNotFound)
	require.Zero(t, countEmbeddingKeys(t, engine, id))
}

func TestEmbeddingCleanup_TransactionDeleteConcurrentSidecar(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:review-sidecar")
	_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.DeleteNode(id))
	node, err := engine.GetNode(id)
	require.NoError(t, err)
	node.ChunkEmbeddings = separatelyStoredNode(id).ChunkEmbeddings
	node.EmbedMeta = map[string]any{"chunk_count": 16, "has_embedding": true}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(node))
	require.Equal(t, 17, countEmbeddingKeys(t, engine, id))
	require.NoError(t, tx.Commit())
	require.Zero(t, countEmbeddingKeys(t, engine, id), "transaction delete must account for embedding-only writes after its snapshot")
}
