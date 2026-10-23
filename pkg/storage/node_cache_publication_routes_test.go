package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The stale callback is replayed explicitly after each actual write route and
// a later delete/recreate. This checks both absence and latest vector retention.
func TestReview1548PublicationRoutes(t *testing.T) {
	for _, route := range []string{"direct-upsert", "transaction", "buffered-wal"} {
		t.Run(route, func(t *testing.T) {
			e := NewMemoryEngine()
			t.Cleanup(func() { _ = e.Close() })
			b := e.BadgerEngine
			n := &Node{ID: "review:route", ChunkEmbeddings: [][]float32{{1, 0}}}
			before := b.nodeCacheGen.current()
			switch route {
			case "direct-upsert":
				require.NoError(t, e.UpdateNode(n))
			case "transaction":
				tx, err := e.BeginTransaction()
				require.NoError(t, err)
				_, err = tx.CreateNode(n)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			case "buffered-wal":
				producer, wal := newWriteBehindWALTestEngine(t, t.TempDir(), time.Hour)
				tx, err := producer.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("review"))
				_, err = tx.CreateNode(n)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
				require.NoError(t, err)
				var entry WALEntry
				for _, candidate := range entries {
					if candidate.Operation == OpBufferedCommit {
						entry = candidate
						break
					}
				}
				require.NotEmpty(t, entry.Data)
				crashClose(t, producer, wal)
				require.NoError(t, ReplayWALEntry(b, entry))
			}
			got, err := e.GetNode(n.ID)
			require.NoError(t, err)
			require.Equal(t, n.ChunkEmbeddings, got.ChunkEmbeddings)
			require.NoError(t, e.DeleteNode(n.ID))
			b.cacheStoreWrittenNode(n, before)
			_, err = e.GetNode(n.ID)
			require.ErrorIs(t, err, ErrNotFound)
			latest := &Node{ID: n.ID, ChunkEmbeddings: [][]float32{{0, 1}}}
			_, err = e.CreateNode(latest)
			require.NoError(t, err)
			b.cacheStoreWrittenNode(n, before)
			got, err = e.GetNode(n.ID)
			require.NoError(t, err)
			require.Equal(t, latest.ChunkEmbeddings, got.ChunkEmbeddings)
			require.NoError(t, e.DeleteNode(n.ID))
			_, err = e.GetNode(n.ID)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
