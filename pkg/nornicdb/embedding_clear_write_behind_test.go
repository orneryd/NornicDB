package nornicdb_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// writeBehindClearEmbedder is the in-process embedding provider of this
// regression. It returns the publication fixture's unit vector, so an
// acknowledged CREATE WITH EMBEDDING stores exactly the vector the fixture's
// 16-dimension search index serves, without a model process or network.
type writeBehindClearEmbedder struct{}

func (writeBehindClearEmbedder) Model() string   { return "write-behind-clear-test" }
func (writeBehindClearEmbedder) Dimensions() int { return 16 }

func (writeBehindClearEmbedder) Embed(context.Context, string) ([]float32, error) {
	return clearPublicationVector(), nil
}

func (writeBehindClearEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

// writeBehindClearConfig is the publication fixture's store shape with async
// writes in the requested mode. The one-hour rotation delay keeps an
// acknowledged implicit commit in the write-behind buffer: the buffer's own
// rotation timer is the only owner that would replay it, and the fixture's WAL
// auto-compaction is off, so no snapshot flush intervenes. No embedding worker
// runs (Memory.EmbeddingEnabled is false), so the reset node cannot be
// re-embedded while the test observes it.
func writeBehindClearConfig(asyncWrites bool) *nornicdb.Config {
	cfg := clearPublicationConfig()
	cfg.Database.AsyncWritesEnabled = asyncWrites
	if asyncWrites {
		cfg.Database.AsyncFlushInterval = time.Hour
	}
	return cfg
}

// writeBehindClearBadger resolves the fixture's physical store. BadgerEngine.DB
// documents a read-only handle, and this test only reads it to show where an
// acknowledged commit physically is.
func writeBehindClearBadger(t *testing.T, db *nornicdb.DB) *storage.BadgerEngine {
	t.Helper()
	engine, ok := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	require.True(t, ok, "the default store is backed by Badger")
	return engine
}

// writeBehindClearCommittedRecords counts the physical records whose key ends
// with the qualified node ID: a node Badger holds has its body and derived
// records, while a node held only by the write-behind buffer has none.
func writeBehindClearCommittedRecords(t *testing.T, engine *storage.BadgerEngine, qualifiedID string) int {
	t.Helper()
	handle := engine.DB()
	require.NotNil(t, handle, "the physical store is open")
	records := 0
	suffix := []byte(qualifiedID)
	require.NoError(t, handle.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if bytes.HasSuffix(it.Item().Key(), suffix) {
				records++
			}
		}
		return nil
	}))
	return records
}

// writeBehindClearHits is the number of nodes vector search returns for vector.
func writeBehindClearHits(t *testing.T, svc *search.Service, vector []float32) int {
	t.Helper()
	options := search.DefaultSearchOptions()
	options.Limit = 10
	minimum := 0.9
	options.MinSimilarity = &minimum
	response, err := svc.Search(t.Context(), "", vector, options)
	require.NoError(t, err)
	return len(response.Results)
}

// Report 53.4: an acknowledged implicit CREATE WITH EMBEDDING is visible to
// reads and to the search index while its commit still rests only in the
// write-behind buffer. db.ClearAllEmbeddings is the public reset for those
// stored embeddings, so it must select that node, report it, and leave neither
// a stored nor a served vector. The synchronous control runs the same statement
// and assertions with the buffer off.
//
// The test never flushes before the reset: the pending state is proved from the
// physical store and the reset must handle it. The flush that follows the reset
// proves replay cannot restore the removed embedding.
func TestAcknowledgedWriteBehindCreateWithEmbeddingIsCleared(t *testing.T) {
	for _, tc := range []struct {
		name        string
		asyncWrites bool
	}{
		{name: "write-behind", asyncWrites: true},
		{name: "synchronous control", asyncWrites: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := nornicdb.Open(t.TempDir(), writeBehindClearConfig(tc.asyncWrites))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			require.Nil(t, db.EmbedQueueStats(), "the fixture starts no embedding worker")
			executor := db.GetCypherExecutor()
			require.NotNil(t, executor)
			executor.SetEmbedder(writeBehindClearEmbedder{})
			svc := clearPublicationSearch(t, db)

			_, err = db.Cypher(ctx,
				"CREATE (n:Doc {text: 'async clear'}) WITH EMBEDDING RETURN count(n) AS created", nil)
			require.NoError(t, err, "the implicit CREATE WITH EMBEDDING is acknowledged")

			nodes, err := db.GetStorage().GetNodesByLabel(ctx, "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			id := nodes[0].ID
			stored, err := db.GetStorage().GetNode(ctx, id)
			require.NoError(t, err)
			require.NotEmpty(t, stored.ChunkEmbeddings, "the acknowledged create's embedding is visible to reads")
			require.Equal(t, true, stored.EmbedMeta["has_embedding"])

			// Publish the acknowledged vector through the canonical per-node
			// refresh owner so the served-index precondition is deterministic.
			// The reset below must remove it through its own publication, not a
			// refresh applied here.
			require.NoError(t, svc.RefreshNode(ctx, id))
			require.Equal(t, 1, svc.EmbeddingCount(), "the acknowledged vector is served before the reset")
			require.Equal(t, 1, writeBehindClearHits(t, svc, clearPublicationVector()))

			scoped, ok := db.GetStorage().(*storage.NamespacedEngine)
			require.True(t, ok, "the default store is namespace scoped")
			qualified := string(scoped.PrefixNodeID(id))
			engine := writeBehindClearBadger(t, db)
			if tc.asyncWrites {
				// Pending-write proof: no physical record names the node while
				// the acknowledged embedding still reaches reads and search.
				require.Zero(t, writeBehindClearCommittedRecords(t, engine, qualified),
					"the acknowledged commit rests only in the write-behind buffer")
			} else {
				require.NotZero(t, writeBehindClearCommittedRecords(t, engine, qualified),
					"the synchronous control's commit is already in Badger")
			}

			count, err := db.ClearAllEmbeddings()
			require.NoError(t, err)
			require.Equal(t, 1, count, "the reset must select the acknowledged node exactly once")

			// Immediate absence, without a flush: the reset's per-node
			// publication and the database's catch-up of it remove both the
			// stored embedding and the served vector.
			cleared, err := db.GetStorage().GetNode(ctx, id)
			require.NoError(t, err)
			require.Empty(t, cleared.ChunkEmbeddings, "the reset leaves no stored embedding")
			require.Equal(t, "async clear", cleared.Properties["text"], "the reset keeps the node")
			require.Zero(t, svc.EmbeddingCount(), "the reset's publication serves no vector")
			require.Zero(t, writeBehindClearHits(t, svc, clearPublicationVector()))

			// The buffered write is replayed only now: replay must not restore
			// the embedding the reset removed.
			require.NoError(t, engine.FlushWriteBehind(ctx))
			require.NotZero(t, writeBehindClearCommittedRecords(t, engine, qualified),
				"the acknowledged commit reached Badger")
			replayed, err := db.GetStorage().GetNode(ctx, id)
			require.NoError(t, err)
			require.Empty(t, replayed.ChunkEmbeddings, "replay restored a cleared embedding")
			require.NoError(t, svc.RefreshNode(ctx, id))
			require.Zero(t, svc.EmbeddingCount())
			require.Zero(t, writeBehindClearHits(t, svc, clearPublicationVector()))
		})
	}
}
