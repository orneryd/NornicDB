package cypher

import (
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A new read after an acknowledged autocommit observes its write-behind
// overlay. The old index has enough rows to avoid an empty/short-window fallback.
func TestIndexedOrderIncludesAcknowledgedBufferedCommit(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 1000000,
		}))
		require.NoError(t, err)
		store := storage.NewNamespacedEngine(base, "buffered_order")
		for _, entry := range []struct {
			id   storage.NodeID
			rank int64
		}{{"committed-ten", 10}, {"committed-twenty", 20}} {
			_, err := store.CreateNode(t.Context(), &storage.Node{ID: entry.id, Labels: []string{"BufferedOrder"}, Properties: map[string]interface{}{"rank": entry.rank}})
			require.NoError(t, err)
		}
		writer := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		_, err = writer.Execute(t.Context(), "CREATE INDEX buffered_rank FOR (n:BufferedOrder) ON (n.rank)", nil)
		require.NoError(t, err)
		schema := store.GetSchema()
		committedIDs := func() int {
			count := 0
			exists, err := schema.VisitPropertyIndexGroups(t.Context(), "BufferedOrder", "rank", false, func(ids []storage.NodeID) bool { count += len(ids); return true })
			require.NoError(t, err)
			require.True(t, exists)
			return count
		}
		require.Equal(t, 2, committedIDs(), "index is populated before the accepted write")
		_, err = writer.Execute(t.Context(), "CREATE (:BufferedOrder {rank:1, secondary:'z'})", nil)
		require.NoError(t, err, "public implicit write must be acknowledged before the new read")
		ids, err := schema.PropertyIndexLookup(t.Context(), "BufferedOrder", "rank", int64(1))
		require.NoError(t, err)
		require.Len(t, ids, 1, "canonical equality owner sees the acknowledged overlay")
		idx, exists, err := schema.GetPropertyIndex(t.Context(), "BufferedOrder", "rank")
		require.NoError(t, err)
		require.True(t, exists)
		nativeIDs, err := idx.LookupFull(t.Context(), int64(1))
		require.NoError(t, err)
		require.Empty(t, nativeIDs, "raw committed index has not received the accepted buffered row")
		reader := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		scan, err := reader.Execute(t.Context(), "MATCH (n:BufferedOrder) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, scan.Rows, "new public read sees all acknowledged nodes")
		result, err := reader.Execute(t.Context(), "MATCH (n:BufferedOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC LIMIT 1", nil)
		require.NoError(t, err)
		require.True(t, reader.LastHotPathTrace().OuterIndexTopK, "the public ordered index path must be reached")
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "the acknowledged buffered node belongs before both committed rows")
		_, err = writer.Execute(t.Context(), "CREATE (:BufferedOrder {rank:1, secondary:'a'}), (:BufferedOrder {rank:5, secondary:'m'})", nil)
		require.NoError(t, err)
		for _, test := range []struct {
			query string
			want  [][]interface{}
		}{
			{"MATCH (n:BufferedOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank DESC LIMIT 2", [][]interface{}{{int64(20)}, {int64(10)}}},
			{"MATCH (n:BufferedOrder) WHERE n.rank IS NOT NULL RETURN n.rank,n.secondary ORDER BY n.rank ASC,n.secondary ASC SKIP 1 LIMIT 2", [][]interface{}{{int64(1), "z"}, {int64(5), "m"}}},
			{"MATCH (n:BufferedOrder) WHERE n.rank >= 1 AND n.rank < 10 RETURN n.rank,n.secondary ORDER BY n.rank ASC,n.secondary ASC SKIP 1 LIMIT 2", [][]interface{}{{int64(1), "z"}, {int64(5), "m"}}},
		} {
			got, err := reader.Execute(t.Context(), test.query, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, got.Rows, test.query)
		}
		for _, query := range []string{
			"MATCH (n:BufferedOrder {rank:10}) SET n.rank = 0",
			"MATCH (n:BufferedOrder {rank:20}) DETACH DELETE n",
			"MATCH (n:BufferedOrder {rank:5}) SET n:Moved REMOVE n:BufferedOrder",
		} {
			_, err := writer.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
		}
		result, err = reader.Execute(t.Context(), "MATCH (n:BufferedOrder) WHERE n.rank IS NOT NULL RETURN n.rank ORDER BY n.rank ASC LIMIT 10", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}, {int64(1)}, {int64(1)}}, result.Rows, "pending rewrite/delete/label-change replace old index memberships")
		nativeIDs, err = idx.LookupFull(t.Context(), int64(1))
		require.NoError(t, err)
		require.Empty(t, nativeIDs, "public reads did not force a flush to obtain correctness")

	})
}
