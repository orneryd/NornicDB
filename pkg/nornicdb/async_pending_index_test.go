package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// openAsyncPendingIndexDB opens a DB with async writes that never flush on
// their own, so every write after the last explicit flush stays pending.
func openAsyncPendingIndexDB(t *testing.T) (*DB, *storage.AsyncEngine) {
	t.Helper()
	db, err := Open(t.TempDir(), &Config{Database: nornicConfig.DatabaseConfig{AsyncWritesEnabled: true, AsyncFlushInterval: time.Hour}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	async, ok := db.GetBaseStorageForManager().(*storage.AsyncEngine)
	require.True(t, ok, "base storage is %T", db.GetBaseStorageForManager())
	return db, async
}

func pendingIndexRows(t *testing.T, db *DB, query string, params map[string]interface{}) [][]interface{} {
	t.Helper()
	result, err := db.ExecuteCypher(context.Background(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

// TestAsyncPendingWritesAreVisibleToIndexLookups: with async writes, a write
// is visible to the next statement's index-backed reads before the flush, as
// it is to a label scan (#719).
func TestAsyncPendingWritesAreVisibleToIndexLookups(t *testing.T) {
	db, async := openAsyncPendingIndexDB(t)
	pendingIndexRows(t, db, "CREATE INDEX ux_id FOR (n:UX) ON (n.id)", nil)

	lookups := func(t *testing.T, value string) map[string][][]interface{} {
		params := map[string]interface{}{"v": value}
		return map[string][][]interface{}{
			"map literal": pendingIndexRows(t, db, fmt.Sprintf("MATCH (n:UX {id: '%s'}) RETURN n.n ORDER BY n.n", value), nil),
			"map param":   pendingIndexRows(t, db, "MATCH (n:UX {id: $v}) RETURN n.n ORDER BY n.n", params),
			"unwind":      pendingIndexRows(t, db, "UNWIND [$v] AS v MATCH (n:UX {id: v}) RETURN n.n ORDER BY n.n", params),
			"where":       pendingIndexRows(t, db, "MATCH (n:UX) WHERE n.id = $v RETURN n.n ORDER BY n.n", params),
			"in":          pendingIndexRows(t, db, "MATCH (n:UX) WHERE n.id IN [$v] RETURN n.n ORDER BY n.n", params),
		}
	}
	expect := func(t *testing.T, value string, want [][]interface{}) {
		t.Helper()
		for shape, rows := range lookups(t, value) {
			require.Equal(t, want, rows, "%s lookup of %q", shape, value)
		}
	}

	t.Run("delete then re-create with the same value", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE (:UX {id: 'a', n: 1})", nil)
		require.NoError(t, async.Flush())
		pendingIndexRows(t, db, "MATCH (n:UX {id: 'a'}) DELETE n", nil)
		pendingIndexRows(t, db, "CREATE (:UX {id: 'a', n: 2})", nil)
		expect(t, "a", [][]interface{}{{int64(2)}})
		require.NoError(t, async.Flush())
		expect(t, "a", [][]interface{}{{int64(2)}})
	})

	t.Run("a pending node beside a flushed node with the same value", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE (:UX {id: 'b', n: 1})", nil)
		require.NoError(t, async.Flush())
		pendingIndexRows(t, db, "CREATE (:UX {id: 'b', n: 2})", nil)
		expect(t, "b", [][]interface{}{{int64(1)}, {int64(2)}})
	})

	t.Run("an update changes the value before the flush", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE (:UX {id: 'c', n: 1})", nil)
		require.NoError(t, async.Flush())
		pendingIndexRows(t, db, "MATCH (n:UX {id: 'c'}) SET n.id = 'd'", nil)
		expect(t, "c", [][]interface{}{})
		expect(t, "d", [][]interface{}{{int64(1)}})
		require.NoError(t, async.Flush())
		expect(t, "c", [][]interface{}{})
		expect(t, "d", [][]interface{}{{int64(1)}})
	})

	t.Run("detach delete of a pending node", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE (:UX {id: 'e', n: 1})-[:R]->(:UX {id: 'f', n: 1})", nil)
		pendingIndexRows(t, db, "MATCH (n:UX {id: 'e'}) DETACH DELETE n", nil)
		expect(t, "e", [][]interface{}{})
		expect(t, "f", [][]interface{}{{int64(1)}})
	})

	t.Run("merge by key finds the pending node", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE (:UX {id: 'g', n: 1})", nil)
		require.NoError(t, async.Flush())
		pendingIndexRows(t, db, "MATCH (n:UX {id: 'g'}) DELETE n", nil)
		pendingIndexRows(t, db, "CREATE (:UX {id: 'g', n: 2})", nil)
		rows := pendingIndexRows(t, db, "MERGE (n:UX {id: 'g'}) ON MATCH SET n.m = 'matched' RETURN n.n, n.m", nil)
		require.Equal(t, [][]interface{}{{int64(2), "matched"}}, rows)
		require.Equal(t, [][]interface{}{{int64(1)}}, pendingIndexRows(t, db, "MATCH (n:UX) WHERE n.id = 'g' RETURN count(n)", nil))
	})

	t.Run("ordered and not-null index scans include pending nodes", func(t *testing.T) {
		pendingIndexRows(t, db, "CREATE INDEX ox_k FOR (n:OX) ON (n.k)", nil)
		pendingIndexRows(t, db, "CREATE (:OX {k: 2}), (:OX {k: 4})", nil)
		require.NoError(t, async.Flush())
		pendingIndexRows(t, db, "CREATE (:OX {k: 1}), (:OX {k: 3})", nil)
		pendingIndexRows(t, db, "MATCH (n:OX {k: 4}) SET n.k = 0", nil)
		require.Equal(t, [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}},
			pendingIndexRows(t, db, "MATCH (n:OX) WHERE n.k IS NOT NULL RETURN n.k ORDER BY n.k LIMIT 3", nil))
		require.Equal(t, [][]interface{}{{int64(3)}, {int64(2)}},
			pendingIndexRows(t, db, "MATCH (n:OX) WHERE n.k IS NOT NULL RETURN n.k ORDER BY n.k DESC LIMIT 2", nil))
		require.Equal(t, [][]interface{}{{int64(4)}},
			pendingIndexRows(t, db, "MATCH (n:OX) WHERE n.k IS NOT NULL RETURN count(n)", nil))
	})

	t.Run("an explicit transaction and auto-commit writes", func(t *testing.T) {
		session := cypher.NewStorageExecutor(db.GetStorage())
		ctx := context.Background()
		pendingIndexRows(t, db, "CREATE (:UX {id: 'h', n: 1})", nil)
		_, err := session.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = session.Execute(ctx, "CREATE (:UX {id: 'h', n: 2})", nil)
		require.NoError(t, err)
		_, err = session.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		expect(t, "h", [][]interface{}{{int64(1)}, {int64(2)}})
		require.NoError(t, async.Flush())
		expect(t, "h", [][]interface{}{{int64(1)}, {int64(2)}})
	})
}

// TestAsyncUniqueConstraintSeesPendingWrites: a UNIQUE value deleted and
// re-created before the flush is not a conflict, and two pending nodes with
// the same value are (#719).
func TestAsyncUniqueConstraintSeesPendingWrites(t *testing.T) {
	db, async := openAsyncPendingIndexDB(t)
	ctx := context.Background()
	_, err := db.ExecuteCypher(ctx, "CREATE CONSTRAINT uq_k FOR (n:UQ) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "CREATE (:UQ {k: 1})", nil)
	require.NoError(t, err)
	require.NoError(t, async.Flush())

	_, err = db.ExecuteCypher(ctx, "MATCH (n:UQ {k: 1}) DELETE n", nil)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "CREATE (:UQ {k: 1})", nil)
	require.NoError(t, err, "a value deleted before the flush is free")

	_, err = db.ExecuteCypher(ctx, "CREATE (:UQ {k: 1})", nil)
	require.Error(t, err, "the pending node holds the value")
	_, err = db.ExecuteCypher(ctx, "CREATE (:UQ {k: 2})", nil)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "CREATE (:UQ {k: 2})", nil)
	require.Error(t, err, "two pending nodes can't share a value")

	require.NoError(t, async.Flush())
	require.Equal(t, [][]interface{}{{int64(2)}}, pendingIndexRows(t, db, "MATCH (n:UQ) RETURN count(n)", nil))
}

// TestAsyncPropertyIndexLookupDuringFlush: lookups racing flushes see each
// committed node exactly once (#719). Run with -race.
func TestAsyncPropertyIndexLookupDuringFlush(t *testing.T) {
	db, async := openAsyncPendingIndexDB(t)
	ctx := context.Background()
	_, err := db.ExecuteCypher(ctx, "CREATE INDEX rx_id FOR (n:RX) ON (n.id)", nil)
	require.NoError(t, err)

	stop := make(chan struct{})
	var flushes sync.WaitGroup
	flushes.Add(1)
	go func() {
		defer flushes.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = async.Flush()
			}
		}
	}()
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("r%d", i)
		_, err := db.ExecuteCypher(ctx, "CREATE (:RX {id: $id})", map[string]interface{}{"id": id})
		require.NoError(t, err)
		rows := pendingIndexRows(t, db, "MATCH (n:RX {id: $id}) RETURN count(n)", map[string]interface{}{"id": id})
		require.Equal(t, [][]interface{}{{int64(1)}}, rows, id)
		_, err = db.ExecuteCypher(ctx, "MATCH (n:RX {id: $id}) SET n.id = $id + 'x'", map[string]interface{}{"id": id})
		require.NoError(t, err)
		rows = pendingIndexRows(t, db, "MATCH (n:RX {id: $id}) RETURN count(n)", map[string]interface{}{"id": id})
		require.Equal(t, [][]interface{}{{int64(0)}}, rows, id)
	}
	close(stop)
	flushes.Wait()
}

// TestCreateIndexIfNotExistsKeepsOneEntryPerNode: running CREATE INDEX for an
// existing index doesn't fill it again, and filling a new index skips pending
// nodes, which the flush indexes (#719).
func TestCreateIndexIfNotExistsKeepsOneEntryPerNode(t *testing.T) {
	db, async := openAsyncPendingIndexDB(t)
	ctx := context.Background()
	_, err := db.ExecuteCypher(ctx, "CREATE (:IX {id: 'a'})", nil)
	require.NoError(t, err)
	require.NoError(t, async.Flush())
	_, err = db.ExecuteCypher(ctx, "CREATE (:IX {id: 'b'})", nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = db.ExecuteCypher(ctx, "CREATE INDEX ix_id IF NOT EXISTS FOR (n:IX) ON (n.id)", nil)
		require.NoError(t, err)
	}
	require.NoError(t, async.Flush())
	schema := db.GetStorage().GetSchema()
	require.Len(t, schema.PropertyIndexLookup("IX", "id", "a"), 1)
	require.Len(t, schema.PropertyIndexLookup("IX", "id", "b"), 1)
}
