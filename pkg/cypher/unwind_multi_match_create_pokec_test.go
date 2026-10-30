package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestUnwindMultiMatchCreatePokecShape covers the comma-separated MATCH
// wiring shape (`UNWIND $pairs AS p MATCH (n:L {id: p[0]}), (m:L {id: p[1]})
// CREATE (n)-[:R]->(m)`) — the Pokec seeder form where the row item is a
// list and one MATCH clause carries two indexed-equality patterns. It must
// take the batched fast path (UnwindMultiMatchCreateBatch) and produce
// exactly one edge per row.
func TestUnwindMultiMatchCreatePokecShape(t *testing.T) {
	badger, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(badger, "test"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX l_id IF NOT EXISTS FOR (n:L) ON (n.id)", nil)
	require.NoError(t, err)
	// Replicate Pokec node count shaping: 4 nodes, pairs reference them.
	_, err = exec.Execute(ctx, "CREATE (:L {id: 1}), (:L {id: 2}), (:L {id: 3}), (:L {id: 4})", nil)
	require.NoError(t, err)

	exec.resetHotPathTrace()
	pairs := [][]int64{{1, 2}, {2, 3}, {3, 4}, {4, 1}, {1, 3}, {2, 4}}
	_, err = exec.Execute(ctx,
		"UNWIND $pairs AS p MATCH (n:L {id: p[0]}), (m:L {id: p[1]}) CREATE (n)-[:R]->(m)",
		map[string]any{"pairs": pairs})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch,
		"Pokec shape must take the batched fast path")

	// Exact edge count and endpoint closure.
	res, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN COUNT(r)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(pairs)), mustInt64Count(t, res))

	// Each created edge matches one row's endpoints.
	for _, pair := range pairs {
		res, err := exec.Execute(ctx, `MATCH (n:L {id: `+itoa64(pair[0])+`})-[r:R]->(m:L {id: `+itoa64(pair[1])+`}) RETURN COUNT(r)`, nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), mustInt64Count(t, res))
	}
}

// TestUnwindMultiMatchCreateCommaMatchMissingRow covers a row whose value
// matches no node: the fast path must skip the row, not fabricate edges.
func TestUnwindMultiMatchCreateCommaMatchMissingRow(t *testing.T) {
	badger, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(badger, "test"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX l_id IF NOT EXISTS FOR (n:L) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:L {id: 1}), (:L {id: 2})", nil)
	require.NoError(t, err)

	pairs := [][]int64{{1, 2}, {2, 99}, {99, 1}, {2, 1}}
	_, err = exec.Execute(ctx,
		"UNWIND $pairs AS p MATCH (n:L {id: p[0]}), (m:L {id: p[1]}) CREATE (n)-[:R]->(m)",
		map[string]any{"pairs": pairs})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN COUNT(r)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), mustInt64Count(t, res))
}

// TestUnwindMultiMatchCreateRowMapPrefetch verifies the map-row shape still
// produces exact edges after the pipeline consumes the batch prefetch index.
func TestUnwindMultiMatchCreateRowMapPrefetch(t *testing.T) {
	badger, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(badger, "test"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX o_order_id IF NOT EXISTS FOR (n:O) ON (n.orderID)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX p_product_id IF NOT EXISTS FOR (n:P) ON (n.productID)", nil)
	require.NoError(t, err)

	rows := make([]map[string]any, 0, 10)
	for i := int64(0); i < 5; i++ {
		rows = append(rows, map[string]any{"orderID": i, "productID": i})
	}
	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:O {orderID: r.orderID})", map[string]any{"rows": rows})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:P {productID: r.productID})", map[string]any{"rows": rows})
	require.NoError(t, err)

	// Repeated values exercise the same prefetch entries across rows.
	wiring := make([]map[string]any, 0, 12)
	for i := int64(0); i < 12; i++ {
		wiring = append(wiring, map[string]any{"orderID": i % 5, "productID": (i + 1) % 5})
	}
	_, err = exec.Execute(ctx,
		"UNWIND $rows AS row MATCH (o:O {orderID: row.orderID}) MATCH (p:P {productID: row.productID}) CREATE (o)-[:ORDERS]->(p)",
		map[string]any{"rows": wiring})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH ()-[r:ORDERS]->() RETURN COUNT(r)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(wiring)), mustInt64Count(t, res))
}

func mustInt64Count(t *testing.T, res *ExecuteResult) int64 {
	t.Helper()
	require.NotNil(t, res)
	if len(res.Rows) != 1 {
		require.FailNowf(t, "expected one row", "got %d rows", len(res.Rows))
	}
	count := toInt64(res.Rows[0][0])
	return count
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
