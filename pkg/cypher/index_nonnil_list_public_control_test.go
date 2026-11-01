package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A property index may prune IS NOT NULL only if it covers every nonnull
// value accepted by the public write/equality contracts, including lists.
func TestIndexedNotNullRetainsValidListProperties(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
		require.NoError(t, err)
		store := storage.NewNamespacedEngine(base, "nonnull_values")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		_, err = exec.Execute(t.Context(), "CREATE (:HasValue {name:'scalar', value:7}), (:HasValue {name:'list', value:[1,2]}), (:HasValue {name:'missing'})", nil)
		require.NoError(t, err)
		query := "MATCH (n:HasValue) WHERE n.value IS NOT NULL RETURN n.name ORDER BY n.name"
		before, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"list"}, {"scalar"}}, before.Rows)
		orderQuery := "MATCH (n:HasValue) WHERE n.value IS NOT NULL RETURN n.name ORDER BY n.value ASC LIMIT 1"
		beforeOrder, err := exec.Execute(t.Context(), orderQuery, nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "CREATE INDEX has_value FOR (n:HasValue) ON (n.value)", nil)
		require.NoError(t, err)
		// Reuse the existing public Engine no-scan control: candidate mode
		// must remain indexed even when the value-order shortcut declines.
		reader := NewStorageExecutor(&noScanMergeLookupEngine{Engine: store}, QueryCachePolicy{MaxEntries: 0, TTL: 0})

		ids, err := store.GetSchema().PropertyIndexLookup(t.Context(), "HasValue", "value", []interface{}{int64(1), int64(2)})
		require.NoError(t, err)
		require.Len(t, ids, 1, "the valid list is present in the populated canonical equality index")
		equality, err := reader.Execute(t.Context(), "MATCH (n:HasValue {value:[1,2]}) RETURN n.name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"list"}}, equality.Rows)
		after, err := reader.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, before.Rows, after.Rows, "a nonempty scalar index bucket cannot hide a valid nonnull list row")
		ordered, err := reader.Execute(t.Context(), orderQuery, nil)
		require.NoError(t, err)
		require.Equal(t, beforeOrder.Rows, ordered.Rows, "actual-value language order must survive index capability decline")
		require.False(t, reader.LastHotPathTrace().OuterIndexTopK, "non-value-orderable keys cannot drive index top-k")

	})
}
