package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIndexedNotNullRetainsAcceptedByteProperties(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
		require.NoError(t, err)
		store := storage.NewNamespacedEngine(base, "nonnull_bytes")
		id, err := store.CreateNode(t.Context(), &storage.Node{ID: "native", Labels: []string{"HasValue"}, Properties: map[string]interface{}{"name": "native-bytes", "value": []byte{1, 2}}})
		require.NoError(t, err)
		native, err := store.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2}, native.Properties["value"])
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
		_, err = exec.Execute(t.Context(), "CREATE (:HasValue {name:'param-bytes', value:$value}), (:HasValue {name:'scalar', value:7})", map[string]interface{}{"value": []byte{3, 4}})
		require.NoError(t, err)
		parameter, err := exec.Execute(t.Context(), "MATCH (n:HasValue {name:'param-bytes'}) RETURN n.value", nil)
		require.NoError(t, err)
		require.Len(t, parameter.Rows, 1)
		require.NotNil(t, parameter.Rows[0][0])
		t.Logf("accepted native property Go type=%T, public byte-parameter property Go type=%T", native.Properties["value"], parameter.Rows[0][0])
		query := "MATCH (n:HasValue) WHERE n.value IS NOT NULL RETURN n.name ORDER BY n.name"
		before, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"native-bytes"}, {"param-bytes"}, {"scalar"}}, before.Rows)
		orderQuery := "MATCH (n:HasValue) WHERE n.value IS NOT NULL RETURN n.name ORDER BY n.value ASC LIMIT 1"
		beforeOrder, err := exec.Execute(t.Context(), orderQuery, nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "CREATE INDEX has_bytes FOR (n:HasValue) ON (n.value)", nil)
		require.NoError(t, err)
		// Reuse the existing public Engine no-scan control: candidate mode
		// must remain indexed even when the value-order shortcut declines.
		reader := NewStorageExecutor(&noScanMergeLookupEngine{Engine: store}, QueryCachePolicy{MaxEntries: 0, TTL: 0})

		idx, exists, err := store.GetSchema().GetPropertyIndex(t.Context(), "HasValue", "value")
		require.NoError(t, err)
		require.True(t, exists)
		scalar, err := idx.LookupFull(t.Context(), int64(7))
		require.NoError(t, err)
		require.Len(t, scalar, 1, "nonempty scalar bucket prevents fallback masking")
		unkeyed, err := idx.LookupFull(t.Context(), []byte{1, 2})
		require.NoError(t, err)
		require.Empty(t, unkeyed, "accepted native byte value has no equality key")
		after, err := reader.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Equal(t, before.Rows, after.Rows, "index candidacy must retain accepted unkeyable nonnull properties")
		ordered, err := reader.Execute(t.Context(), orderQuery, nil)
		require.NoError(t, err)
		require.Equal(t, beforeOrder.Rows, ordered.Rows, "actual-value language order must survive index capability decline")
		require.False(t, reader.LastHotPathTrace().OuterIndexTopK, "non-value-orderable keys cannot drive index top-k")

	})
}
