package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-307: the query-result caches retain and deliver one shared result image.
// These tests hold the result a cache stored, and the result a cache delivered,
// to the contract the storage value owner already gives every other retention
// point: the cache owns its image, and each caller owns the image it receives.

// nor307Result is a representative cached read result: two rows, four columns,
// a nested list, a returned node, a null cell and a temporal value.
func nor307Result() *ExecuteResult {
	return &ExecuteResult{
		Columns: []string{"name", "items", "node", "at"},
		Rows: [][]any{
			{
				"alpha",
				[]any{int64(1), int64(2)},
				&storage.Node{
					ID:         "nor307:node",
					Labels:     []string{"Nor307"},
					Properties: map[string]any{"name": "alpha", "tags": []any{"a", "b"}},
				},
				time.Unix(11, 0).UTC(),
			},
			{"beta", []any{int64(3)}, nil, time.Unix(12, 0).UTC()},
		},
		Stats:          &QueryStats{PropertiesSet: 7},
		Metadata:       map[string]any{"plan": "nor307", "nested": map[string]any{"k": int64(1)}},
		Administration: &AdministrationOutcome{CreatedDatabase: "nor307db"},
	}
}

// requireNor307Result asserts the complete cached image: column and row order,
// concrete value types and every value.
func requireNor307Result(t *testing.T, result *ExecuteResult) {
	t.Helper()
	require.NotNil(t, result)

	require.Len(t, result.Rows, 2)
	require.Equal(t, "alpha", result.Rows[0][0])
	items, ok := result.Rows[0][1].([]any)
	require.True(t, ok, "a list cell keeps its list type")
	require.Equal(t, []any{int64(1), int64(2)}, items)
	require.IsType(t, int64(0), items[0])

	node, ok := result.Rows[0][2].(*storage.Node)
	require.True(t, ok, "a returned node keeps its type")
	require.Equal(t, "alpha", node.Properties["name"])
	require.Equal(t, []any{"a", "b"}, node.Properties["tags"])
	require.Equal(t, "nor307:node", string(node.ID))
	require.Equal(t, []string{"Nor307"}, node.Labels)
	require.IsType(t, time.Time{}, result.Rows[0][3])
	require.Equal(t, time.Unix(11, 0).UTC(), result.Rows[0][3])

	require.Equal(t, "beta", result.Rows[1][0])
	require.Equal(t, []any{int64(3)}, result.Rows[1][1])
	require.Nil(t, result.Rows[1][2])
	require.Equal(t, time.Unix(12, 0).UTC(), result.Rows[1][3])

	require.Equal(t, []string{"name", "items", "node", "at"}, result.Columns)

	require.NotNil(t, result.Stats)
	require.Equal(t, 7, result.Stats.PropertiesSet)
	require.Equal(t, "nor307", result.Metadata["plan"])
	require.Equal(t, map[string]any{"k": int64(1)}, result.Metadata["nested"])
	require.NotNil(t, result.Administration)
	require.Equal(t, "nor307db", result.Administration.CreatedDatabase)
}

// TestCachePutDoesNotKeepTheCallersResult covers the input a caller still holds
// after Put: changing any part of it may not change the cached result.
func TestCachePutDoesNotKeepTheCallersResult(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cache func() ownershipTestResultCache
	}{
		{name: "query-cache", cache: func() ownershipTestResultCache { return NewQueryCache(8) }},
		{name: "smart-query-cache", cache: func() ownershipTestResultCache { return NewSmartQueryCache(8) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			source := nor307Result()
			cache.Put("MATCH (n:Nor307) RETURN n.name, n.items, n, n.at", nil, source, time.Hour)

			// The caller keeps its own result and changes all of it.
			source.Columns[0] = "renamed"
			source.Rows[0][0] = "changed by caller"
			source.Rows[0][1].([]any)[0] = "changed by caller"
			source.Rows[0][2].(*storage.Node).Properties["name"] = "changed by caller"
			source.Rows[0][2].(*storage.Node).Properties["tags"].([]any)[0] = "changed by caller"
			source.Metadata["plan"] = "changed by caller"
			source.Metadata["nested"].(map[string]any)["k"] = "changed by caller"
			source.Stats.PropertiesSet = 99
			source.Rows = source.Rows[:1]

			got, found := cache.Get("MATCH (n:Nor307) RETURN n.name, n.items, n, n.at", nil)
			require.True(t, found)
			requireNor307Result(t, got)
		})
	}
}

// TestCacheHitDeliversTheCallersOwnResult covers the output a caller receives:
// changing it may change neither later hits nor the cached image.
func TestCacheHitDeliversTheCallersOwnResult(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cache func() ownershipTestResultCache
	}{
		{name: "query-cache", cache: func() ownershipTestResultCache { return NewQueryCache(8) }},
		{name: "smart-query-cache", cache: func() ownershipTestResultCache { return NewSmartQueryCache(8) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			cache.Put("MATCH (n:Nor307) RETURN n.name, n.items, n, n.at", nil, nor307Result(), time.Hour)

			delivered, found := cache.Get("MATCH (n:Nor307) RETURN n.name, n.items, n, n.at", nil)
			require.True(t, found)
			requireNor307Result(t, delivered)

			delivered.Columns[0] = "renamed"
			delivered.Rows[0][0] = "changed by caller"
			delivered.Rows[0][1].([]any)[0] = "changed by caller"
			delivered.Rows[0][2].(*storage.Node).Properties["name"] = "changed by caller"
			delivered.Metadata["plan"] = "changed by caller"
			delivered.Stats.PropertiesSet = 99
			delivered.Rows = nil

			got, found := cache.Get("MATCH (n:Nor307) RETURN n.name, n.items, n, n.at", nil)
			require.True(t, found)
			requireNor307Result(t, got)
		})
	}
}

// TestCacheDetachmentPreservesEmptyNilAndDeletedImages covers the images a
// cached read can legitimately have: no result, no rows, no columns, and null
// cells. None of them becomes a non-nil or missing image through the copy.
func TestCacheDetachmentPreservesEmptyNilAndDeletedImages(t *testing.T) {
	cache := NewQueryCache(8)

	cache.Put("RETURN nothing", nil, nil, time.Hour)
	got, found := cache.Get("RETURN nothing", nil)
	require.True(t, found)
	require.Nil(t, got)

	cache.Put("RETURN no rows", nil, &ExecuteResult{Columns: []string{"n"}}, time.Hour)
	got, found = cache.Get("RETURN no rows", nil)
	require.True(t, found)
	require.Equal(t, []string{"n"}, got.Columns)
	require.Nil(t, got.Rows)

	cache.Put("RETURN empty rows", nil, &ExecuteResult{Columns: []string{}, Rows: [][]any{}}, time.Hour)
	got, found = cache.Get("RETURN empty rows", nil)
	require.True(t, found)
	require.NotNil(t, got.Columns)
	require.NotNil(t, got.Rows)
	require.Empty(t, got.Columns)
	require.Empty(t, got.Rows)

	cache.Put("RETURN null cell", nil, &ExecuteResult{Columns: []string{"n"}, Rows: [][]any{{nil}}}, time.Hour)
	got, found = cache.Get("RETURN null cell", nil)
	require.True(t, found)
	require.Len(t, got.Rows, 1)
	require.Nil(t, got.Rows[0][0])
}

// TestSmartQueryCacheDetachmentKeepsLabelInvalidation covers the cache state
// that is not a row: a cached result is still invalidated by the labels it was
// stored under, and by an unlabelled query's every-write dependency.
func TestSmartQueryCacheDetachmentKeepsLabelInvalidation(t *testing.T) {
	cache := NewSmartQueryCache(8)
	cache.PutWithLabels("MATCH (n:Alpha) RETURN n.name", nil,
		&ExecuteResult{Columns: []string{"name"}, Rows: [][]any{{"alpha"}}}, time.Hour, []string{"Alpha"})
	cache.PutWithLabels("MATCH (n) RETURN n.name", nil,
		&ExecuteResult{Columns: []string{"name"}, Rows: [][]any{{"alpha"}, {"beta"}}}, time.Hour, nil)

	cache.InvalidateLabels([]string{"Alpha"})

	_, found := cache.Get("MATCH (n:Alpha) RETURN n.name", nil)
	require.False(t, found, "a write to Alpha invalidates the result reading Alpha")
	_, found = cache.Get("MATCH (n) RETURN n.name", nil)
	require.False(t, found, "a write invalidates an unlabelled query's result")
}

// nor307PublicExecutor builds the supported embedded API: one executor with a
// result cache, over a store that holds one Nor307Ownership node.
func nor307PublicExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor307")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	_, err := exec.Execute(t.Context(), "CREATE (n:Nor307Ownership {name: 'alpha', tags: ['a', 'b']})", nil)
	require.NoError(t, err)
	return exec
}

// TestPublicExecuteCachedReadIsCallerOwned reproduces NOR-307 through the
// supported embedded Execute API: an embedded caller changes a returned cached
// row or nested entity property, then repeats the query.
func TestPublicExecuteCachedReadIsCallerOwned(t *testing.T) {
	const query = "MATCH (n:Nor307Ownership) RETURN n.name AS name, n.tags AS tags, n"

	// requireOriginal asserts the returned rows before the column names so the
	// reported symptom (a changed row or entity property) is the first effect
	// a caller sees.
	requireOriginal := func(t *testing.T, result *ExecuteResult) {
		t.Helper()
		require.Len(t, result.Rows, 1)
		require.Equal(t, "alpha", result.Rows[0][0])
		tags, ok := result.Rows[0][1].([]any)
		require.True(t, ok, "a list cell keeps its list type")
		require.Equal(t, []any{"a", "b"}, tags)
		node, ok := result.Rows[0][2].(*storage.Node)
		require.True(t, ok, "a returned node keeps its type")
		require.Equal(t, "alpha", node.Properties["name"])
		require.Equal(t, []string{"a", "b"}, node.Properties["tags"])
		require.Equal(t, []string{"name", "tags", "n"}, result.Columns)
	}

	repeatedRead := func(t *testing.T, exec *StorageExecutor, change func(*ExecuteResult)) *ExecuteResult {
		t.Helper()
		first, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.Len(t, first.Rows, 1)
		change(first)

		hitsBefore, _, _, _, _ := exec.cache.Stats()
		repeated, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		hitsAfter, _, _, _, _ := exec.cache.Stats()
		require.Greater(t, hitsAfter, hitsBefore, "the repeated read must be served from the result cache")
		requireOriginal(t, repeated)
		return repeated
	}

	t.Run("caller changes the result the miss returned", func(t *testing.T) {
		exec := nor307PublicExecutor(t)
		repeatedRead(t, exec, func(missed *ExecuteResult) {
			missed.Rows[0][0] = "changed by caller"
			missed.Rows[0][1].([]any)[0] = "changed by caller"
			missed.Rows[0][2].(*storage.Node).Properties["name"] = "changed by caller"
		})
	})

	t.Run("caller changes the result the hit delivered", func(t *testing.T) {
		exec := nor307PublicExecutor(t)
		repeatedRead(t, exec, func(missed *ExecuteResult) {
			require.Len(t, missed.Rows, 1)
		})

		hitDelivered, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		hitDelivered.Rows[0][1].([]any)[0] = "changed by caller"
		hitDelivered.Rows[0][2].(*storage.Node).Properties["name"] = "changed by caller"

		hitsBefore, _, _, _, _ := exec.cache.Stats()
		repeated, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		hitsAfter, _, _, _, _ := exec.cache.Stats()
		require.Greater(t, hitsAfter, hitsBefore, "the repeated read must be served from the result cache")
		requireOriginal(t, repeated)
	})

	t.Run("a canceled statement is not served from the cache", func(t *testing.T) {
		exec := nor307PublicExecutor(t)
		_, err := exec.Execute(t.Context(), query, nil)
		require.NoError(t, err)

		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		result, err := exec.Execute(canceled, query, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
	})
}
