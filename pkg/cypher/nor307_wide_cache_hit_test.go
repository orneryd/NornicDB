package cypher

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-307 / B31 reported shapes. The cache-hit controls below hold the two
// results the report measured - a wide read of many 18-key map rows and a
// 25-row read whose every row returns a node carrying chunk vectors - to the
// detached-result contract the reported correction requires: the cache owns its
// image, every delivery is the caller's own image, and the caller's original
// result stays theirs after publication.
//
// A copy of these shapes is what a cache hit pays for, so a change that makes
// the copy cheaper must keep every assertion here: container identity shared
// inside the result is preserved inside the delivered image, and no delivered
// value is shared with the cached image or with the caller's source result.

// nor307WideCacheHitQuery is the reported query key for the wide map read.
const nor307WideCacheHitQuery = "MATCH (n:Nor307) RETURN n {.*} AS row"

// nor307WideCacheHitRows is the correctness scale of the wide shape. The
// benchmark owns the report's 21,748 rows; the detachment and identity controls
// assert the same graph on a bounded row count.
const nor307WideCacheHitRows = 256

// cacheHitOwnershipCaches are the result-cache owners that deliver a result to
// an embedded consumer: the plain cache and the smart cache the executor uses.
func cacheHitOwnershipCaches() map[string]ownershipTestResultCache {
	return map[string]ownershipTestResultCache{
		"query-cache":       NewQueryCache(8),
		"smart-query-cache": NewSmartQueryCache(8),
	}
}

// requireWideMapRow asserts one delivered row of the wide shape: the row holds
// one map with the report's key count and keeps each property's concrete type.
func requireWideMapRow(t *testing.T, row []any, index int) {
	t.Helper()
	require.Len(t, row, 1)
	value, ok := row[0].(map[string]any)
	require.True(t, ok, "a returned map row keeps its map type")
	require.Len(t, value, nor307WideMapKeys)

	ordinal := strconv.Itoa(index)
	require.Equal(t, "name-"+ordinal, value["name"])
	require.Equal(t, int64(index), value["seq"])
	require.Equal(t, float64(index)/2, value["score"])
	require.Equal(t, index, value["rank"])
	require.Equal(t, index%2 == 0, value["active"])
	require.Equal(t, "group-"+strconv.Itoa(index%17), value["group"])
	require.Equal(t, "kind-"+strconv.Itoa(index%5), value["kind"])
	require.Equal(t, "owner-"+strconv.Itoa(index%31), value["owner"])
	require.Equal(t, "region-"+strconv.Itoa(index%7), value["region"])
	require.Equal(t, "status-"+strconv.Itoa(index%3), value["status"])
	require.Equal(t, "label-"+strconv.Itoa(index%11), value["label"])
	require.Nil(t, value["notes"])
	require.Equal(t, int32(index), value["unit"])
	require.Equal(t, float32(index)/4, value["weight"])
	require.Equal(t, uint16(index), value["version"])

	tags, ok := value["tags"].([]any)
	require.True(t, ok, "a list property keeps its list type")
	require.Equal(t, []any{"a", "b"}, tags)
	scores, ok := value["scores"].([]int64)
	require.True(t, ok, "a typed list property keeps its element type")
	require.Equal(t, []int64{int64(index), int64(index + 1), int64(index + 2)}, scores)
	aliases, ok := value["aliases"].([]string)
	require.True(t, ok, "a string-list property keeps its type")
	require.Equal(t, []string{"first", "second"}, aliases)
}

// requireWideMapDelivered asserts the whole delivered image of the wide shape.
func requireWideMapDelivered(t *testing.T, result *ExecuteResult, rows int) {
	t.Helper()
	require.NotNil(t, result)
	require.Equal(t, []string{"row"}, result.Columns)
	require.Len(t, result.Rows, rows)
	requireWideMapRow(t, result.Rows[0], 0)
	requireWideMapRow(t, result.Rows[rows-1], rows-1)
}

// TestCacheHitDetachesReportedWideMapRows covers the wide cached read: every
// hit returns the caller's own complete image of it, and the caller changing
// that image changes neither the cached image nor a later hit.
func TestCacheHitDetachesReportedWideMapRows(t *testing.T) {
	for name, cache := range cacheHitOwnershipCaches() {
		t.Run(name, func(t *testing.T) {
			source := nor307WideMapResult(nor307WideCacheHitRows)
			cache.Put(nor307WideCacheHitQuery, nil, source, time.Hour)

			// The caller keeps its own result after publication. Changing all of
			// it may not reach the cached image.
			sourceRowValue := source.Rows[0][0].(map[string]any)
			sourceRowValue["seq"] = "changed by caller"
			sourceRowValue["tags"].([]any)[0] = "changed by caller"
			sourceRowValue["scores"].([]int64)[0] = -1
			sourceRowValue["new"] = "changed by caller"
			delete(sourceRowValue, "name")
			source.Rows = source.Rows[:1]
			source.Columns[0] = "changed by caller"

			delivered, found := cache.Get(nor307WideCacheHitQuery, nil)
			require.True(t, found)
			requireWideMapDelivered(t, delivered, nor307WideCacheHitRows)

			// The delivery is the caller's own image: no row, map, list or column
			// container is shared with the caller's source result.
			sourceRow := source.Rows[0]
			deliveredRow := delivered.Rows[0]
			require.NotEqual(t, reflect.ValueOf(sourceRow).Pointer(), reflect.ValueOf(deliveredRow).Pointer(),
				"a delivered row is not the source row")
			deliveredValue := deliveredRow[0].(map[string]any)
			require.NotEqual(t,
				reflect.ValueOf(sourceRow[0].(map[string]any)).Pointer(),
				reflect.ValueOf(deliveredValue).Pointer(),
				"a delivered map is not the source map")
			require.NotEqual(t,
				reflect.ValueOf(sourceRow[0].(map[string]any)["tags"].([]any)).Pointer(),
				reflect.ValueOf(deliveredValue["tags"].([]any)).Pointer(),
				"a delivered list is not the source list")
			require.NotEqual(t,
				reflect.ValueOf(sourceRow[0].(map[string]any)["aliases"].([]string)).Pointer(),
				reflect.ValueOf(deliveredValue["aliases"].([]string)).Pointer(),
				"a delivered typed list is not the source list")
			require.NotEqual(t,
				reflect.ValueOf(source.Columns).Pointer(), reflect.ValueOf(delivered.Columns).Pointer(),
				"delivered columns are not the source columns")

			// Deliveries are distinct from each other as well.
			second, found := cache.Get(nor307WideCacheHitQuery, nil)
			require.True(t, found)
			requireWideMapDelivered(t, second, nor307WideCacheHitRows)
			require.NotSame(t, delivered, second)
			require.NotEqual(t, reflect.ValueOf(delivered.Rows).Pointer(), reflect.ValueOf(second.Rows).Pointer())
			secondValue := second.Rows[0][0].(map[string]any)
			require.NotEqual(t, reflect.ValueOf(deliveredValue).Pointer(), reflect.ValueOf(secondValue).Pointer())

			// Changing the delivered image may not change the next hit.
			deliveredValue["seq"] = "changed by caller"
			deliveredValue["tags"].([]any)[0] = "changed by caller"
			deliveredValue["scores"].([]int64)[1] = -1
			deliveredValue["aliases"].([]string)[0] = "changed by caller"
			delivered.Rows[1] = nil
			delivered.Rows = delivered.Rows[:2]
			delivered.Columns[0] = "changed by caller"

			third, found := cache.Get(nor307WideCacheHitQuery, nil)
			require.True(t, found)
			requireWideMapDelivered(t, third, nor307WideCacheHitRows)
		})
	}
}

// TestCacheHitPreservesReportedWideMapNestedIdentity covers what a cheaper copy
// of the wide shape must keep: a container reached by more than one row stays
// one container inside the delivered image, and it is still not the source's.
func TestCacheHitPreservesReportedWideMapNestedIdentity(t *testing.T) {
	for name, cache := range cacheHitOwnershipCaches() {
		t.Run(name, func(t *testing.T) {
			shared := map[string]any{"name": "shared", "tags": []any{"a", "b"}}
			source := &ExecuteResult{
				Columns: []string{"row"},
				Rows:    [][]any{{shared}, {shared}},
			}
			cache.Put(nor307WideCacheHitQuery, nil, source, time.Hour)

			delivered, found := cache.Get(nor307WideCacheHitQuery, nil)
			require.True(t, found)
			require.Len(t, delivered.Rows, 2)

			first := delivered.Rows[0][0].(map[string]any)
			require.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(delivered.Rows[1][0].(map[string]any)).Pointer(),
				"a map returned by two rows stays one map in the delivered image")
			require.NotEqual(t, reflect.ValueOf(shared).Pointer(), reflect.ValueOf(first).Pointer())

			first["name"] = "changed by caller"
			first["tags"].([]any)[0] = "changed by caller"
			require.Equal(t, "shared", shared["name"])
			require.Equal(t, []any{"a", "b"}, shared["tags"])

			next, found := cache.Get(nor307WideCacheHitQuery, nil)
			require.True(t, found)
			nextValue := next.Rows[0][0].(map[string]any)
			require.Equal(t, "shared", nextValue["name"])
			require.Equal(t, []any{"a", "b"}, nextValue["tags"])
			require.Equal(t, reflect.ValueOf(nextValue).Pointer(),
				reflect.ValueOf(next.Rows[1][0].(map[string]any)).Pointer())
		})
	}
}

// TestCacheHitDetachesReportedVectorRows covers the report's vector control: a
// 25-row read whose rows return nodes with mutable chunk vectors. Every hit
// delivers vectors the caller owns, so changing them changes nothing retained.
func TestCacheHitDetachesReportedVectorRows(t *testing.T) {
	const (
		query    = "MATCH (n:Nor307Vector) RETURN n"
		rows     = 25
		chunks   = 128
		chunkLen = 1024
	)
	for name, cache := range cacheHitOwnershipCaches() {
		t.Run(name, func(t *testing.T) {
			source := nor307BenchmarkResult("vectors", rows)
			cache.Put(query, nil, source, time.Hour)

			sourceNode := source.Rows[0][1].(*storage.Node)
			require.Len(t, sourceNode.ChunkEmbeddings, chunks)

			// The caller keeps its own result after publication.
			sourceNode.ChunkEmbeddings[0][0] = -1
			sourceNode.Properties["name"] = "changed by caller"
			source.Rows = source.Rows[:1]

			delivered, found := cache.Get(query, nil)
			require.True(t, found)
			require.Len(t, delivered.Rows, rows)
			require.Equal(t, []string{"name", "node"}, delivered.Columns)

			deliveredNode, ok := delivered.Rows[0][1].(*storage.Node)
			require.True(t, ok, "a returned node keeps its type")
			require.Len(t, deliveredNode.ChunkEmbeddings, chunks)
			require.Len(t, deliveredNode.ChunkEmbeddings[0], chunkLen)
			require.Equal(t, float32(1), deliveredNode.ChunkEmbeddings[0][0],
				"the published image is the caller's result at publication, not later changes")
			require.Equal(t, "name-0", deliveredNode.Properties["name"])

			// The vectors a caller receives are its own bytes.
			require.NotSame(t, sourceNode, deliveredNode)
			require.NotEqual(t,
				reflect.ValueOf(sourceNode.ChunkEmbeddings[0]).Pointer(),
				reflect.ValueOf(deliveredNode.ChunkEmbeddings[0]).Pointer(),
				"a delivered vector chunk is not the source chunk")

			second, found := cache.Get(query, nil)
			require.True(t, found)
			secondNode := second.Rows[0][1].(*storage.Node)
			require.NotEqual(t,
				reflect.ValueOf(deliveredNode.ChunkEmbeddings[0]).Pointer(),
				reflect.ValueOf(secondNode.ChunkEmbeddings[0]).Pointer(),
				"each delivery owns its vectors")

			// Changing a delivery's vectors and properties may not change the next
			// hit, at any chunk.
			deliveredNode.ChunkEmbeddings[0][0] = -7
			deliveredNode.ChunkEmbeddings[chunks-1][0] = -7
			deliveredNode.Properties["name"] = "changed by caller"
			deliveredNode.Labels[0] = "changed by caller"
			delivered.Rows = nil

			third, found := cache.Get(query, nil)
			require.True(t, found)
			thirdNode := third.Rows[0][1].(*storage.Node)
			require.Equal(t, float32(1), thirdNode.ChunkEmbeddings[0][0])
			require.Equal(t, float32(chunks), thirdNode.ChunkEmbeddings[chunks-1][0])
			require.Equal(t, "name-0", thirdNode.Properties["name"])
			require.Equal(t, []string{"Nor307"}, thirdNode.Labels)
		})
	}
}
