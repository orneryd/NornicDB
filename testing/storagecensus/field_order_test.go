package storagecensus

import (
	"reflect"
	"testing"
)

// TestSortFieldWritesDeterministic tests that sortFieldWrites produces deterministic ordering
// with the same field and position, using fn as a tie-breaker.
func TestSortFieldWritesDeterministic(t *testing.T) {
	// Use the same field from pkg/storage.bufferedOrderResources.stripes
	// and the same position from pkg/storage/buffered_write_order.go:299
	// with different fn strings to test fn as tie-breaker
	rows := []fieldWrite{
		{field: "pkg/storage.bufferedOrderResources.stripes", fn: "pkg/storage.writeRelease[pkg/storage.entityAuthorityResources].release", pos: "pkg/storage/buffered_write_order.go:299"},
		{field: "pkg/storage.bufferedOrderResources.stripes", fn: "pkg/storage.writeRelease[pkg/storage.bufferedOrderResources].release", pos: "pkg/storage/buffered_write_order.go:299"},
		{field: "pkg/storage.BadgerEngine.nodeCount", fn: "pkg/storage.BadgerEngine.count", pos: "pkg/storage/engine.go:100"},
	}

	// Canonical expected order: sort by field, then pos, then fn
	expected := []fieldWrite{
		{field: "pkg/storage.BadgerEngine.nodeCount", fn: "pkg/storage.BadgerEngine.count", pos: "pkg/storage/engine.go:100"},
		{field: "pkg/storage.bufferedOrderResources.stripes", fn: "pkg/storage.writeRelease[pkg/storage.bufferedOrderResources].release", pos: "pkg/storage/buffered_write_order.go:299"},
		{field: "pkg/storage.bufferedOrderResources.stripes", fn: "pkg/storage.writeRelease[pkg/storage.entityAuthorityResources].release", pos: "pkg/storage/buffered_write_order.go:299"},
	}

	for i := range rows {
		for j := range rows {
			if i == j {
				continue
			}
			k := 3 - i - j
			got := []fieldWrite{rows[i], rows[j], rows[k]}
			sortFieldWrites(got)
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("input permutation %d,%d,%d produced %v; want %v", i, j, k, got, expected)
			}
		}
	}
}
