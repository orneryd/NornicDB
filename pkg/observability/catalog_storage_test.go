package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewStorageMetrics + the closed kind/index enums
// land in Plan 04-04. Compile-fail until then.

// TestStorageMetrics_BytesKindClosedEnum asserts MET-10 + CONTEXT D-07:
// `nornicdb_storage_bytes{kind}` accepts only {nodes, edges, index, wal,
// search}. Cardinality ceiling = 5 (RESEARCH §Q11).
func TestStorageMetrics_BytesKindClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-04 (Storage bag delivery)")

	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_storage_bytes", 5, func(tenant string) {
		for _, kind := range []string{"nodes", "edges", "index", "wal", "search"} {
			bag.Bytes.WithLabelValues(kind).Set(1.0)
		}
		_ = tenant
	})
}

// TestIndexRebuild_IndexEnum asserts CONTEXT D-13c: index-rebuild metric
// uses the closed enum {label, edge_between, temporal, embedding,
// user_created}. user_created is the catch-all bucket so arbitrary
// user-named index strings NEVER become label values.
func TestIndexRebuild_IndexEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-04 (Storage bag delivery)")

	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_storage_index_rebuild_total", 15, func(tenant string) {
		for _, idx := range []string{"label", "edge_between", "temporal", "embedding", "user_created"} {
			for _, res := range []string{"success", "failure", "aborted"} {
				bag.IndexRebuild.WithLabelValues(idx, res).Inc()
			}
		}
		_ = tenant
	})
}
