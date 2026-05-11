package storage

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// TestClassifyIndexName covers the five enum buckets across known prefixes
// plus arbitrary user-created names → "user_created".
func TestClassifyIndexName(t *testing.T) {
	cases := []struct {
		name     string
		internal string
		want     string
	}{
		{"label_simple", "label_Person", "label"},
		{"label_namespaced", "label_KnowledgeBase:Concept", "label"},
		{"edge_between_simple", "edge_between_KNOWS", "edge_between"},
		{"temporal_basic", "temporal_user_activity", "temporal"},
		{"embedding_chunk", "embedding_chunk", "embedding"},
		// User-named indexes — D-13c bucket-everything safeguard.
		{"user_pgvec_idx", "my_special_pgvec", "user_created"},
		{"user_random", "abc123", "user_created"},
		{"empty", "", "user_created"},
		{"prefix_collision", "labelfoo", "user_created"}, // missing underscore
		{"upper", "Label_Person", "user_created"},        // case-sensitive
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyIndexName(tc.internal))
			require.Equal(t, tc.want, ClassifyIndexName(tc.internal))
		})
	}
}

// TestIndexRebuild_NoFreeFormLabel drives 1k random user-created index
// names through the StorageMetrics index_rebuild_total counter and
// verifies the cardinality ceiling stays at 5 indices × 3 results = 15
// (not 1000 × 3). This is the falsifiability gate for T-04-02.
func TestIndexRebuild_NoFreeFormLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewStorageMetrics(reg, false /* tenant */, storageProbeAdapter{})
	require.NotNil(t, bag)

	for i := 0; i < 1000; i++ {
		userIdx := fmt.Sprintf("user_idx_%d", i)
		bucket := ClassifyIndexName(userIdx)
		bag.IndexRebuildTotal.WithLabelValues(bucket, "success").Inc()
	}

	count, err := testutil.GatherAndCount(reg, "nornicdb_storage_index_rebuild_total")
	require.NoError(t, err)
	// 1 user_created bucket × 1 result so far ⇒ 1 distinct series.
	require.LessOrEqual(t, count, 1, "user-created names must collapse to a single bucket")

	// Add a few known-prefix names to confirm the OTHER buckets still
	// surface independently.
	for _, idx := range []string{"label_X", "edge_between_Y", "temporal_Z", "embedding_W"} {
		bag.IndexRebuildTotal.WithLabelValues(ClassifyIndexName(idx), "success").Inc()
	}
	count2, err := testutil.GatherAndCount(reg, "nornicdb_storage_index_rebuild_total")
	require.NoError(t, err)
	require.LessOrEqual(t, count2, 5, "ceiling = 5 closed-enum index values × 1 result observed")
}
