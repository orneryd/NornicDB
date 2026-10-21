package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBuildSkipsAStoredEmbeddingWithOtherDimensions: one stored node whose
// embedding has other dimensions than the index's does not fail the build.
// Its vector is skipped (and logged), its text is indexed, and every other
// node is indexed.
func TestBuildSkipsAStoredEmbeddingWithOtherDimensions(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(buildWriteNode("nornic:good", "goodterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	_, err = engine.CreateNode(buildWriteNode("nornic:bad", "badterm", []float32{1, 0}))
	require.NoError(t, err)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, []string{"nornic:good"}, buildWriteSearchIDs(t, svc, "goodterm"))
	require.Equal(t, []string{"nornic:bad"}, buildWriteSearchIDs(t, svc, "badterm"))
	_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:good", "v"))
	require.True(t, ok)
	_, ok = svc.getVectorLookup()(nodeNamedVectorKey("nornic:bad", "v"))
	require.False(t, ok)
}
