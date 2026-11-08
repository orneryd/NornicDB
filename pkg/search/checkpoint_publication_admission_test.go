package search

import (
	"context"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCheckpointMetadataDigestSurvivesWireRoundTrip(t *testing.T) {
	for _, empty := range []bool{false, true} {
		meta := VectorFileStoreMeta{Version: vecFileVersion, Dimensions: 3, DataSlots: 1, IDToOrdinal: map[string]int64{"a": 0}, PayloadDigest: "payload", NodeLabels: map[string][]string{"nil": nil, "empty": {}, "labels": {"Doc", "Vector"}}, NodeChunkVectors: map[string][]string{"nil": nil, "empty": {}, "chunks": {"a"}}, NodeNamedVectors: map[string]map[string]string{"nil": nil, "empty": {}, "named": {"v": "a"}}, NodePropertyVectors: map[string]map[string]string{"nil": nil, "empty": {}, "property": {"p": "a"}}}
		if empty {
			meta.IDToOrdinal = map[string]int64{}
			meta.DataSlots = 0
		}
		digest, err := vectorMetadataDigest(meta)
		require.NoError(t, err)
		meta.MetadataDigest = digest
		path := filepath.Join(t.TempDir(), "metadata")
		require.NoError(t, writeMsgpackSnapshot(path, &meta))
		decoded, err := decodeVectorCheckpoint(path)
		require.NoError(t, err)
		require.Equal(t, digest, decoded.MetadataDigest)
	}
}

// Cancellation after vector publication can leave the old population record
// beside a new, internally coherent vector tuple. Restart must reject that
// combination and rebuild the complete current text/vector population.
func TestCheckpointPopulationCancellationAfterVectorPublication(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	defer engine.Close()
	seed := compatStoreService(engine, dir)
	for _, n := range compatStoreNodes {
		_, err = engine.CreateNode(t.Context(), storage.CopyNode(n))
		require.NoError(t, err)
	}
	require.NoError(t, seed.BuildIndexes(t.Context()))
	require.NoError(t, seed.Close())
	require.NoError(t, engine.DeleteNode(t.Context(), "nornic:c"))
	svc := compatStoreService(engine, dir)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hit := false
	vectorCheckpointHook = func(base, phase string) {
		if base == filepath.Join(dir, "search", "vectors") && phase == "committed" {
			hit = true
			cancel()
		}
	}
	t.Cleanup(func() { vectorCheckpointHook = nil })
	err = svc.BuildIndexes(ctx)
	vectorCheckpointHook = nil
	require.True(t, hit)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, svc.Close())
	restored := compatStoreService(engine, dir)
	defer restored.Close()
	require.NoError(t, restored.BuildIndexes(t.Context()))
	require.Equal(t, 2, restored.fulltext().Count())
	require.Equal(t, 2, restored.EmbeddingCount())
	_, exists := restored.fulltext().GetDocument("nornic:c")
	require.False(t, exists)
	response, err := restored.Search(t.Context(), "charlie", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Empty(t, response.Results)
}
