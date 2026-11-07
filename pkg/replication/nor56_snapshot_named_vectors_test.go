package replication

import (
	"bytes"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNOR56SnapshotKeepsNamedVectorsWhenBodyCacheWarm: a replication snapshot
// written after earlier snapshots warmed the storage body cache carries the
// same named vectors and chunk vectors as the first one.
func TestNOR56SnapshotKeepsNamedVectorsWhenBodyCacheWarm(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	want := &storage.Node{ID: "test:n", Properties: map[string]any{"text": "keep"},
		NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}, ChunkEmbeddings: make([][]float32, 16),
		EmbedMeta: map[string]any{"chunk_count": int64(16)}}
	for i := range want.ChunkEmbeddings {
		want.ChunkEmbeddings[i] = make([]float32, 1536)
		want.ChunkEmbeddings[i][0] = float32(i + 1)
	}
	_, err := engine.CreateNode(t.Context(), storage.CopyNode(want))
	require.NoError(t, err)
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	for round := 0; round < 3; round++ {
		var buf bytes.Buffer
		require.NoError(t, adapter.WriteSnapshot(&buf))
		var snapshot storage.ReplicationSnapshot
		require.NoError(t, storage.DecodeCommandPayload(buf.Bytes(), &snapshot))
		require.Len(t, snapshot.Nodes, 1, "round %d", round)
		require.Equal(t, want.NamedEmbeddings, snapshot.Nodes[0].NamedEmbeddings, "round %d: named vectors", round)
		require.Equal(t, want.ChunkEmbeddings, snapshot.Nodes[0].ChunkEmbeddings, "round %d: chunk vectors", round)
	}
}
