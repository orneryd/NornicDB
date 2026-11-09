package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestLargeWALEmbeddingOutcomesRemainBounded(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "permanent_failure"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			wal, err := NewWAL(filepath.Join(root, "wal"), nil)
			require.NoError(t, err)
			engine := NewWALEngine(newTestMemoryEngine(t), wal)
			t.Cleanup(func() { _ = engine.Close() })
			id := NodeID("audit:large")
			_, err = engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"body": "original"}})
			require.NoError(t, err)
			vectors := make([][]float32, 2048)
			for i := range vectors {
				vectors[i] = make([]float32, 2048)
				for j := range vectors[i] {
					vectors[i][j] = float32(i*2048+j+1) / 3
				}
			}
			meta := map[string]any{"has_embedding": !failed, "embedding_failed": failed, "chunk_count": len(vectors)}
			outcome := embeddingWriteback(t, engine, id, vectors, meta, time.Time{})
			legacy, err := marshalWALRecoveryPayload(nil, WALNodeData{Node: outcome})
			require.NoError(t, err)
			require.Greater(t, len(legacy), int(walMaxEntrySize), "fixture must reproduce the old frame refusal")
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(t.Context(), outcome))
			got, err := engine.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, vectors, got.ChunkEmbeddings)
			require.Equal(t, failed, got.EmbedMeta["embedding_failed"])
			require.NoError(t, wal.Sync())
			entries, err := ReadWALEntriesFromDir(t.Context(), filepath.Join(root, "wal"))
			require.NoError(t, err)
			var audits int
			for _, entry := range entries {
				if entry.Operation != OpUpdateEmbedding {
					continue
				}
				audits++
				t.Logf("old audit bytes=%d bounded audit bytes=%d", len(legacy), len(entry.Data))
				require.Equal(t, "audit", entry.Database)
				require.Less(t, len(entry.Data), 1024)
				var data WALNodeData
				require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
				require.Equal(t, NodeID("large"), data.Node.ID)
				require.Empty(t, data.Node.ChunkEmbeddings)
				require.Empty(t, data.Node.Properties)
				require.EqualValues(t, len(vectors), data.Node.EmbedMeta["chunk_count"])
				require.EqualValues(t, 2048, data.Node.EmbedMeta["embedding_dimensions"])
				require.Equal(t, failed, data.Node.EmbedMeta["embedding_failed"])
				require.NoError(t, ReplayWALEntry(t.Context(), newTestMemoryEngine(t), entry), "audit replay stays a no-op")
			}
			require.Equal(t, 1, audits)
			engine.snapshotDir = filepath.Join(root, "snapshots")
			require.NoError(t, os.MkdirAll(engine.snapshotDir, 0755))
			require.NoError(t, engine.createSnapshotAndCompact(t.Context()))
			_, err = ReadWALEntriesFromDir(t.Context(), filepath.Join(root, "wal"))
			require.NoError(t, err, "compaction must leave a readable WAL")
			integrity, err := CheckWALIntegrity(filepath.Join(root, "wal", "wal.log"))
			require.NoError(t, err)
			require.True(t, integrity.Healthy)
			snapshots, err := os.ReadDir(engine.snapshotDir)
			require.NoError(t, err)
			require.Len(t, snapshots, 1)
			recovered, result, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.Context(), filepath.Join(root, "wal"), filepath.Join(engine.snapshotDir, snapshots[0].Name())))
			require.NoError(t, err)
			t.Cleanup(func() { _ = recovered.Close() })
			require.False(t, result.HasCriticalErrors())
			recoveredNode, err := recovered.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, "original", recoveredNode.Properties["body"])
		})
	}
}

// Audit projection must also exclude body fields, named vectors and arbitrary
// metadata: each can grow independently of the generated chunk vectors.
func TestWALEmbeddingAuditProjection(t *testing.T) {
	require.Nil(t, embeddingAuditNodeForWAL("audit", nil))
	for _, chunks := range [][][]float32{nil, {{1, 2}}} {
		node := &Node{
			ID: "audit:node", Labels: []string{"Document"},
			Properties:      map[string]any{"body": "kept in native storage"},
			NamedEmbeddings: map[string][]float32{"title": {1, 2}},
			ChunkEmbeddings: chunks,
			EmbedMeta:       map[string]any{"has_embedding": len(chunks) > 0, "embedding_failed": len(chunks) == 0, "chunk_texts": []string{"kept in native storage"}},
		}
		audit := embeddingAuditNodeForWAL("audit", node)
		require.Equal(t, NodeID("node"), audit.ID)
		require.Empty(t, audit.Labels)
		require.Empty(t, audit.Properties)
		require.Empty(t, audit.NamedEmbeddings)
		require.Empty(t, audit.ChunkEmbeddings)
		require.Len(t, audit.EmbedMeta, 4)
		require.Equal(t, len(chunks), audit.EmbedMeta["chunk_count"])
		dims := 0
		if len(chunks) > 0 {
			dims = 2
		}
		require.Equal(t, dims, audit.EmbedMeta["embedding_dimensions"])
		require.Equal(t, node.EmbedMeta["has_embedding"], audit.EmbedMeta["has_embedding"])
		require.Equal(t, node.EmbedMeta["embedding_failed"], audit.EmbedMeta["embedding_failed"])
		require.Equal(t, "kept in native storage", node.Properties["body"], "projection must not mutate the sidecar source")
	}
}
