package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyRecoveryPreservesExactIntegerLexemes(t *testing.T) {
	raw := []byte(`{"node":{"id":"legacy","properties":{"integer":9007199254740993,"nested":[9007199254740993,1.5,null,[]]}}}`)
	var record WALNodeData
	require.NoError(t, decodeRecoveryPayload(raw, &record))
	require.Equal(t, int64(9007199254740993), record.Node.Properties["integer"])
	require.Equal(t, []interface{}{int64(9007199254740993), 1.5, nil, []interface{}{}}, record.Node.Properties["nested"])
	var buffered WALBufferedCommitData
	require.NoError(t, decodeRecoveryPayload([]byte(`{"ops":[{"node":{"id":"legacy","properties":{"integer":9007199254740993}}}]}`), &buffered))
	require.Equal(t, int64(9007199254740993), buffered.Ops[0].Node.Properties["integer"])
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"nodes":[{"id":"legacy","properties":{"integer":9007199254740993}}]}`), 0600))
	snapshot, err := LoadSnapshot(t.Context(), path)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), snapshot.Nodes[0].Properties["integer"])
}

// A snapshot is the store's last copy once automatic compaction truncates the
// journal, so it carries the vectors and the embedding metadata its reads
// return (reports 45, 53.5). The frame holds those vectors inline, so a
// restored entity is settled: it never claims separate storage again, the
// metadata that describes the vectors is restored with them, metadata
// unrelated to the vectors is restored as stored, and the stored entity itself
// is not modified.
func TestSnapshotRestoresStoredVectorsAndMetadata(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	vectors := make([][]float32, 16)
	for i := range vectors {
		vectors[i] = make([]float32, 1536)
		vectors[i][0] = 1
	}
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:large", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "kept"}, ChunkEmbeddings: vectors, EmbedMeta: map[string]interface{}{"has_embedding": true, "has_chunks": true, "chunk_count": int64(16), "unrelated": "kept"}})
	require.NoError(t, err)
	source, err := engine.GetNode(t.Context(), "test:large")
	require.NoError(t, err)
	require.Equal(t, vectors, source.ChunkEmbeddings, "the store keeps the vectors the test wrote")

	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{}))
	visitor := &recoveryFieldsVisitor{}
	_, err = ReadSnapshot(t.Context(), bytes.NewReader(output.Bytes()), visitor)
	require.NoError(t, err)
	require.Len(t, visitor.nodes, 1)
	restored := visitor.nodes[0]
	require.Equal(t, source.ChunkEmbeddings, restored.ChunkEmbeddings, "a snapshot carries the stored vectors")
	require.Empty(t, restored.NamedEmbeddings)
	require.Equal(t, source.EmbedMeta, restored.EmbedMeta, "embedding metadata is restored with the vectors it describes")
	require.False(t, restored.RuntimeMetadata().EmbeddingsStoredSeparately, "a frame holds its vectors inline, so it never claims separate storage")
	require.Equal(t, true, restored.EmbedMeta["has_embedding"])
	require.Equal(t, int64(16), restored.EmbedMeta["chunk_count"])
	require.Equal(t, "kept", restored.EmbedMeta["unrelated"], "metadata unrelated to the vectors is restored")
	require.False(t, NodeNeedsEmbedding(restored), "a restored node holds the vectors it was embedded with")

	original, err := engine.GetNode(t.Context(), "test:large")
	require.NoError(t, err)
	require.True(t, original.EmbedMeta["has_embedding"].(bool), "snapshot must not mutate stored metadata")
	require.Equal(t, source.ChunkEmbeddings, original.ChunkEmbeddings, "snapshot must not mutate stored vectors")
}

func TestLegacyRecoveryIntegerRangeAndMalformedValues(t *testing.T) {
	for _, tc := range []struct {
		literal string
		want    interface{}
		invalid bool
	}{
		// The release's legacy recovery reader already interpreted integer
		// lexemes exactly; this PR must not reinterpret them through float64.
		{"1152921504606847000", int64(1152921504606847000), false},
		{"9223372036854775807", int64(9223372036854775807), false},
		{"-9223372036854775808", int64(-9223372036854775808), false},
		{"18446744073709551615", uint64(18446744073709551615), false},
		{"1.0", float64(1), false}, {"1e0", float64(1), false},
		{"18446744073709551616", nil, true}, {"-9223372036854775809", nil, true}, {"1e1000", nil, true},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			var record WALNodeData
			err := decodeRecoveryPayload([]byte(`{"node":{"id":"legacy","properties":{"value":`+tc.literal+`}}}`), &record)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, record.Node.Properties["value"])
		})
	}
	var bulk WALBulkNodesData
	require.Error(t, decodeRecoveryPayload([]byte(`{"nodes":[{"properties":{"nested":[{"value":18446744073709551616}],"other":18446744073709551616}}]}`), &bulk))
	path := filepath.Join(t.TempDir(), "concatenated.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"nodes":[]} {"nodes":[]}`), 0600))
	_, err := LoadSnapshot(t.Context(), path)
	require.Error(t, err, "extra JSON values must not be silently ignored")
}

// A flagged body can survive without its sidecar records. Export must not
// advertise missing chunks, while a real named vector still backs availability.
func TestSnapshotMetadataDoesNotAdvertiseUnavailableVectors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		vector []float32
	}{{"nil", nil}, {"empty", []float32{}}, {"present", []float32{1, 2}}} {
		t.Run(tc.name, func(t *testing.T) {
			node := &Node{ID: "test:missing", NamedEmbeddings: map[string][]float32{"title": tc.vector}, EmbedMeta: map[string]any{
				"has_embedding": true, "has_chunks": true, "chunk_count": int64(16), "embedding_dimensions": 2, "embedded_at": "before", "unrelated": "kept",
			}}
			metadata := node.RuntimeMetadata()
			metadata.EmbeddingsStoredSeparately = true
			node.SetRuntimeMetadata(metadata)
			exported := snapshotNodeVectorMetadata(node)
			require.NotSame(t, node, exported)
			require.Empty(t, exported.ChunkEmbeddings)
			require.False(t, exported.RuntimeMetadata().EmbeddingsStoredSeparately)
			require.NotContains(t, exported.EmbedMeta, "has_chunks")
			require.NotContains(t, exported.EmbedMeta, "chunk_count")
			require.Equal(t, len(tc.vector) > 0, exported.EmbedMeta["has_embedding"] == true)
			if len(tc.vector) == 0 {
				require.NotContains(t, exported.EmbedMeta, "embedding_dimensions")
				require.NotContains(t, exported.EmbedMeta, "embedded_at")
			}
			require.Equal(t, node.NamedEmbeddings, exported.NamedEmbeddings)
			require.Equal(t, "kept", exported.EmbedMeta["unrelated"])
			require.True(t, node.RuntimeMetadata().EmbeddingsStoredSeparately)
			require.Equal(t, true, node.EmbedMeta["has_chunks"], "export does not mutate the source")
			require.Equal(t, int64(16), node.EmbedMeta["chunk_count"])
		})
	}
}
