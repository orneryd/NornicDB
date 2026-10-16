package storage

import (
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
	snapshot, err := LoadSnapshot(path)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), snapshot.Nodes[0].Properties["integer"])
}

func TestLightweightSnapshotDoesNotAdvertiseOmittedVectors(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	vectors := make([][]float32, 16)
	for i := range vectors {
		vectors[i] = make([]float32, 1536)
		vectors[i][0] = 1
	}
	_, err = engine.CreateNode(&Node{ID: "test:large", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "kept"}, ChunkEmbeddings: vectors, EmbedMeta: map[string]interface{}{"has_embedding": true, "has_chunks": true, "chunk_count": int64(16), "unrelated": "kept"}})
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{}))
	visitor := &recoveryFieldsVisitor{}
	_, err = ReadSnapshot(bytes.NewReader(output.Bytes()), visitor)
	require.NoError(t, err)
	require.Len(t, visitor.nodes, 1)
	restored := visitor.nodes[0]
	require.Empty(t, restored.ChunkEmbeddings)
	require.False(t, restored.EmbeddingsStoredSeparately)
	require.NotEqual(t, true, restored.EmbedMeta["has_embedding"])
	require.NotEqual(t, true, restored.EmbedMeta["has_chunks"])
	require.NotContains(t, restored.EmbedMeta, "chunk_count")
	require.Equal(t, "kept", restored.EmbedMeta["unrelated"])
	original, err := engine.GetNode("test:large")
	require.NoError(t, err)
	require.True(t, original.EmbedMeta["has_embedding"].(bool), "snapshot must not mutate stored metadata")
}

func TestLegacyRecoveryIntegerRangeAndMalformedValues(t *testing.T) {
	for _, tc := range []struct {
		literal string
		want    interface{}
		invalid bool
	}{
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
	_, err := LoadSnapshot(path)
	require.Error(t, err, "extra JSON values must not be silently ignored")
}
