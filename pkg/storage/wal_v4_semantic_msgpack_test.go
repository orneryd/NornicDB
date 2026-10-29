package storage

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestV4WALMigrationTranscodesHistoricalSemanticPayload(t *testing.T) {
	encodeOld := func(value any) []byte {
		t.Helper()
		var output bytes.Buffer
		require.NoError(t, gob.NewEncoder(&output).Encode(value))
		return output.Bytes()
	}
	stamp := time.Date(2024, 3, 5, 10, 20, 30, 123456789, time.UTC)
	properties := map[string]any{"count": int64(9), "name": "café", "when": stamp}
	named, err := EncodeNamedProperties(properties)
	require.NoError(t, err)
	// Compare the migration against the historical decoder's Go value
	// representation, including its fixed-offset time normalization.
	wantProperties, err := DecodeNamedProperties(named)
	require.NoError(t, err)
	metadata, err := EncodeNamedProperties(map[string]any{"source": "fixture"})
	require.NoError(t, err)
	leaf := encodeOld(v4GobNodePayload{
		ID: "test:n", Labels: []string{"Item"},
		NamedEmbeddings: map[string][]float32{"vector": {1, 2}},
		ChunkEmbeddings: [][]float32{{3, 4}},
		CreatedAtUnixNs: stamp.UnixNano(), UpdatedAtUnixNs: stamp.UnixNano(),
	})
	legacy := encodeOld(v4GobEntityTransactionEnvelope{
		Version: 1, Namespace: "test", LogicalTxID: "legacy-tx",
		Changes: []v4GobEntityTransactionChange{{
			Type: EntityCreateNode, Data: leaf, NamedProperties: named,
			EmbedMeta: metadata, VisibilitySuppressed: true,
		}},
	})
	data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: legacy})
	require.NoError(t, err)
	entry := WALEntry{Sequence: 7, Timestamp: stamp, Operation: OpEntityCommit, Database: "test", Data: data, Checksum: crc32Checksum(data)}
	original := priorWALRecord(t, 5, entry)
	dir := t.TempDir()
	source, destination := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	require.NoError(t, os.WriteFile(source, original, 0600))
	require.NoError(t, upgradeWALFileV4(source, destination, false, nil))
	stillOriginal, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, original, stillOriginal)
	entries, err := ReadWALEntries(destination, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, crc32Checksum(entries[0].Data), entries[0].Checksum)
	var converted walEntityCommitData
	require.NoError(t, decodeRecoveryPayload(entries[0].Data, &converted))
	reader := bytes.NewReader(converted.Payload)
	var value any
	require.NoError(t, msgpack.NewDecoder(reader).Decode(&value), "the semantic transaction must be MessagePack")
	require.Zero(t, reader.Len(), "the semantic transaction must contain one complete MessagePack value")
	require.False(t, bytes.Contains(converted.Payload, leaf), "nested gob entity payload must also be migrated")
	envelope, ops, err := DecodeEntityTransaction(converted.Payload)
	require.NoError(t, err)
	require.Equal(t, "legacy-tx", envelope.LogicalTxID)
	require.Len(t, ops, 1)
	require.NotNil(t, ops[0].Node)
	require.Equal(t, NodeID("test:n"), ops[0].Node.ID)
	require.Equal(t, wantProperties, ops[0].Node.Properties)
	require.True(t, stamp.Equal(ops[0].Node.Properties["when"].(time.Time)))
	require.Equal(t, map[string]any{"source": "fixture"}, ops[0].Node.EmbedMeta)
	require.Equal(t, [][]float32{{3, 4}}, ops[0].Node.ChunkEmbeddings)
	require.Equal(t, map[string][]float32{"vector": {1, 2}}, ops[0].Node.NamedEmbeddings)
	require.True(t, ops[0].Node.VisibilitySuppressed)
	require.True(t, stamp.Equal(ops[0].Node.CreatedAt))
	require.True(t, stamp.Equal(ops[0].Node.UpdatedAt))
}

func TestV4WALMigrationPreservesCurrentSemanticPayload(t *testing.T) {
	stamp := time.Date(2024, 3, 5, 10, 20, 30, 123456789, time.UTC)
	payload, err := EncodeEntityTransaction("test", "current", []Operation{{
		Type: OpCreateNode, Node: &Node{ID: "test:n", Properties: map[string]any{"count": int64(9)}},
	}})
	require.NoError(t, err)
	data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: payload})
	require.NoError(t, err)
	entry := WALEntry{Sequence: 8, Timestamp: stamp, Operation: OpEntityCommit, Database: "test", Data: data, Checksum: crc32Checksum(data)}
	dir := t.TempDir()
	source, destination := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	require.NoError(t, os.WriteFile(source, priorWALRecord(t, walFormatVersionCurrent, entry), 0600))
	require.NoError(t, upgradeWALFileV4(source, destination, false, nil))
	entries, err := ReadWALEntries(destination, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, data, entries[0].Data)
	require.Equal(t, crc32Checksum(data), entries[0].Checksum)
}

func TestV4WALMigrationRejectsMalformedHistoricalSemanticPayload(t *testing.T) {
	data, err := marshalWALRecoveryPayload(nil, walEntityCommitData{Payload: []byte("not a gob transaction")})
	require.NoError(t, err)
	entry := WALEntry{Sequence: 9, Timestamp: time.Unix(9, 0), Operation: OpEntityCommit, Database: "test", Data: data, Checksum: crc32Checksum(data)}
	original := priorWALRecord(t, 5, entry)
	dir := t.TempDir()
	source, destination := filepath.Join(dir, "wal.log"), filepath.Join(dir, "upgraded")
	require.NoError(t, os.WriteFile(source, original, 0600))
	require.Error(t, upgradeWALFileV4(source, destination, false, nil))
	_, err = os.Stat(destination)
	require.True(t, os.IsNotExist(err), "a rejected conversion must not leave a partial destination")
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, original, got)
}
