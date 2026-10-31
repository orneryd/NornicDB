package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/vmihailenco/msgpack/v5"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise both WAL append routes: the ordinary recovery projection and the
// forced-flush buffered payload. Interface arguments must retain the concrete
// integer kinds of metadata while whole FLOAT properties keep their own kind.
func TestNumericWireWALIntegerEnvelopes(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true})
	wal := engine.bufferedCommitWAL
	t.Cleanup(func() { _ = wal.Close(); _ = engine.Close() })
	const sequence = uint64(9007199254740993)
	wal.sequence.Store(sequence)
	properties := map[string]any{"whole": float64(2), "integer": int64(9007199254740993)}
	commit := bufferedCommit{
		txID: "integer-envelope", logicalTxID: "accepted-integer-envelope", namespace: "test",
		version:        MVCCVersion{CommitTimestamp: time.Unix(1700000000, 123).UTC(), CommitSequence: math.MaxUint64},
		counterNodeMax: sequence, counterEdgeMax: math.MaxUint64, hasCounters: true,
		propKeyDrain: propKeyTxnDrain{counters: map[string]uint64{"test": sequence}},
		ops:          []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:a", Properties: properties}}},
	}
	require.NoError(t, wal.AppendBufferedCommit(&commit, engine.propKeyDict))
	_, err := wal.AppendWithDatabaseReturningSeq(OpTxCommit, WALTxData{TxID: "integer-envelope", OpCount: 7}, "test")
	require.NoError(t, err)
	require.NoError(t, wal.Checkpoint())
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for i, entry := range entries {
		assert.Equal(t, sequence+uint64(i)+1, entry.Sequence)
	}
	buffered, err := decompressWALBufferedCommitPayload(entries[0].Data)
	require.NoError(t, err)
	// An independent MessagePack reader is strict for typed uint64 fields in
	// TJ's current recovery format. Legacy WAL framing is a migration concern.
	// Author: #1665 implementation lane (Codex). The retained serialization
	// owner frames the MessagePack body; strip and verify that header first.
	serializer, body, hasHeader, err := splitSerializationHeader(buffered)
	require.NoError(t, err)
	require.True(t, hasHeader)
	require.Equal(t, serializerIDMsgpack, serializer)
	var old WALBufferedCommitData
	wire := msgpack.NewDecoder(bytes.NewReader(body))
	// Author: #1665 implementation lane (Codex). valueEncoder resets to
	// MessagePack defaults, so this independent reader must use Go field names.
	// JSON tags describe the historical JSON envelope, not this typed body.
	require.NoError(t, wire.Decode(&old))
	// Author: #1665 implementation lane (Codex). MVCCVersion.Compare defines
	// timestamp identity as UTC UnixNano plus CommitSequence. MessagePack can
	// restore the same instant with time.Local; location pointers are not wire
	// metadata. Check exact numeric values, and run all independent field checks
	// so a representation mismatch cannot conceal a later metadata failure.
	assert.Equal(t, commit.logicalTxID, old.LogicalTxID)
	assert.Equal(t, commit.txID, old.TxID)
	assert.Equal(t, commit.namespace, old.Namespace)
	assert.Equal(t, commit.version.CommitTimestamp.UnixNano(), old.Version.CommitTimestamp.UnixNano())
	assert.Equal(t, commit.version.CommitSequence, old.Version.CommitSequence)
	assert.Equal(t, commit.counterNodeMax, old.NodeMax)
	assert.Equal(t, commit.counterEdgeMax, old.EdgeMax)
	assert.Equal(t, commit.hasCounters, old.HasCounters)
	assert.Equal(t, commit.propKeyDrain.counters, old.PropKeyCounters)
	require.Len(t, old.Ops, 1)
	require.NotNil(t, old.Ops[0].Node)
	assert.Equal(t, properties, old.Ops[0].Node.Properties)
	assert.IsType(t, float64(0), old.Ops[0].Node.Properties["whole"], "the current typed wire retains FLOAT independently of its integer metadata")
	var current WALBufferedCommitData
	require.NoError(t, decodeRecoveryPayload(buffered, &current))
	require.Len(t, current.Ops, 1)
	require.NotNil(t, current.Ops[0].Node)
	assert.Equal(t, properties, current.Ops[0].Node.Properties)
	var marker WALTxData
	require.NoError(t, json.Unmarshal(entries[1].Data, &marker))
	assert.Equal(t, 7, marker.OpCount)
	var checkpoint struct {
		Sequence uint64 `json:"sequence"`
	}
	require.NoError(t, json.Unmarshal(entries[2].Data, &checkpoint))
	assert.Equal(t, sequence+2, checkpoint.Sequence)
}

func TestNumericWireWALFrameIntegerFields(t *testing.T) {
	entry := WALEntry{
		Sequence: math.MaxUint64, Checksum: math.MaxUint32,
		Timestamp: time.Unix(1700000000, 123).UTC(), Operation: OpTxCommit,
		Data: []byte(`{"op_count":7}`), Database: "test",
	}
	var buffer bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &buffer, &entry, nil, 0)
	require.NoError(t, err)
	var decoded WALEntry
	_, err = processWALEntryPayload(walPayloadDecode, nil, &decoded, payload, walFormatVersionCurrent)
	require.NoError(t, err)
	require.Equal(t, entry.Sequence, decoded.Sequence)
	require.Equal(t, entry.Checksum, decoded.Checksum)
	require.Equal(t, entry.Data, decoded.Data)
	for _, version := range []uint8{1, 2, 3, 4, 5} {
		_, err = processWALEntryPayload(walPayloadDecode, nil, &decoded, payload, version)
		require.Error(t, err, "superseded frames are owned by startup migration")
	}
	header := []byte(`{"seq":0.0}`)
	invalid := binary.LittleEndian.AppendUint32(nil, uint32(len(header)))
	invalid = append(invalid, header...)
	invalid = binary.LittleEndian.AppendUint32(invalid, 0)
	_, err = processWALEntryPayload(walPayloadDecode, nil, &decoded, invalid, walFormatVersionCurrent)
	require.Error(t, err, "typed integer readers must continue rejecting FLOAT tokens")
}

func TestNumericWireSnapshotIntegerMetadata(t *testing.T) {
	store := NewNamespacedEngine(newTestMemoryEngine(t), "test")
	t.Cleanup(func() { _ = store.Close() })
	properties := map[string]any{"whole": float64(2), "integer": int64(9007199254740993)}
	node := &Node{ID: "a", Properties: properties}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	edge := &Edge{ID: "r", StartNode: "a", EndNode: "a", Type: "SELF", Properties: properties}
	require.NoError(t, store.CreateEdge(edge))
	for _, materialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "streaming", true: "materialized"}[materialized], func(t *testing.T) {
			var raw bytes.Buffer
			if materialized {
				path := filepath.Join(t.TempDir(), "snapshot.nds")
				require.NoError(t, SaveSnapshot(&Snapshot{Sequence: math.MaxUint64, Nodes: []*Node{node}, Edges: []*Edge{edge}}, path))
				loaded, err := LoadSnapshot(path)
				require.NoError(t, err)
				require.Equal(t, uint64(math.MaxUint64), loaded.Sequence)
				require.Equal(t, properties, loaded.Nodes[0].Properties)
				require.Equal(t, properties, loaded.Edges[0].Properties)
				return
			}
			require.NoError(t, WriteSnapshot(context.Background(), store, &raw, SnapshotOptions{Sequence: math.MaxUint64}))
			visitor := &recoveryFieldsVisitor{}
			metadata, err := ReadSnapshot(bytes.NewReader(raw.Bytes()), visitor)
			require.NoError(t, err)
			require.Equal(t, uint64(math.MaxUint64), metadata.Sequence)
			require.Equal(t, uint64(1), metadata.NodeCount)
			require.Equal(t, uint64(1), metadata.EdgeCount)
			require.Equal(t, properties, visitor.nodes[0].Properties)
			require.Equal(t, properties, visitor.edges[0].Properties)
		})
	}
}
