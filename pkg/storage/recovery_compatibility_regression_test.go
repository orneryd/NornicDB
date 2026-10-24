package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryReadsLegacySnapshotFormats(t *testing.T) {
	n := &Node{ID: "a", Labels: []string{"Legacy"}, Properties: map[string]any{"name": "kept", "integer": int64(9007199254740993)}}
	legacy := &Snapshot{Sequence: 19, Timestamp: time.Unix(20, 0).In(time.FixedZone("Fixture", 3600)), Version: "legacy", Nodes: []*Node{n}}
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	loaded, err := LoadSnapshot(path)
	require.NoError(t, err)
	require.True(t, legacy.Timestamp.Equal(loaded.Timestamp))
	expected := *legacy
	expected.Timestamp = loaded.Timestamp
	assert.Equal(t, &expected, loaded)
	var framed bytes.Buffer
	buffered := bufio.NewWriter(&framed)
	require.NoError(t, writeSnapshotHeader(buffered, 19, legacy.Timestamp))
	nodeJSON, err := json.Marshal(n)
	require.NoError(t, err)
	require.NoError(t, writeSnapshotFrame(buffered, streamSnapshotNode, json.RawMessage(nodeJSON), defaultSnapshotRecordSize))
	require.NoError(t, writeSnapshotFrame(buffered, streamSnapshotFooter, snapshotCompletion{Nodes: 1}, defaultSnapshotRecordSize))
	require.NoError(t, buffered.Flush())
	framed.Bytes()[4] = 1
	visitor := &recoveryFieldsVisitor{}
	meta, err := ReadSnapshot(bytes.NewReader(framed.Bytes()), visitor)
	require.NoError(t, err)
	assert.Equal(t, uint64(19), meta.Sequence)
	require.Len(t, visitor.nodes, 1)
	assert.Equal(t, n, visitor.nodes[0])
}

func TestRecoverySidecarKeepsLegacyProjectionAndRejectsInvalidExtensions(t *testing.T) {
	n, _ := recoveryFieldFixture()
	source := WALNodeData{Node: n, TxID: "legacy-tx"}
	raw, err := marshalWALRecoveryPayload(nil, source)
	require.NoError(t, err)
	var old WALNodeData
	require.NoError(t, json.Unmarshal(raw, &old))
	assert.Equal(t, n.ID, old.Node.ID)
	assert.Equal(t, n.Labels, old.Node.Labels)
	assert.Equal(t, "legacy-tx", old.TxID)
	assert.Equal(t, float64(9007199254740992), old.Node.Properties["integer"], "old readers retain their previous JSON semantics")
	for _, raw := range []string{`{"_nornicdb_encoding":"future","_nornicdb_data":"AQ=="}`, `{"_nornicdb_encoding":"storage-msgpack-v1"}`, `{"_nornicdb_encoding":"storage-msgpack-v1","_nornicdb_data":"AQ=="}`, `{"node":`} {
		var target WALNodeData
		assert.Error(t, decodeRecoveryPayload([]byte(raw), &target), raw)
	}
	// Extensions remain private to recovery; public entity JSON keeps its shape.
	public, err := json.Marshal(n)
	require.NoError(t, err)
	assert.NotContains(t, string(public), "_nornicdb_")
	assert.NotContains(t, string(public), "VisibilitySuppressed")
}

func TestRecoverySnapshotRejectsTruncationCorruptionAndUnknownVersion(t *testing.T) {
	n, _ := recoveryFieldFixture()
	var buffer bytes.Buffer
	require.NoError(t, writeSnapshotCollection(&buffer, &Snapshot{Nodes: []*Node{n}}))
	raw := buffer.Bytes()
	for _, cut := range []int{0, 4, 12, len(raw) - 1} {
		_, err := ReadSnapshot(bytes.NewReader(raw[:cut]), &recoveryFieldsVisitor{})
		assert.Error(t, err)
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[30] ^= 1
	_, err := ReadSnapshot(bytes.NewReader(corrupt), &recoveryFieldsVisitor{})
	assert.Error(t, err)
	future := append([]byte(nil), raw...)
	future[4] = 3
	visitor := &recoveryFieldsVisitor{}
	_, err = ReadSnapshot(bytes.NewReader(future), visitor)
	require.Error(t, err)
	assert.Empty(t, visitor.nodes, "unsupported versions must fail before visiting entities")
}

func TestRecoveryReplayUndoPreservesEntityFields(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "bulk"}[bulk], func(t *testing.T) {
			n, e := recoveryFieldFixture()
			n.ID = "a"
			e.ID = "e"
			e.StartNode = "a"
			e.EndNode = "b"
			engine := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = engine.Close() })
			ns := NewNamespacedEngine(engine, "test")
			_, err := ns.CreateNode(&Node{ID: "b"})
			require.NoError(t, err)
			apply := func(op OperationType, data any) WALEntry {
				raw, err := marshalWALRecoveryPayload(nil, data)
				require.NoError(t, err)
				entry := WALEntry{Operation: op, Database: "test", Data: raw}
				require.NoError(t, ReplayWALEntry(engine, entry))
				return entry
			}
			if bulk {
				apply(OpBulkNodes, WALBulkNodesData{Nodes: []*Node{n}})
				apply(OpBulkEdges, WALBulkEdgesData{Edges: []*Edge{e}})
			} else {
				apply(OpCreateNode, WALNodeData{Node: n})
				apply(OpCreateEdge, WALEdgeData{Edge: e})
			}
			gotN, err := ns.GetNode("a")
			require.NoError(t, err)
			assertRecoveryNodeFields(t, n, gotN)
			gotE, err := ns.GetEdge("e")
			require.NoError(t, err)
			assertRecoveryEdgeFields(t, e, gotE)
			var deleteN, deleteE WALEntry
			if bulk {
				deleteE = apply(OpBulkDeleteEdges, WALBulkDeleteEdgesData{IDs: []string{"e"}, OldEdges: []*Edge{e}})
				deleteN = apply(OpBulkDeleteNodes, WALBulkDeleteNodesData{IDs: []string{"a"}, OldNodes: []*Node{n}})
			} else {
				deleteE = apply(OpDeleteEdge, WALDeleteData{ID: "e", OldEdge: e})
				deleteN = apply(OpDeleteNode, WALDeleteData{ID: "a", OldNode: n})
			}
			require.NoError(t, UndoWALEntry(engine, deleteN))
			require.NoError(t, UndoWALEntry(engine, deleteE))
			gotN, err = ns.GetNode("a")
			require.NoError(t, err)
			assertRecoveryNodeFields(t, n, gotN)
			gotE, err = ns.GetEdge("e")
			require.NoError(t, err)
			assertRecoveryEdgeFields(t, e, gotE)
			updated := *n
			updated.UpdatedAt = n.UpdatedAt.Add(time.Hour)
			entry := apply(OpUpdateNode, WALNodeData{Node: &updated, OldNode: n})
			require.NoError(t, UndoWALEntry(engine, entry))
			gotN, err = ns.GetNode("a")
			require.NoError(t, err)
			assertRecoveryNodeFields(t, n, gotN)
			updatedE := *e
			updatedE.Confidence = 0.9
			entry = apply(OpUpdateEdge, WALEdgeData{Edge: &updatedE, OldEdge: e})
			require.NoError(t, UndoWALEntry(engine, entry))
			gotE, err = ns.GetEdge("e")
			require.NoError(t, err)
			assertRecoveryEdgeFields(t, e, gotE)
		})
	}
}

func TestRecoverySnapshotSaveRestoreAcrossRoutes(t *testing.T) {
	for _, bare := range []bool{false, true} {
		t.Run(map[bool]string{false: "qualified", true: "bare"}[bare], func(t *testing.T) {
			n, e := recoveryFieldFixture()
			namespace := "test"
			if bare {
				namespace = defaultRecoveryDatabase()
				n.ID = "a"
				e.ID = "e"
				e.StartNode = "a"
				e.EndNode = "b"
			}
			b := &Node{ID: NodeID(namespace + ":b")}
			if bare {
				b.ID = "b"
			}
			snapshot := &Snapshot{Sequence: 37, Version: "custom", Timestamp: time.Unix(120, 456), Nodes: []*Node{n, b}, Edges: []*Edge{e}}
			path := filepath.Join(t.TempDir(), "snapshot.nds")
			require.NoError(t, SaveSnapshot(snapshot, path))
			loaded, err := LoadSnapshot(path)
			require.NoError(t, err)
			assert.Equal(t, snapshot.Sequence, loaded.Sequence)
			assert.Equal(t, snapshot.Version, loaded.Version)
			assert.True(t, snapshot.Timestamp.Equal(loaded.Timestamp))
			memory, _, err := runtimeFixtureOwner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.TempDir(), path))
			require.NoError(t, err)
			t.Cleanup(func() { _ = memory.Close() })
			target := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = target.Close() })
			_, status, err := RecoverIntoEngine(target, t.TempDir(), path)
			require.NoError(t, err)
			assert.True(t, status.SnapshotStreaming)
			for _, engine := range []Engine{memory, target} {
				got, err := engine.GetNode(NodeID(namespace + ":a"))
				require.NoError(t, err)
				assertRecoveryNodeFields(t, n, got)
				gotE, err := engine.GetEdge(EdgeID(namespace + ":e"))
				require.NoError(t, err)
				assertRecoveryEdgeFields(t, e, gotE)
			}
		})
	}
}

func TestRecoveryCodecPreservesTemporalPropertiesWithoutMutatingInputs(t *testing.T) {
	location, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	stamp := time.Date(2026, 2, 3, 4, 5, 6, 7, location)
	node := &Node{ID: "test:a", Properties: map[string]any{"stamp": stamp, "nested": []any{map[string]any{"count": int64(9)}}}, EmbedMeta: map[string]any{"stamp": stamp}}
	originalProperties := node.Properties
	payload := WALBulkNodesData{Nodes: []*Node{node}}
	raw, err := marshalWALRecoveryPayload(nil, payload)
	require.NoError(t, err)
	assert.Same(t, node, payload.Nodes[0])
	assert.Equal(t, stamp, originalProperties["stamp"])
	var decoded WALBulkNodesData
	require.NoError(t, decodeRecoveryPayload(raw, &decoded))
	require.Len(t, decoded.Nodes, 1)
	got, ok := decoded.Nodes[0].Properties["stamp"].(time.Time)
	require.True(t, ok)
	assert.Equal(t, stamp, got)
	assert.Equal(t, stamp.Location().String(), got.Location().String())
	assert.Equal(t, node.EmbedMeta, decoded.Nodes[0].EmbedMeta)
}

func TestRecoveryCodecPointerAndNilPayloads(t *testing.T) {
	n, e := recoveryFieldFixture()
	for _, source := range []any{n, e, &WALNodeData{Node: n}, &WALEdgeData{Edge: e}, &WALDeleteData{OldNode: n, OldEdge: e}, &WALBulkNodesData{Nodes: []*Node{n}}, &WALBulkEdgesData{Edges: []*Edge{e}}, &WALBulkDeleteNodesData{OldNodes: []*Node{n}}, &WALBulkDeleteEdgesData{OldEdges: []*Edge{e}}} {
		raw, err := marshalWALRecoveryPayload(nil, source)
		require.NoError(t, err)
		assert.Contains(t, string(raw), walRecoveryEncoding)
	}
	for _, source := range []any{(*Node)(nil), (*Edge)(nil), (*WALNodeData)(nil), (*WALEdgeData)(nil), (*WALDeleteData)(nil), (*WALBulkNodesData)(nil), (*WALBulkEdgesData)(nil), (*WALBulkDeleteNodesData)(nil), (*WALBulkDeleteEdgesData)(nil)} {
		raw, err := marshalWALRecoveryPayload(nil, source)
		require.NoError(t, err)
		assert.JSONEq(t, "null", string(raw))
	}
	// Control records and empty bulk payloads keep working without entity data.
	raw, err := marshalWALRecoveryPayload(nil, map[string]any{"tx_id": "t"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"tx_id":"t"}`, string(raw))
	raw, err = encodeRecoveryPayload(WALBulkNodesData{})
	require.NoError(t, err)
	var bulk WALBulkNodesData
	require.NoError(t, decodeRecoveryPayload(raw, &bulk))
	assert.Nil(t, bulk.Nodes)
	var buffer bytes.Buffer
	raw, err = marshalWALRecoveryPayload(&buffer, WALNodeData{Node: n})
	require.NoError(t, err)
	var decoded WALNodeData
	require.NoError(t, decodeRecoveryPayload(raw, &decoded))
	assertRecoveryNodeFields(t, n, decoded.Node)
}

func TestRecoveryMaterializedSnapshotKeepsLargeEntitySupport(t *testing.T) {
	node := &Node{ID: "test:large", Properties: map[string]any{"body": string(bytes.Repeat([]byte{'x'}, defaultSnapshotRecordSize+1))}}
	path := filepath.Join(t.TempDir(), "large.nds")
	require.NoError(t, SaveSnapshot(&Snapshot{Nodes: []*Node{node}}, path), "materialized backups previously accepted entities larger than the streaming record limit")
	snapshot, err := LoadSnapshot(path)
	require.NoError(t, err)
	require.Len(t, snapshot.Nodes, 1)
	assert.Equal(t, node.Properties, snapshot.Nodes[0].Properties)
	file, err := os.Open(path)
	require.NoError(t, err)
	_, err = ReadSnapshot(file, &recoveryFieldsVisitor{})
	require.ErrorContains(t, err, "reader limit")
	require.NoError(t, file.Close())
	// Memory recovery retains its pre-existing 64KiB Badger value ceiling;
	// its storage error must be reached after the large frame was decoded.
	restored, _, err := runtimeFixtureOwner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(t.TempDir(), path))
	require.ErrorContains(t, err, "exceeded 65536 limit")
	if restored != nil {
		_ = restored.Close()
	}
	target, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	_, _, err = RecoverIntoEngine(target, t.TempDir(), path)
	require.NoError(t, err)
	got, err := target.GetNode(node.ID)
	require.NoError(t, err)
	assert.Equal(t, node.Properties, got.Properties)
}

type recordingSnapshotReader struct {
	*bytes.Reader
	largestRead int
}

func (r *recordingSnapshotReader) Read(p []byte) (int, error) {
	if len(p) > r.largestRead {
		r.largestRead = len(p)
	}
	return r.Reader.Read(p)
}
func TestRecoveryLargeClaimedSnapshotLengthDoesNotPreallocate(t *testing.T) {
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	require.NoError(t, writeSnapshotHeader(writer, 0, time.Now()))
	require.NoError(t, writer.WriteByte(streamSnapshotNode))
	require.NoError(t, binary.Write(writer, binary.LittleEndian, uint32(materializedSnapshotRecordSize)))
	require.NoError(t, writer.Flush())
	reader := &recordingSnapshotReader{Reader: bytes.NewReader(buffer.Bytes())}
	_, err := readSnapshot(reader, &recoveryFieldsVisitor{}, nil, materializedSnapshotRecordSize)
	require.Error(t, err)
	assert.LessOrEqual(t, reader.largestRead, 64<<10, "a claimed4GiB length must not become a giant read/allocation before data exists")
}

func TestRecoverySnapshotRejectsNilEntitiesWithoutReplacingBackup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot *Snapshot
		message  string
	}{
		{"nil snapshot", nil, "nil snapshot"},
		{"nil node", &Snapshot{Nodes: []*Node{nil}}, "nil node"},
		{"nil edge", &Snapshot{Edges: []*Edge{nil}}, "nil edge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.nds")
			backup := []byte("previous durable backup")
			require.NoError(t, os.WriteFile(path, backup, 0600))
			require.ErrorContains(t, SaveSnapshot(tc.snapshot, path), tc.message)
			actual, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, backup, actual)
			_, err = os.Stat(path + ".tmp")
			require.True(t, os.IsNotExist(err), "failed write left a temporary snapshot: %v", err)
		})
	}
}
