package storage

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The current WAL writes one typed payload, not a second JSON projection
// for older binaries. This exercises both direct and batched WAL writers.
func TestWALCurrentPayloadRoundTripWithoutLegacyProjection(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	when := time.Date(2026, 3, 4, 5, 6, 7, 8, zone)
	node := &Node{ID: "test:n", Properties: map[string]any{
		"integer": int64(9007199254740993), "when": when,
	}}
	payload := []byte{0, 0xff, 0x80, 42}
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: node}, "test"))
	batch := wal.NewBatch()
	require.NoError(t, batch.AppendNode(OpCreateNode, node))
	require.NoError(t, batch.Commit())

	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for _, entry := range entries {
		serializer, _, headered, err := splitSerializationHeader(entry.Data)
		require.NoError(t, err)
		require.True(t, headered, "operation %s must have one typed payload", entry.Operation)
		require.Equal(t, serializerIDMsgpack, serializer)
	}
	var commit walEntityCommitData
	require.NoError(t, decodeRecoveryPayload(entries[0].Data, &commit))
	require.Equal(t, payload, commit.Payload)
	for _, entry := range entries[1:] {
		var got WALNodeData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &got))
		require.Equal(t, node.ID, got.Node.ID)
		require.Equal(t, node.Properties["integer"], got.Node.Properties["integer"])
		gotTime, ok := got.Node.Properties["when"].(time.Time)
		require.True(t, ok)
		require.True(t, when.Equal(gotTime))
		require.Equal(t, zone.String(), gotTime.Location().String())
	}
}

// A semantic record for one Northwind-sized relationship batch. The
// transaction is prepared before timing so this measures the WAL payload
// representation, not operation construction or disk synchronization.
func BenchmarkWALSemanticPayloadEncoding(b *testing.B) {
	ops := make([]Operation, 2000)
	for i := range ops {
		id := fmt.Sprintf("nornic:edge-%d", i)
		ops[i] = Operation{Type: OpCreateEdge, Edge: &Edge{
			ID: EdgeID(id), StartNode: NodeID(fmt.Sprintf("nornic:customer-%d", i)),
			EndNode: NodeID(fmt.Sprintf("nornic:order-%d", i)), Type: "PURCHASED",
			Properties: map[string]any{"quantity": int64(i + 1), "region": "North America"},
		}}
	}
	semantic, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "nornic", LogicalID: "bench", Operations: ops})
	if err != nil {
		b.Fatal(err)
	}
	value := walEntityCommitData{Payload: semantic}
	var buffer bytes.Buffer
	payload, err := marshalWALRecoveryPayload(&buffer, value)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(semantic)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalWALRecoveryPayload(&buffer, value); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(payload)), "wal-bytes/op")
}
