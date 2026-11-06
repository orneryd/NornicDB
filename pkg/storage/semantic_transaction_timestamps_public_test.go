// Author: NOR-303 implementation lane (Codex), 2026-10-29.
package storage_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSemanticTransactionTimestampDistinctions(t *testing.T) {
	for _, at := range []time.Time{{}, time.Unix(0, 0).UTC(), time.Unix(0, -1).UTC(), time.Unix(0, 1).UTC(), time.Date(1600, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(2500, 1, 1, 0, 0, 0, 1, time.UTC)} {
		t.Run(at.Format(time.RFC3339Nano), func(t *testing.T) {
			payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
				Namespace: "test", LogicalID: "timestamp-codec", Mode: storage.CommitModeExplicit,
				Operations: []storage.Operation{
					{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:n", CreatedAt: at, UpdatedAt: at}},
					{Type: storage.OpCreateEdge, Edge: &storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:n", Type: "SELF", CreatedAt: at, UpdatedAt: at}},
				},
			})
			require.NoError(t, err)
			_, operations, err := storage.DecodeEntityTransaction(payload)
			require.NoError(t, err)
			fmt.Printf("NOR288_CODEC input=%s payload_hex=%s\n", at.Format(time.RFC3339Nano), hex.EncodeToString(payload))
			for _, field := range []struct {
				name  string
				value time.Time
			}{
				{"node.CreatedAt", operations[0].Node.CreatedAt}, {"node.UpdatedAt", operations[0].Node.UpdatedAt},
				{"edge.CreatedAt", operations[1].Edge.CreatedAt}, {"edge.UpdatedAt", operations[1].Edge.UpdatedAt},
			} {
				fmt.Printf("NOR288_CODEC field=%s input=%s decoded=%s\n", field.name, at.Format(time.RFC3339Nano), field.value.UTC().Format(time.RFC3339Nano))
				assert.True(t, at.Equal(field.value), "%s must preserve timestamp distinction", field.name)
			}
		})
	}
}

func TestSemanticTransactionTimestampFieldsAreIndependent(t *testing.T) {
	unset, epoch, before, after := time.Time{}, time.Unix(0, 0), time.Unix(0, -1), time.Unix(0, 1)
	for _, update := range []bool{false, true} {
		nodeKind, edgeKind := storage.OpCreateNode, storage.OpCreateEdge
		if update {
			nodeKind, edgeKind = storage.OpUpdateNode, storage.OpUpdateEdge
		}
		payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
			Namespace: "test", LogicalID: "independent-fields", Mode: storage.CommitModeExplicit,
			Operations: []storage.Operation{
				{Type: nodeKind, Node: &storage.Node{ID: "test:n", CreatedAt: unset, UpdatedAt: epoch}},
				{Type: edgeKind, Edge: &storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:n", CreatedAt: before, UpdatedAt: after}},
			},
		})
		require.NoError(t, err)
		envelope, ops, err := storage.DecodeEntityTransaction(payload)
		require.NoError(t, err)
		require.Equal(t, uint16(3), envelope.Version)
		require.Equal(t, nodeKind, ops[0].Type)
		require.Equal(t, edgeKind, ops[1].Type)
		assert.True(t, ops[0].Node.CreatedAt.IsZero())
		assert.True(t, ops[0].Node.UpdatedAt.Equal(epoch))
		assert.True(t, ops[1].Edge.CreatedAt.Equal(before))
		assert.True(t, ops[1].Edge.UpdatedAt.Equal(after))
	}
}

func TestSemanticTransactionTimestampVersionIsExplicit(t *testing.T) {
	payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
		Namespace: "test", LogicalID: "timestamp-version", Operations: []storage.Operation{{
			Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:n", CreatedAt: time.Unix(0, 0)},
		}},
	})
	require.NoError(t, err)
	var wire map[string]msgpack.RawMessage
	require.NoError(t, msgpack.Unmarshal(payload, &wire))
	for _, version := range []uint16{1, 2, 4} {
		wire["Version"], err = msgpack.Marshal(version)
		require.NoError(t, err)
		changed, err := msgpack.Marshal(wire)
		require.NoError(t, err)
		_, _, err = storage.DecodeEntityTransaction(changed)
		require.Error(t, err, "v3 timestamps must not be reinterpreted as version %d", version)
	}
	delete(wire, "Version")
	missing, err := msgpack.Marshal(wire)
	require.NoError(t, err)
	_, _, err = storage.DecodeEntityTransaction(missing)
	require.Error(t, err)
}

func TestSemanticTransactionReadsVersion2Timestamps(t *testing.T) {
	// Captured from pristine cd67390c0, not synthesized by the v3 encoder.
	raw, err := os.ReadFile("testdata/semantic_transaction_v2_timestamps.json")
	require.NoError(t, err)
	var fixture struct {
		Cases []struct {
			Input      string
			PayloadHex string `json:"payload_hex"`
			Decoded    map[string]string
		}
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Cases, 4)
	for _, tc := range fixture.Cases {
		t.Run(tc.Input, func(t *testing.T) {
			payload, err := hex.DecodeString(tc.PayloadHex)
			require.NoError(t, err)
			envelope, ops, err := storage.DecodeEntityTransaction(payload)
			require.NoError(t, err)
			require.Equal(t, uint16(2), envelope.Version)
			require.Len(t, ops, 2)
			for name, got := range map[string]time.Time{
				"node.CreatedAt": ops[0].Node.CreatedAt, "node.UpdatedAt": ops[0].Node.UpdatedAt,
				"edge.CreatedAt": ops[1].Edge.CreatedAt, "edge.UpdatedAt": ops[1].Edge.UpdatedAt,
			} {
				assert.Equal(t, tc.Decoded[name], got.UTC().Format(time.RFC3339Nano), "%s retains v2's decoded value", name)
			}
			var wire map[string]msgpack.RawMessage
			require.NoError(t, msgpack.Unmarshal(payload, &wire))
			wire["Version"], err = msgpack.Marshal(uint16(3))
			require.NoError(t, err)
			changed, err := msgpack.Marshal(wire)
			require.NoError(t, err)
			_, _, err = storage.DecodeEntityTransaction(changed)
			require.Error(t, err, "v2 nanoseconds must not be reinterpreted as v3 timestamps")
		})
	}
}

func TestSemanticTransactionVersion3BytesMatchCapturedVectors(t *testing.T) {
	// These bytes precede the pointer timestamp encoder: this is a byte-level
	// compatibility oracle, independent of the current writer's representation.
	raw, err := os.ReadFile("testdata/semantic_transaction_v3_timestamps.json")
	require.NoError(t, err)
	var fixture struct {
		Cases []struct {
			Input      string
			PayloadHex string `json:"payload_hex"`
		}
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Cases, 6)
	for _, tc := range fixture.Cases {
		t.Run(tc.Input, func(t *testing.T) {
			captured, err := hex.DecodeString(tc.PayloadHex)
			require.NoError(t, err)
			envelope, ops, err := storage.DecodeEntityTransaction(captured)
			require.NoError(t, err)
			require.Equal(t, uint16(3), envelope.Version)
			encoded, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{
				Namespace: envelope.Namespace, LogicalID: envelope.LogicalTxID, Operations: ops, Mode: envelope.Mode,
			})
			require.NoError(t, err)
			assert.Equal(t, captured, encoded, "allocation reduction must retain every v3 byte")
		})
	}
}
