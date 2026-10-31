package replication

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1665 implementation lane (Codex). Historical JSON belongs to the
// explicit versioned migration owner, never the current MessagePack admission.
func TestNumericWireReplicationLegacyJSONUsesMigrationOwner(t *testing.T) {
	nodeJSON := []byte(`{"id":"a","properties":{"v":9007199254740993,"d":2.0}}`)
	_, err := storage.DecodeNodePayload(nodeJSON)
	require.Error(t, err, "current replication must reject historical JSON")
	nodeWire, err := storage.TranscodeV4EntityChange(storage.EntityCreateNode, nodeJSON)
	require.NoError(t, err)
	node, err := storage.DecodeNodePayload(nodeWire)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": float64(9007199254740992), "d": float64(2)}, node.Properties)
	edgeJSON := []byte(`{"id":"r","properties":{"v":-9007199254740993,"d":1.25}}`)
	_, err = storage.DecodeEdgePayload(edgeJSON)
	require.Error(t, err, "current replication must reject historical JSON")
	edgeWire, err := storage.TranscodeV4EntityChange(storage.EntityCreateEdge, edgeJSON)
	require.NoError(t, err)
	edge, err := storage.DecodeEdgePayload(edgeWire)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": float64(-9007199254740992), "d": 1.25}, edge.Properties)
}

func TestNumericWireReplicationNumbersSurviveReplay(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(map[bool]string{true: "migrated_v4_json", false: "current_messagepack"}[legacy], func(t *testing.T) {
			properties := map[string]interface{}{
				"large_whole": float64(1 << 60), "whole": float64(2), "negative_zero": math.Copysign(0, -1),
				"integer": int64(9007199254740993), "unsigned": uint64(math.MaxUint64),
			}
			expected := map[string]interface{}{
				"large_whole": float64(1 << 60), "whole": float64(2), "negative_zero": math.Copysign(0, -1),
				"integer": int64(9007199254740993), "unsigned": uint64(math.MaxUint64),
			}
			if legacy {
				expected["integer"], expected["unsigned"] = float64(9007199254740992), float64(math.MaxUint64)
			}
			node := &storage.Node{ID: "a", Labels: []string{"NumericReplay"}, Properties: properties}
			edge := &storage.Edge{ID: "r", Type: "REPLAY", StartNode: "a", EndNode: "a", Properties: properties}
			nodeBytes, edgeBytes := []byte(nil), []byte(nil)
			if legacy {
				nodeJSON, err := json.Marshal(node) // The historical peer's writer.
				require.NoError(t, err)
				edgeJSON, err := json.Marshal(edge)
				require.NoError(t, err)
				require.Contains(t, string(nodeJSON), `"large_whole":1152921504606847000`)
				nodeBytes, err = storage.TranscodeV4EntityChange(storage.EntityCreateNode, nodeJSON)
				require.NoError(t, err)
				edgeBytes, err = storage.TranscodeV4EntityChange(storage.EntityCreateEdge, edgeJSON)
				require.NoError(t, err)
			} else {
				nodeBytes = fixtureowner.Encode(t, storage.EncodeNodePayload, node)
				edgeBytes = fixtureowner.Encode(t, storage.EncodeEdgePayload, edge)
			}
			commands := []*Command{
				{Type: CmdCreateNode, Data: nodeBytes},
				{Type: CmdCreateEdge, Data: edgeBytes},
			}
			fixture := setupTestAdapter(t)
			source, walDir := fixture.adapter, fixture.walDir
			t.Cleanup(func() { _ = source.Close() })
			// The WAL's signed integer timestamp must not pass through float64.
			committedAt := time.Unix(1700000000, 123).UTC()
			for _, command := range commands {
				command.Timestamp = committedAt
				require.NoError(t, source.ApplyCommand(command))
			}
			require.NoError(t, source.FlushWAL())
			require.NoError(t, source.Close())

			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			target, err := NewStorageAdapterWithWAL(storage.NewNamespacedEngine(base, "test"), walDir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = target.Close() })
			entries, err := target.GetWALEntries(0, 100)
			require.NoError(t, err)
			require.Len(t, entries, len(commands))
			for _, entry := range entries {
				require.Equal(t, committedAt.UnixNano(), entry.Timestamp)
				require.True(t, committedAt.Equal(entry.Command.Timestamp))
				require.NoError(t, target.ApplyCommand(entry.Command))
			}
			replayed, err := target.Engine().GetNode("a")
			require.NoError(t, err)
			require.Equal(t, expected, replayed.Properties)
			require.True(t, math.Signbit(replayed.Properties["negative_zero"].(float64)))
			replayedEdge, err := target.Engine().GetEdge("r")
			require.NoError(t, err)
			require.Equal(t, expected, replayedEdge.Properties)
			require.True(t, math.Signbit(replayedEdge.Properties["negative_zero"].(float64)))

			// Author: #1665 implementation lane (Codex). The named snapshot
			// envelope owns property tags. A structurally similar anonymous
			// record does not implement that wire contract.
			snapshot := fixtureowner.Encode[any](t, storage.EncodeCommandPayload, storage.ReplicationSnapshot{
				WALPosition: math.MaxUint64, Nodes: []*storage.Node{replayed}, Edges: []*storage.Edge{replayedEdge},
			})
			restored := setupTestAdapter(t).adapter
			t.Cleanup(func() { _ = restored.Close() })
			legacySnapshot, err := json.Marshal(storage.ReplicationSnapshot{
				WALPosition: math.MaxUint64, Nodes: []*storage.Node{node}, Edges: []*storage.Edge{edge},
			})
			require.NoError(t, err)
			require.Error(t, restored.RestoreSnapshot(bytes.NewReader(legacySnapshot)), "current snapshot admission must reject historical JSON")
			count, err := restored.Engine().NodeCount()
			require.NoError(t, err)
			require.Zero(t, count, "rejected snapshot must not publish partial entities")
			require.NoError(t, restored.RestoreSnapshot(bytes.NewReader(snapshot)))
			position, err := restored.GetWALPosition()
			require.NoError(t, err)
			require.Equal(t, uint64(math.MaxUint64), position)
			restoredNode, err := restored.Engine().GetNode("a")
			require.NoError(t, err)
			require.Equal(t, expected, restoredNode.Properties)
			require.True(t, math.Signbit(restoredNode.Properties["negative_zero"].(float64)))
			restoredEdge, err := restored.Engine().GetEdge("r")
			require.NoError(t, err)
			require.Equal(t, expected, restoredEdge.Properties)

			// Exercise the retained snapshot writer as well as historical input.
			var currentSnapshot bytes.Buffer
			require.NoError(t, target.WriteSnapshot(&currentSnapshot))
			current := setupTestAdapter(t).adapter
			require.NoError(t, current.RestoreSnapshot(&currentSnapshot))
			currentNode, err := current.Engine().GetNode("a")
			require.NoError(t, err)
			require.Equal(t, expected, currentNode.Properties)
			currentEdge, err := current.Engine().GetEdge("r")
			require.NoError(t, err)
			require.Equal(t, expected, currentEdge.Properties)
		})
	}
}

func TestNumericWireReplicationAcceptedTransactions(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, format := range []string{"typed", "legacy_json", "trailing_value"} {
			t.Run(map[bool]string{false: "direct", true: "buffered"}[buffered]+"/"+format, func(t *testing.T) {
				engine, adapter := wireFixture(t, buffered)
				properties := map[string]any{"integer": int64(9007199254740993), "unsigned": uint64(math.MaxUint64), "whole": float64(2), "negative_zero": math.Copysign(0, -1)}
				node := &storage.Node{ID: "wire:a", Labels: []string{"Numeric"}, Properties: properties}
				edge := &storage.Edge{ID: "wire:r", StartNode: node.ID, EndNode: node.ID, Type: "SELF", Properties: properties}
				operations := []storage.Operation{{Type: storage.OpCreateNode, Node: node}, {Type: storage.OpCreateEdge, Edge: edge}}
				command := wireCommand(t, "numeric-accepted", operations...)
				switch format {
				case "legacy_json":
					legacy, err := json.Marshal(node)
					command.Data = legacy
					require.NoError(t, err)
				case "trailing_value":
					command.Data = append(command.Data, command.Data...)
				}
				if format != "typed" {
					require.Error(t, adapter.ApplyCommand(command))
					nodes, err := engine.NodeCount()
					require.NoError(t, err)
					require.Zero(t, nodes, "strict envelope rejection must not publish partial entities")
					return
				}
				require.NoError(t, adapter.ApplyCommand(command))
				require.NoError(t, adapter.ApplyCommand(command), "accepted transaction replay is idempotent")
				storedNode, err := engine.GetStoredNode(node.ID)
				require.NoError(t, err)
				require.Equal(t, properties, storedNode.Properties)
				require.True(t, math.Signbit(storedNode.Properties["negative_zero"].(float64)))
				storedEdge, err := engine.GetStoredEdge(edge.ID)
				require.NoError(t, err)
				require.Equal(t, properties, storedEdge.Properties)
				nodes, err := engine.NodeCount()
				require.NoError(t, err)
				require.EqualValues(t, 1, nodes)
				edges, err := engine.EdgeCount()
				require.NoError(t, err)
				require.EqualValues(t, 1, edges)
			})
		}
	}
}
