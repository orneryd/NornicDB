package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestReplicationRejectsForgedNamedValueLengthsWithoutApplying(t *testing.T) {
	for _, field := range []string{"properties", "metadata"} {
		t.Run(field, func(t *testing.T) {
			engine, adapter := wireFixture(t, false)
			node := wireNode("safe", "expected")
			change := storage.Operation{Type: storage.OpCreateNode, Node: node}
			command := wireCommand(t, "forged", change)
			var raw map[string]any
			require.NoError(t, msgpack.Unmarshal(command.Data, &raw))
			changes := raw["Changes"].([]any)
			image := changes[0].(map[string]any)["Node"].(map[string]any)
			forged := msgpack.RawMessage{0xdf, 0xff, 0xff, 0xff, 0xff}
			if field == "properties" {
				image["Properties"] = forged
			} else {
				image["EmbedMeta"] = forged
			}
			var err error
			command.Data, err = msgpack.Marshal(raw)
			require.NoError(t, err)
			require.Error(t, adapter.ApplyCommand(context.Background(), command))
			_, err = engine.GetStoredNode(node.ID)
			require.ErrorIs(t, err, storage.ErrNotFound)
			require.NoError(t, adapter.FlushWAL())
			entries, err := adapter.GetWALEntries(0, 10)
			require.NoError(t, err)
			require.Empty(t, entries, "a rejected transaction must not be published to peers")
			require.NoError(t, adapter.ApplyCommand(context.Background(), wireCommand(t, "forged", storage.Operation{Type: storage.OpCreateNode, Node: node})))
			got, err := engine.GetStoredNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, "expected", got.Properties["value"])
		})
	}
}

func TestReplicationRetainsFailedAcceptedTransactionForRecovery(t *testing.T) {
	engine, adapter := wireFixture(t, false)
	edge := wireEdge("invalid", "missing-start", "missing-end", "value")
	command := wireCommand(t, "uncommittable", storage.Operation{Type: storage.OpCreateEdge, Edge: edge})
	require.Error(t, adapter.ApplyCommand(context.Background(), command))
	_, err := engine.GetStoredEdge(edge.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "an already accepted transaction must remain available for recovery")
	require.Equal(t, command.Data, entries[0].Command.Data)
}

func TestLeaderRejectsUncommittableCommandBeforePublication(t *testing.T) {
	engine, adapter := wireFixture(t, false)
	leader := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, leader.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, leader.Shutdown()) })
	edge := wireEdge("invalid", "missing-start", "missing-end", "value")
	payload, err := storage.EncodeEdgePayload(edge)
	require.NoError(t, err)
	require.Error(t, leader.Apply(&Command{Type: CmdCreateEdge, Data: payload}, time.Second))
	_, err = engine.GetStoredEdge(edge.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Empty(t, entries, "an unaccepted command must not be sent to replicas")
}
