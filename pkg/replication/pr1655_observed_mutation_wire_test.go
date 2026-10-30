package replication

import (
	"bytes"
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This file pins the PR1655 observed-mutation transport contract:
//
//   - the blind direct update kinds keep their exact payload,
//   - the two appended request kinds carry the record {Before, After, Policy}
//     whose images use the existing node/edge codec,
//   - a request normalizes through the existing leader prepare path into the
//     existing accepted transaction encoding, and
//   - a request without an observed preimage stays a deliberate blind write.
//
// MessagePack maps the storage image carries are emitted in Go map order, so
// repeated encodes of the same image are compared by length, never by bytes.

func pr1655Node(id storage.NodeID, props map[string]any) *storage.Node {
	return &storage.Node{
		ID:         id,
		Labels:     []string{"Person"},
		Properties: props,
		CreatedAt:  time.Unix(100, 0).UTC(),
		UpdatedAt:  time.Unix(200, 0).UTC(),
	}
}

func pr1655Edge(id storage.EdgeID, start, end storage.NodeID, props map[string]any) *storage.Edge {
	return &storage.Edge{
		ID:         id,
		StartNode:  start,
		EndNode:    end,
		Type:       "KNOWS",
		Properties: props,
		CreatedAt:  time.Unix(300, 0).UTC(),
	}
}

func TestPR1655BlindDirectUpdatePayloadUnchanged(t *testing.T) {
	node := pr1655Node("test:n", map[string]any{"name": "after"})
	edge := pr1655Edge("test:e", "test:a", "test:b", map[string]any{"weight": int64(2)})

	blindUpdateNode, err := encodeEntityCommandPayload(CmdUpdateNode, node)
	require.NoError(t, err)
	blindUpdateEdge, err := encodeEntityCommandPayload(CmdUpdateEdge, edge)
	require.NoError(t, err)

	// Control: the blind update payload is exactly the node/edge image the
	// published direct kinds have always carried.
	imageNode, err := storage.EncodeNodePayload(node)
	require.NoError(t, err)
	require.True(t, bytes.Equal(imageNode, blindUpdateNode))
	imageEdge, err := storage.EncodeEdgePayload(edge)
	require.NoError(t, err)
	require.True(t, bytes.Equal(imageEdge, blindUpdateEdge))

	// A request with no observed preimage is not the blind payload: it still
	// announces its own kind so a leader can tell the two apart.
	blindNodeRequest, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, storage.NodeUpdateRequest{After: node})
	require.NoError(t, err)
	require.False(t, bytes.Equal(blindUpdateNode, blindNodeRequest))

	// Existing numeric slots are untouched; the request kinds are appended.
	require.Equal(t, CommandType(19), CmdCommitTransaction)
	require.Equal(t, CommandType(20), CmdUpdateNodeRequest)
	require.Equal(t, CommandType(21), CmdUpdateEdgeRequest)
}

func TestPR1655NodeRequestCodecRoundTrip(t *testing.T) {
	before := pr1655Node("test:n", map[string]any{"name": "before", "ids": []int64{7, 8}})
	after := pr1655Node("test:n", map[string]any{"name": "after", "ids": []int64{7, 9}})

	for _, policy := range []storage.EntityImagePolicy{storage.ClientNodeEdits, storage.StoredNodeImages} {
		request := storage.NodeUpdateRequest{Before: before, After: after, Policy: policy}
		encoded, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, request)
		require.NoError(t, err)

		again, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, request)
		require.NoError(t, err)
		require.Equal(t, len(encoded), len(again))

		ops, handled, err := decodeEntityCommandOperations(CmdUpdateNodeRequest, encoded)
		require.NoError(t, err)
		require.True(t, handled)
		require.Len(t, ops, 1)
		require.Equal(t, storage.OpUpdateNode, ops[0].Type)

		decoded := ops[0].NodeUpdate()
		require.Equal(t, policy, decoded.Policy)
		require.NotNil(t, decoded.After)
		require.Equal(t, after.ID, decoded.After.ID)
		require.Equal(t, after.Labels, decoded.After.Labels)
		require.Equal(t, "after", decoded.After.Properties["name"])
		require.Equal(t, []int64{7, 9}, decoded.After.Properties["ids"])
		require.Equal(t, after.CreatedAt, decoded.After.CreatedAt)

		require.NotNil(t, decoded.Before, "the observed preimage must survive transport")
		require.Equal(t, before.ID, decoded.Before.ID)
		require.Equal(t, before.Labels, decoded.Before.Labels)
		require.Equal(t, "before", decoded.Before.Properties["name"])
		require.Equal(t, []int64{7, 8}, decoded.Before.Properties["ids"])
		require.Equal(t, before.CreatedAt, decoded.Before.CreatedAt)
		require.Equal(t, before.UpdatedAt, decoded.Before.UpdatedAt)
	}
}

func TestPR1655NodeRequestWithoutBeforeKeepsBlindWrite(t *testing.T) {
	after := pr1655Node("test:n", map[string]any{"name": "after"})
	encoded, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, storage.NodeUpdateRequest{After: after})
	require.NoError(t, err)

	ops, handled, err := decodeEntityCommandOperations(CmdUpdateNodeRequest, encoded)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, ops, 1)
	require.Nil(t, ops[0].NodeUpdate().Before, "an omitted preimage stays a deliberate blind image write")
	require.Equal(t, storage.ClientNodeEdits, ops[0].NodeUpdate().Policy)
}

func TestPR1655EdgeRequestCodecRoundTrip(t *testing.T) {
	before := pr1655Edge("test:e", "test:a", "test:b", map[string]any{"weight": int64(1), "since": "2019"})
	after := pr1655Edge("test:e", "test:a", "test:b", map[string]any{"weight": int64(2), "since": "2019"})

	request := storage.EdgeUpdateRequest{Before: before, After: after}
	encoded, err := encodeEntityCommandPayload(CmdUpdateEdgeRequest, request)
	require.NoError(t, err)
	again, err := encodeEntityCommandPayload(CmdUpdateEdgeRequest, request)
	require.NoError(t, err)
	require.Equal(t, len(encoded), len(again))

	ops, handled, err := decodeEntityCommandOperations(CmdUpdateEdgeRequest, encoded)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, ops, 1)
	require.Equal(t, storage.OpUpdateEdge, ops[0].Type)

	decoded := ops[0].EdgeUpdate()
	require.NotNil(t, decoded.After)
	require.Equal(t, after.ID, decoded.After.ID)
	require.Equal(t, after.StartNode, decoded.After.StartNode)
	require.Equal(t, after.EndNode, decoded.After.EndNode)
	require.Equal(t, after.Type, decoded.After.Type)
	require.Equal(t, int64(2), decoded.After.Properties["weight"])

	require.NotNil(t, decoded.Before, "the observed preimage must survive transport")
	require.Equal(t, before.ID, decoded.Before.ID)
	require.Equal(t, before.StartNode, decoded.Before.StartNode)
	require.Equal(t, before.EndNode, decoded.Before.EndNode)
	require.Equal(t, before.Type, decoded.Before.Type)
	require.Equal(t, int64(1), decoded.Before.Properties["weight"])
	require.Equal(t, "2019", decoded.Before.Properties["since"])

	blind, err := encodeEntityCommandPayload(CmdUpdateEdgeRequest, storage.EdgeUpdateRequest{After: after})
	require.NoError(t, err)
	blindOps, _, err := decodeEntityCommandOperations(CmdUpdateEdgeRequest, blind)
	require.NoError(t, err)
	require.Len(t, blindOps, 1)
	require.Nil(t, blindOps[0].EdgeUpdate().Before)
}

func TestPR1655InvalidRequestRejected(t *testing.T) {
	after := pr1655Node("test:n", map[string]any{"name": "after"})
	afterBytes, err := storage.EncodeNodePayload(after)
	require.NoError(t, err)

	t.Run("node After required", func(t *testing.T) {
		_, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, storage.NodeUpdateRequest{Before: after})
		require.ErrorIs(t, err, storage.ErrInvalidData)
	})
	t.Run("edge After required", func(t *testing.T) {
		_, err := encodeEntityCommandPayload(CmdUpdateEdgeRequest, storage.EdgeUpdateRequest{Before: pr1655Edge("test:e", "test:a", "test:b", nil)})
		require.ErrorIs(t, err, storage.ErrInvalidData)
	})
	t.Run("unknown policy rejected at encode", func(t *testing.T) {
		_, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, storage.NodeUpdateRequest{After: after, Policy: storage.EntityImagePolicy(9)})
		require.ErrorIs(t, err, storage.ErrInvalidData)
	})
	t.Run("unknown policy rejected at decode", func(t *testing.T) {
		payload, err := storage.EncodeCommandPayload(nodeRequestPayload{After: afterBytes, Policy: 9})
		require.NoError(t, err)
		ops, handled, err := decodeEntityCommandOperations(CmdUpdateNodeRequest, payload)
		require.True(t, handled)
		require.ErrorIs(t, err, storage.ErrInvalidData)
		require.Nil(t, ops)
	})
	t.Run("absent node After rejected at decode", func(t *testing.T) {
		payload, err := storage.EncodeCommandPayload(nodeRequestPayload{})
		require.NoError(t, err)
		ops, handled, err := decodeEntityCommandOperations(CmdUpdateNodeRequest, payload)
		require.True(t, handled)
		require.ErrorIs(t, err, storage.ErrInvalidData)
		require.Nil(t, ops)
	})
	t.Run("absent edge After rejected at decode", func(t *testing.T) {
		payload, err := storage.EncodeCommandPayload(edgeRequestPayload{})
		require.NoError(t, err)
		ops, handled, err := decodeEntityCommandOperations(CmdUpdateEdgeRequest, payload)
		require.True(t, handled)
		require.ErrorIs(t, err, storage.ErrInvalidData)
		require.Nil(t, ops)
	})
	t.Run("unreadable Before rejected", func(t *testing.T) {
		payload, err := storage.EncodeCommandPayload(nodeRequestPayload{Before: []byte{0xc1}, After: afterBytes})
		require.NoError(t, err)
		ops, handled, err := decodeEntityCommandOperations(CmdUpdateNodeRequest, payload)
		require.True(t, handled)
		require.Error(t, err)
		require.Nil(t, ops)
	})
}

// TestPR1655LeaderNormalizesRequest proves the request kind reaches the
// existing accepted-transaction prepare path at its leader.
func TestPR1655LeaderNormalizesRequest(t *testing.T) {
	backing := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	engine := storage.NewNamespacedEngine(backing, "test")
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })

	ctx := context.Background()
	require.NoError(t, adapter.ApplyCommand(&Command{
		Type:      CmdCreateNode,
		Data:      fixtureowner.Encode(t, storage.EncodeNodePayload, pr1655Node("test:n", map[string]any{"name": "stored"})),
		Timestamp: time.Now(),
	}))
	_, err = backing.GetNode("test:n")
	require.NoError(t, err)

	request := storage.NodeUpdateRequest{
		Before: pr1655Node("test:n", map[string]any{"name": "stored"}),
		After:  pr1655Node("test:n", map[string]any{"name": "updated"}),
	}
	data, err := encodeEntityCommandPayload(CmdUpdateNodeRequest, request)
	require.NoError(t, err)

	normalized, settle, handled, err := adapter.PrepareEntityCommand(ctx, &Command{Type: CmdUpdateNodeRequest, Data: data, Timestamp: time.Now()})
	require.NoError(t, err)
	require.True(t, handled)
	require.NotNil(t, normalized)
	require.Equal(t, CmdCommitTransaction, normalized.Type, "the leader admits the request as an accepted transaction")
	require.NotEmpty(t, normalized.RequestID)

	// Abandon the prepared frame: nothing may have been published.
	normalized.accepted.prepared.AbortNative()
	if settle != nil {
		settle(storage.CommitOutcome{State: storage.CommitDiscarded})
	}
	stored, err := backing.GetNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "stored", stored.Properties["name"], "preparing admission must not publish the mutation")
}
