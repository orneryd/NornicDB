package storage

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"testing"
)

func TestSemanticEncoderPoolPreservesRawFormatAndOwnedBytes(t *testing.T) {
	request := EntityTransactionRequest{Namespace: "test", LogicalID: "stable-encoding", Mode: CommitModeDirect, Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:one", Properties: map[string]any{"v": int64(7)}}}}}
	actual, err := EncodeEntityTransaction(request)
	require.NoError(t, err)
	owned := bytes.Clone(actual)
	var wire semanticTransactionWire
	require.NoError(t, msgpack.Unmarshal(actual, &wire))
	reference, err := msgpack.Marshal(wire)
	require.NoError(t, err)
	require.Equal(t, reference, actual, "one-entry maps make this a deterministic raw MessagePack byte control")
	for range 32 {
		_, err := encodeValue(map[string]any{"after": "retained pooled header"})
		require.NoError(t, err)
		_, err = EncodeEntityTransaction(EntityTransactionRequest{Namespace: "other", LogicalID: "next", Mode: CommitModeDirect, Operations: []Operation{{Type: OpDeleteNode, NodeID: "other:deleted"}}})
		require.NoError(t, err)
	}
	require.Equal(t, owned, actual, "returning a pooled encoder cannot mutate an owned semantic record")
	envelope, ops, err := DecodeEntityTransaction(actual)
	require.NoError(t, err)
	require.Equal(t, "test", envelope.Namespace)
	require.EqualValues(t, 7, ops[0].Node.Properties["v"])
}
