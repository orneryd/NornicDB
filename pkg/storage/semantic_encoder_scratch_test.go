package storage

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are lease-component controls. The existing public owned-byte and native
// interleaved-encoder tests cover their callers; no scratch escapes the encoder.
func TestSemanticEncoderScratchReusesAndClearsWireImages(t *testing.T) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	request := EntityTransactionRequest{Namespace: "test", LogicalID: "first", Mode: CommitModeDirect, Operations: []Operation{
		{Type: OpCreateNode, Node: &Node{ID: "test:one", Labels: []string{"First"}, Properties: map[string]any{"v": int64(7)}}},
		{Type: OpCreateEdge, Edge: &Edge{ID: "test:edge", StartNode: "test:one", EndNode: "test:one", Type: "R", Properties: map[string]any{"v": int64(8)}}},
	}}
	first, err := encoder.entityTransaction(request)
	require.NoError(t, err)
	owned := bytes.Clone(first)
	nodeAddress, edgeAddress, changeAddress := &encoder.semantic.nodes[0], &encoder.semantic.edges[0], &encoder.semantic.changes[0]
	encoder.semantic.release()
	requireClearedSemanticScratch(t, &encoder.semantic)
	_, err = encoder.entityTransaction(request)
	require.NoError(t, err)
	require.Same(t, nodeAddress, &encoder.semantic.nodes[0])
	require.Same(t, edgeAddress, &encoder.semantic.edges[0])
	require.Same(t, changeAddress, &encoder.semantic.changes[0])
	next, err := encoder.entityTransaction(EntityTransactionRequest{Namespace: "other", LogicalID: "second", Mode: CommitModeDirect, Operations: []Operation{{Type: OpDeleteNode, NodeID: "other:deleted"}}})
	require.NoError(t, err)
	envelope, ops, err := DecodeEntityTransaction(next)
	require.NoError(t, err)
	require.Equal(t, "second", envelope.LogicalTxID)
	require.Len(t, ops, 1)
	require.Equal(t, OpDeleteNode, ops[0].Type)
	original, ops, err := DecodeEntityTransaction(owned)
	require.NoError(t, err)
	require.Equal(t, "first", original.LogicalTxID)
	require.Len(t, ops, 2)
	encoder.semantic.release()
	requireClearedSemanticScratch(t, &encoder.semantic)
}

func requireClearedSemanticScratch(t *testing.T, s *semanticTransactionScratch) {
	t.Helper()
	require.Empty(t, s.nodes)
	require.Empty(t, s.edges)
	require.Empty(t, s.changes)
	require.Equal(t, semanticTransactionWire[semanticTransactionTimestamp]{}, s.wire)
	for _, n := range s.nodes[:cap(s.nodes)] {
		require.Equal(t, semanticTransactionNode[semanticTransactionTimestamp]{}, n)
	}
	for _, e := range s.edges[:cap(s.edges)] {
		require.Equal(t, semanticTransactionEdge[semanticTransactionTimestamp]{}, e)
	}
	for _, c := range s.changes[:cap(s.changes)] {
		require.Equal(t, semanticTransactionChange[semanticTransactionTimestamp]{}, c)
	}
}

func TestSemanticEncoderScratchDropsOversizedArrays(t *testing.T) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	operations := make([]Operation, maxRetainedSemanticChanges+1)
	for i := range operations {
		operations[i] = Operation{Type: OpCreateNode, Node: &Node{ID: NodeID("test:n" + strconv.Itoa(i))}}
	}
	_, err := encoder.entityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "large", Mode: CommitModeDirect, Operations: operations})
	require.NoError(t, err)
	require.Greater(t, cap(encoder.semantic.nodes), maxRetainedSemanticChanges)
	encoder.semantic.release()
	requireClearedSemanticScratch(t, &encoder.semantic)
	require.Zero(t, cap(encoder.semantic.nodes))
	require.Zero(t, cap(encoder.semantic.changes))
}

func TestSemanticEncoderScratchErrorAndHealthyReuse(t *testing.T) {
	encoder := getValueEncoder()
	defer putValueEncoder(encoder)
	_, err := encoder.entityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "rejected", Mode: CommitModeDirect, Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:bad", Properties: map[string]any{"bad": func() {}}}}}})
	require.Error(t, err)
	encoder.semantic.release()
	requireClearedSemanticScratch(t, &encoder.semantic)
	payload, err := encoder.entityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "healthy", Mode: CommitModeDirect, Operations: []Operation{{Type: OpDeleteNode, NodeID: "test:healthy"}}})
	require.NoError(t, err)
	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Equal(t, "healthy", envelope.LogicalTxID)
	require.Len(t, ops, 1)
}
