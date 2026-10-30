package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR1655ObservationIsNotPersisted(t *testing.T) {
	actual := &Node{ID: "test:a", Properties: map[string]any{"value": "stored predecessor"}}
	after := &Node{ID: "test:a", Properties: map[string]any{"value": "accepted"}}
	operation := Operation{Type: OpUpdateNode, NodeID: after.ID, Node: after, OldNode: actual}
	runtimeOperation := operation
	runtimeOperation.nodeImagePolicy = StoredNodeImages
	// A non-serializable marker makes accidental traversal of private runtime
	// observation data fail instead of merely producing different valid bytes.
	runtimeOperation.nodeObservedBefore = &Node{ID: "a", Properties: map[string]any{"runtime only": func() {}}}
	baseline, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "fixture", Mode: CommitModeDirect, Operations: []Operation{operation}})
	require.NoError(t, err)
	observed, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "fixture", Mode: CommitModeDirect, Operations: []Operation{runtimeOperation}})
	require.NoError(t, err)
	require.Equal(t, baseline, observed, "runtime observation and input policy must not alter accepted semantic bytes")
	plain := WALBufferedCommitData{TxID: "fixture", Namespace: "test", Ops: []Operation{operation}}
	baseline, err = marshalWALRecoveryPayload(nil, plain)
	require.NoError(t, err)
	plain.Ops = []Operation{runtimeOperation}
	observed, err = marshalWALRecoveryPayload(nil, plain)
	require.NoError(t, err)
	require.Equal(t, baseline, observed, "runtime fields must not alter buffered recovery bytes")
	var decoded WALBufferedCommitData
	require.NoError(t, decodeRecoveryPayload(observed, &decoded))
	require.Len(t, decoded.Ops, 1)
	require.Nil(t, decoded.Ops[0].nodeObservedBefore)
	require.Equal(t, ClientNodeEdits, decoded.Ops[0].nodeImagePolicy)
	require.Equal(t, "stored predecessor", decoded.Ops[0].OldNode.Properties["value"], "undo/accounting keep the actual stored predecessor")
}
