package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMutationPreimageFoldStopsAfterInFlightCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	node := &Node{ID: "test:observed"}
	operations := make([]Operation, 2*bulkReadCancellationInterval)
	for i := range operations {
		operations[i] = Operation{Type: OpUpdateNode, NodeID: node.ID, OldNode: node}
	}
	calls := 0
	var canceledAt time.Time
	before, distinct, err := mutationPreimagesLocked(ctx, operations, 1, OpUpdateNode, func(op *Operation) (NodeID, *Node) {
		calls++
		if calls == 1 {
			canceledAt = time.Now()
			cancel()
		}
		return nodeClaimPreimage(op)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, before, "a canceled fold cannot publish its partial map")
	require.False(t, distinct)
	require.Greater(t, calls, 0, "cancel after real fold work starts")
	require.LessOrEqual(t, calls, bulkReadCancellationInterval)
	require.Less(t, time.Since(canceledAt), 100*time.Millisecond)
}

func TestSingleMutationPreimageHonorsCanceledAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node := &Node{ID: "test:single"}
	before, _, err := mutationPreimagesLocked(ctx, []Operation{{Type: OpUpdateNode, NodeID: node.ID, OldNode: node}}, 1, OpUpdateNode, nodeClaimPreimage)
	require.Nil(t, before)
	require.ErrorIs(t, err, context.Canceled)
}
