package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPR1655SemanticNodeTimeRoundTrip(t *testing.T) {
	for _, stamp := range []time.Time{{}, time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)} {
		for _, kind := range []OperationType{OpCreateNode, OpUpdateNode} {
			t.Run(string(kind)+"/"+stamp.String(), func(t *testing.T) {
				node := &Node{ID: "test:n", CreatedAt: stamp, UpdatedAt: stamp}
				data, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "fixture", Mode: CommitModeExplicit, Operations: []Operation{{Type: kind, Node: node}}})
				require.NoError(t, err)
				_, ops, err := DecodeEntityTransaction(data)
				require.NoError(t, err)
				require.Len(t, ops, 1)
				require.True(t, stamp.Equal(ops[0].Node.CreatedAt), "created: %v", ops[0].Node.CreatedAt)
				require.True(t, stamp.Equal(ops[0].Node.UpdatedAt), "updated: %v", ops[0].Node.UpdatedAt)
			})
		}
	}
}
