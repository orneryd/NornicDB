package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipCacheRawInvalidHeaderIsRefusedBeforeDurability(t *testing.T) {
	for _, data := range [][]byte{nil, {1}} {
		t.Run(map[bool]string{true: "empty", false: "truncated"}[len(data) == 0], func(t *testing.T) {
			b := createTestBadgerEngine(t)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			original := &Edge{ID: "test:raw-refused", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"v": int64(1)}}
			require.NoError(t, b.CreateEdge(original))
			_, err := b.GetOutgoingEdges(original.StartNode)
			require.NoError(t, err)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			tx.bufferSet(edgeKey(original.ID), data)
			err = tx.Commit()
			if len(data) == 0 {
				require.ErrorIs(t, err, ErrInvalidData)
			} else {
				require.ErrorContains(t, err, "compact edge truncated")
			}
			got, err := b.GetEdge(original.ID)
			require.NoError(t, err)
			require.Equal(t, int64(1), got.Properties["v"])
			next := copyEdge(original)
			next.Properties["v"] = int64(2)
			require.NoError(t, b.UpdateEdge(next), "refused writer releases its transaction and publication resources")
			rows, err := b.GetOutgoingEdges(original.StartNode)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, int64(2), rows[0].Properties["v"])
		})
	}
}
