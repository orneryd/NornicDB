package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionCurrentAdjacencyOwnsReturnedRelationships(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind-%v", buffered), func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: !buffered, manualDrain: buffered})
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:link", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]interface{}{"state": "original", "nested": []interface{}{map[string]interface{}{"key": "original"}}}}))
			// Warm the actual adjacency and body caches before opening the reader.
			for range 2 {
				_, err := engine.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Node: "test:a"})
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			rows, err := tx.ReadEdges(t.Context(), AdjacentEdgeRead{Node: "test:a", ReadPolicy: CurrentEdgeMatch})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			rows[0].Type = "CALLER_EDIT"
			rows[0].Properties["state"] = "caller-edit"
			rows[0].Properties["nested"].([]interface{})[0].(map[string]interface{})["key"] = "caller-edit"
			require.NoError(t, tx.Rollback())
			retainedRows, err := engine.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Node: "test:a"})
			require.NoError(t, err)
			require.Len(t, retainedRows, 1)
			retained := retainedRows[0]
			require.Equal(t, "LINK", retained.Type, "editing a transaction result must not mutate the retained cache")
			require.Equal(t, "original", retained.Properties["state"])
			require.Equal(t, "original", retained.Properties["nested"].([]interface{})[0].(map[string]interface{})["key"])
		})
	}
}
