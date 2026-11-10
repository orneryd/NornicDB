package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBodyWriterAuthorityAllowsLiveMetadataAndIndependentEndpoints(t *testing.T) {
	for _, kind := range []StoredEntityKind{StoredEntityNodeKind, StoredEntityEdgeKind} {
		name := "node"
		if kind == StoredEntityEdgeKind {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			id := "test:a"
			if kind == StoredEntityNodeKind {
				_, err = tx.LockNodeForWrite(t.Context(), NodeID(id))
			} else {
				id = "test:r"
				_, err = tx.LockEdgeForWrite(t.Context(), EdgeID(id))
			}
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			require.NoError(t, engine.SetManualSuppression(ctx, kind, id, true), "live metadata must not queue behind an unchanged body")
			require.NoError(t, engine.SetManualSuppression(ctx, kind, id, false))
			require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:independent", StartNode: "test:a", EndNode: "test:b", Type: "OTHER"}), "an endpoint is not a body mutation")
			// The separate body key still excludes every actual body writer.
			bounded, stop := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer stop()
			if kind == StoredEntityNodeKind {
				node, err := engine.GetStoredNode(t.Context(), NodeID(id))
				require.NoError(t, err)
				node.Properties = map[string]interface{}{"peer": int64(1)}
				err = engine.UpdateNode(bounded, NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				edge, err := engine.GetStoredEdge(t.Context(), EdgeID(id))
				require.NoError(t, err)
				edge.Properties = map[string]interface{}{"peer": int64(1)}
				err = engine.UpdateEdge(bounded, EdgeUpdateRequest{After: edge})
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
		})
	}
}
