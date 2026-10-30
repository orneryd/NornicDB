package storage

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaFilterHandoffCancellation(t *testing.T) {
	for _, path := range []string{"bulk-candidates", "point-types", "pending-edge-keys"} {
		t.Run(path, func(t *testing.T) {
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			sm := b.GetSchemaForNamespace("test")
			for i := 0; i < 20; i++ {
				label := fmt.Sprintf("Other%d", i)
				require.NoError(t, sm.AddConstraint(Constraint{Name: "exists_" + label, Type: ConstraintExists, Label: label, Properties: []string{"k"}}))
				require.NoError(t, sm.AddPropertyTypeConstraint("type_"+label, label, "k", PropertyTypeInteger))
			}
			ctx := preparationContext(t, "GetConstraintsForLabelsWithContext", 3)
			var run func(context.Context) error
			wantNodes, wantEdges := 1, 0
			switch path {
			case "bulk-candidates":
				ctx.via = ".validateBulkNodeConstraintsWithContext"
				run = func(ctx context.Context) error {
					return b.BulkCreateNodesWithContext(ctx, []*Node{{ID: "test:subject", Labels: []string{"P"}, Properties: map[string]any{"k": int64(1)}}})
				}
			case "point-types":
				ctx.owner, ctx.via = "GetPropertyTypeConstraintsForLabelsWithContext", ".validateNodeConstraintsInTxn"
				run = func(ctx context.Context) error {
					_, err := b.CreateNodeWithContext(ctx, &Node{ID: "test:subject", Labels: []string{"P"}, Properties: map[string]any{"k": int64(1)}})
					return err
				}
			case "pending-edge-keys":
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
					require.NoError(t, err)
				}
				ctx.via = ".appendConstraintValueKeysWithContext"
				wantNodes, wantEdges = 2, 1
				run = func(ctx context.Context) error {
					tx, err := b.BeginTransaction()
					if err != nil {
						return err
					}
					defer tx.Rollback()
					if err := tx.CreateEdge(&Edge{ID: "test:subject", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"k": int64(1)}}); err != nil {
						return err
					}
					return tx.CommitContext(ctx)
				}
			}
			err = run(ctx)
			nodes, readErr := b.AllNodes()
			require.NoError(t, readErr)
			edges, readErr := b.AllEdges()
			require.NoError(t, readErr)
			t.Logf("path=%s observations=%d error=%v stored nodes=%d edges=%d", path, ctx.hits, err, len(nodes), len(edges))
			require.GreaterOrEqual(t, ctx.hits, 3)
			require.ErrorIs(t, err, context.Canceled)
			if path == "pending-edge-keys" {
				require.Len(t, nodes, 2)
			} else {
				require.Empty(t, nodes)
			}
			require.Empty(t, edges)
			require.NoError(t, run(context.Background()))
			nodes, readErr = b.AllNodes()
			require.NoError(t, readErr)
			require.Len(t, nodes, wantNodes)
			edges, readErr = b.AllEdges()
			require.NoError(t, readErr)
			require.Len(t, edges, wantEdges)
		})
	}
}
