package storage

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceInputPreparationCancellation(t *testing.T) {
	for _, stage := range []string{"input-image", "token-append", "schema-export", "bulk-input", "bulk-token", "bulk-schema"} {
		t.Run(stage, func(t *testing.T) {
			path := t.TempDir()
			b, err := NewBadgerEngine(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			ns := NewNamespacedEngine(b, "test")
			sm := b.GetSchemaForNamespace("test")
			for i := 0; i < 20; i++ {
				require.NoError(t, sm.AddConstraint(Constraint{Name: fmt.Sprintf("other_%d", i), Type: ConstraintExists, Label: fmt.Sprintf("Other%d", i), Properties: []string{"k"}}))
			}
			node := &Node{ID: "subject", Labels: []string{"FreshSubject"}, Properties: map[string]any{}}
			for i := 0; i < 20; i++ {
				node.Properties[fmt.Sprintf("p%d", i)] = int64(i)
				if stage == "token-append" || stage == "bulk-token" {
					node.Labels = append(node.Labels, fmt.Sprintf("Fresh%d", i))
				}
			}
			ctx := preparationContext(t, "copyNodeWithoutEmbeddingsWithContext", 3)
			switch stage {
			case "input-image":
				ctx.via = ".NodeWriteInput"
			case "bulk-input":
				ctx.owner = "NamespacedEngine).BulkCreateNodesWithContext"
			case "token-append", "bulk-token":
				ctx.owner, ctx.via = "appendTokenNamesWithContext", ".RegisterTokensWithContext"
			case "schema-export", "bulk-schema":
				ctx.owner, ctx.via = "exportDefinitionLockedWithContext", ".RegisterTokensWithContext"
			}
			run := func(ctx context.Context) error { _, err := ns.CreateNodeWithContext(ctx, node); return err }
			want := 1
			if strings.HasPrefix(stage, "bulk-") {
				batch := make([]*Node, 20)
				for i := range batch {
					copy := *node
					copy.ID = NodeID(fmt.Sprintf("subject%d", i))
					batch[i] = &copy
				}
				want = len(batch)
				run = func(ctx context.Context) error { return ns.BulkCreateNodesWithContext(ctx, batch) }
			}
			err = run(ctx)
			rows, readErr := b.GetStoredNodesByLabel("FreshSubject")
			require.NoError(t, readErr)
			t.Logf("stage=%s observations=%d create=%v stored=%d", stage, ctx.hits, err, len(rows))
			require.GreaterOrEqual(t, ctx.hits, 3)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, rows)
			require.Equal(t, NodeID("subject"), node.ID)
			if stage != "input-image" && stage != "bulk-input" {
				require.NotContains(t, sm.ExportDefinition().LabelTokens, "FreshSubject")
				require.NoError(t, b.Close())
				b, err = NewBadgerEngine(path)
				require.NoError(t, err)
				ns, sm = NewNamespacedEngine(b, "test"), b.GetSchemaForNamespace("test")
				require.NotContains(t, sm.ExportDefinition().LabelTokens, "FreshSubject", "canceled token/schema preparation must not recover a name")
			}
			err = run(context.Background())
			require.NoError(t, err)
			rows, readErr = b.GetStoredNodesByLabel("FreshSubject")
			require.NoError(t, readErr)
			require.Len(t, rows, want)
		})
	}
}
