package cypher

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Select the supported point-engine protocol. Optional native transaction
// methods are intentionally absent so CREATE consumes its contextual entrance.
type namespaceInputPointEngine struct {
	storage.Engine
	namespace *storage.NamespacedEngine
	bulkCalls int
}

func (*namespaceInputPointEngine) Namespace() string { return "test" }
func (e *namespaceInputPointEngine) CreateNodeWithContext(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return e.namespace.CreateNodeWithContext(ctx, node)
}

func (e *namespaceInputPointEngine) BulkCreateNodesWithContext(ctx context.Context, nodes []*storage.Node) error {
	e.bulkCalls++
	return e.namespace.BulkCreateNodesWithContext(ctx, nodes)
}

func TestNamespaceInputStatementPreparationCancellation(t *testing.T) {
	for _, stage := range []string{"input-image", "token-append", "schema-export", "bulk-input", "bulk-token", "bulk-schema"} {
		t.Run(stage, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			ns := storage.NewNamespacedEngine(b, "test")
			sm := b.GetSchemaForNamespace("test")
			for i := 0; i < 20; i++ {
				require.NoError(t, sm.AddConstraint(storage.Constraint{Name: fmt.Sprintf("other_%d", i), Type: storage.ConstraintExists, Label: fmt.Sprintf("Other%d", i), Properties: []string{"k"}}))
			}
			adapter := &namespaceInputPointEngine{Engine: ns, namespace: ns}
			e := NewStorageExecutor(adapter)
			properties := map[string]any{}
			for i := 0; i < 20; i++ {
				properties[fmt.Sprintf("p%d", i)] = int64(i)
			}
			query := "CREATE (n:FreshSubject $properties) RETURN n"
			if stage == "token-append" || stage == "bulk-token" {
				labels := ":FreshSubject"
				for i := 0; i < 20; i++ {
					labels += fmt.Sprintf(":Fresh%d", i)
				}
				query = "CREATE (n" + labels + " $properties) RETURN n"
			}
			want := 1
			if strings.HasPrefix(stage, "bulk-") {
				pattern := strings.TrimSuffix(strings.TrimPrefix(query, "CREATE "), " RETURN n")
				patterns := make([]string, 20)
				for i := range patterns {
					patterns[i] = strings.Replace(pattern, "(n:", fmt.Sprintf("(n%d:", i), 1)
				}
				query = "CREATE " + strings.Join(patterns, ",")
				want = 20
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: "copyNodeWithoutEmbeddingsWithContext", via: ".NodeWriteInput", after: 3, cancel: cancel}
			switch stage {
			case "bulk-input":
				ctx.owner, ctx.via = "NamespacedEngine).BulkCreateNodesWithContext", ""
			case "token-append", "bulk-token":
				ctx.owner, ctx.via = "appendTokenNamesWithContext", ".RegisterTokensWithContext"
			case "schema-export", "bulk-schema":
				ctx.owner, ctx.via = "exportDefinitionLockedWithContext", ".RegisterTokensWithContext"
			}
			_, err = e.Execute(ctx, query, map[string]any{"properties": properties})
			if strings.HasPrefix(stage, "bulk-") {
				require.Positive(t, adapter.bulkCalls, "the public clause must reach the bulk input owner")
			}
			rows, readErr := b.GetStoredNodesByLabel("FreshSubject")
			require.NoError(t, readErr)
			t.Logf("stage=%s observations=%d execute=%v stored=%d", stage, ctx.hits, err, len(rows))
			require.GreaterOrEqual(t, ctx.hits, 3)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, rows)
			if stage != "input-image" && stage != "bulk-input" {
				require.NotContains(t, sm.ExportDefinition().LabelTokens, "FreshSubject")
			}
			_, err = e.Execute(context.Background(), query, map[string]any{"properties": properties})
			require.NoError(t, err)
			rows, readErr = b.GetStoredNodesByLabel("FreshSubject")
			require.NoError(t, readErr)
			require.Len(t, rows, want)
		})
	}
}
