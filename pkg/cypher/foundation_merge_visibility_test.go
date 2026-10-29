// Author: Lane #1644 (Codex). Admission uses the visible view; physical
// uniqueness rejects a conflicting create without disclosing its hidden holder.
package cypher_test

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicMergeVisibility(t *testing.T) {
	for _, key := range []struct{ name, requirement, pattern string }{
		{"scalar", "n.k IS UNIQUE", "k: 'caller-key'"},
		{"tuple-unique", "(n.k, n.part) IS UNIQUE", "k: 'caller-key', part: 'caller-part'"},
		{"node-key", "(n.k, n.part) IS NODE KEY", "k: 'caller-key', part: 'caller-part'"},
	} {
		for _, explicit := range []bool{false, true} {
			name := key.name + "/implicit"
			if explicit {
				name = key.name + "/explicit"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				base := storage.NewMemoryEngine()
				defer base.Close()
				tenant := storage.NewNamespacedEngine(base, "tenant")
				executor := cypher.NewStorageExecutor(tenant)
				_, err := executor.Execute(ctx, "CREATE CONSTRAINT holder_key FOR (n:Doc) REQUIRE "+key.requirement, nil)
				require.NoError(t, err)
				_, err = tenant.CreateNode(&storage.Node{ID: "private-holder-identity", Labels: []string{"Doc"}, Properties: map[string]any{"k": "caller-key", "part": "caller-part", "private": "private-holder-property"}})
				require.NoError(t, err)
				require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, "tenant:private-holder-identity", true))
				before, err := base.GetStoredNode("tenant:private-holder-identity")
				require.NoError(t, err)
				visible, err := executor.Execute(ctx, "MATCH (n:Doc) RETURN n", nil)
				require.NoError(t, err)
				require.Empty(t, visible.Rows)
				if explicit {
					_, err = executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, mergeErr := executor.Execute(ctx, "MERGE (n:Doc {"+key.pattern+"}) ON MATCH SET n.private = 'changed' RETURN n", nil)
				if result != nil {
					for _, row := range result.Rows {
						for _, value := range row {
							if node, ok := value.(*storage.Node); ok {
								require.NotEqual(t, "private-holder-identity", string(node.ID))
								require.NotContains(t, node.Properties, "private")
							}
						}
					}
				}
				if explicit {
					if mergeErr == nil {
						_, mergeErr = executor.Execute(ctx, "COMMIT", nil)
					}
					_, _ = executor.Execute(ctx, "ROLLBACK", nil)
				}
				require.Error(t, mergeErr)
				var violation *storage.ConstraintViolationError
				require.ErrorAs(t, mergeErr, &violation)
				require.Contains(t, mergeErr.Error(), "already exists")
				for cause := mergeErr; cause != nil; cause = errors.Unwrap(cause) {
					require.NotContains(t, cause.Error(), "private-holder-identity")
					require.NotContains(t, cause.Error(), "private-holder-property")
				}
				after, err := base.GetStoredNode("tenant:private-holder-identity")
				require.NoError(t, err)
				require.Equal(t, before, after)
				stored, err := tenant.GetStoredNodesByLabel("Doc")
				require.NoError(t, err)
				require.Len(t, stored, 1)
			})
		}
	}
}
