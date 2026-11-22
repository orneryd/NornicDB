package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestL9MigrationBridgeVisibilityOwner(t *testing.T) {
	ctx := t.Context()
	for _, label := range []string{"_Migrated", "MigrationBridge"} {
		t.Run(label, func(t *testing.T) {
			exec, store := nor170Executor(t, config.GetParserType())
			substitute := func(statement string) string { return strings.ReplaceAll(statement, "_Migrated", label) }
			_, err := exec.Execute(ctx, substitute(nor170NodeUpsertCypher), map[string]any{"rows": nor170NodeRows()})
			require.NoError(t, err)
			require.Len(t, nor170RawNodes(t, ctx, store, label), 2)
			_, err = exec.Execute(ctx, substitute(nor170EdgeCreateCypher), map[string]any{"rows": nor170EdgeRows()})
			require.NoError(t, err)
			edges, err := store.AllEdges(ctx)
			require.NoError(t, err)
			promotion, err := exec.Execute(ctx, substitute(nor170Promotion("Person")), nil)
			require.NoError(t, err)
			t.Logf("%s physical edges=%d promoted labels=%d", label, len(edges), promotion.Stats.LabelsAdded)
			if strings.HasPrefix(label, "_") {
				require.Empty(t, edges)
				require.Zero(t, promotion.Stats.LabelsAdded)
			} else {
				require.Len(t, edges, 1)
				require.Equal(t, 2, promotion.Stats.LabelsAdded)
			}
		})
	}
}
