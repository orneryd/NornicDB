package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// VERSION must use the same last-update anchor for procedure explanations
// and storage visibility, rather than explaining a recent edit as stale.
func TestKnowledgePolicyResolveRelationshipVersionUsesUpdatedTime(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run("parser="+parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			eng := newTestMemoryEngine(t).BadgerEngine
			eng.SetDecayEnabled(true)
			eng.SetRevealAll(true) // Resolve must also explain a stale relationship.
			store := storage.NewNamespacedEngine(eng, "nornic")
			schema := store.GetSchema()
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
				Name: "edge_version", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
				HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromVersion, Enabled: true, DecayEnabled: true,
			}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
				Name: "links_version", TargetEdgeType: "LINKS", IsEdge: true, ProfileRef: "edge_version",
			}))
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			now := time.Now()
			old := now.Add(-72 * time.Hour)
			edge := &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINKS", CreatedAt: old, UpdatedAt: now}
			require.NoError(t, store.CreateEdge(edge))
			for _, recent := range []bool{true, false} {
				if !recent {
					edge.UpdatedAt = old
					require.NoError(t, store.UpdateEdge(edge))
				}
				exec := NewStorageExecutor(store)
				result, err := exec.Execute(context.Background(), "CALL kp.resolve('e', '', '')", nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				row := result.Rows[0]
				require.Equal(t, "VERSION", row[3])
				require.Equal(t, !recent, row[14], "suppression eligibility follows last update")
				if recent {
					require.InDelta(t, 1.0, row[12], 0.01)
				} else {
					require.Less(t, row[12].(float64), 0.1)
				}
			}
		})
	}
}
