package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecayScalarRelationshipVersionAndPropertyScores(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			restoreParserTypeAfter(t)
			config.SetParserType(parser)
			engine := newTestMemoryEngine(t).BadgerEngine
			engine.SetDecayEnabled(true)
			engine.SetRevealAll(true)
			store := storage.NewNamespacedEngine(engine, "nornic")
			schema := store.GetSchema()
			for _, profile := range []knowledgepolicy.DecayProfileBundle{
				{Name: "entity_version", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromVersion, Enabled: true, DecayEnabled: true},
				{Name: "property_fast", Scope: knowledgepolicy.ScopeProperty, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromVersion, Enabled: true, DecayEnabled: true},
			} {
				require.NoError(t, schema.CreateDecayProfileBundle(profile))
			}
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", TargetEdgeType: "LINKS", IsEdge: true, ProfileRef: "entity_version", PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "summary", ProfileRef: "property_fast"}}}))
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			now := time.Now()
			old := now.Add(-72 * time.Hour)
			edge := &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINKS", Properties: map[string]any{"summary": "old text"}, CreatedAt: old, UpdatedAt: now.Add(-time.Minute)}
			require.NoError(t, store.CreateEdge(edge))
			for _, recent := range []bool{true, false} {
				if !recent {
					edge.UpdatedAt = old
					require.NoError(t, store.UpdateEdge(storage.EdgeUpdateRequest{After: edge}))
				}
				executor := NewStorageExecutor(store)
				result, err := executor.Execute(context.Background(), `MATCH ()-[r:LINKS]->() RETURN decayScore(r), decay(r), decayScore(r, {property:'summary'}), decay(r, {property:'summary'})`, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 4)
				row := result.Rows[0]
				entity, ok := row[1].(map[string]any)
				require.True(t, ok)
				if recent {
					assert.InDelta(t, 1.0, row[0], 0.03)
					assert.InDelta(t, 1.0, entity["score"], 0.03)
				} else {
					assert.Less(t, row[0].(float64), 0.1)
					assert.Less(t, entity["score"].(float64), 0.1)
				}
				assert.Equal(t, "VERSION", entity["scoreFrom"])
				assert.Less(t, row[2].(float64), 0.1, "relationship property rules must resolve by type rather than node labels")
				property, ok := row[3].(map[string]any)
				require.True(t, ok)
				assert.Less(t, property["score"].(float64), 0.1)
				assert.Equal(t, true, property["applies"])
				assert.Equal(t, "VERSION", property["scoreFrom"])
			}
			// Node scoring keeps the same shared dispatch; its property override
			// must remain separate from the entity's longer version half-life.
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "node_version", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromVersion, Enabled: true, DecayEnabled: true}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "docs", TargetLabels: []string{"Doc"}, ProfileRef: "node_version", PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "summary", ProfileRef: "property_fast"}}}))
			node, err := store.GetNode("a")
			require.NoError(t, err)
			node.Labels = []string{"Doc"}
			node.Properties = map[string]any{"summary": "old text"}
			node.CreatedAt = old
			node.UpdatedAt = now.Add(-time.Minute)
			require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: node}))
			result, err := NewStorageExecutor(store).Execute(context.Background(), `MATCH (n:Doc) RETURN decayScore(n), decay(n), decayScore(n, {property:'summary'}), decay(n, {property:'summary'})`, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			assert.InDelta(t, 1.0, result.Rows[0][0], 0.03)
			assert.InDelta(t, 1.0, result.Rows[0][1].(map[string]any)["score"], 0.03)
			assert.Less(t, result.Rows[0][2].(float64), 0.1)
			assert.Less(t, result.Rows[0][3].(map[string]any)["score"].(float64), 0.1)

		})
	}
}
