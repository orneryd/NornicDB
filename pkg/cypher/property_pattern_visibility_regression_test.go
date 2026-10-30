package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type propertyViewCountingEngine struct {
	storage.Engine
	discoveries atomic.Int64
}

func (e *propertyViewCountingEngine) GetInnerEngine() storage.Engine {
	e.discoveries.Add(1)
	return e.Engine
}

func TestDisabledPropertyViewDoesNotRediscoverEnginePerValue(t *testing.T) {
	engine := &propertyViewCountingEngine{Engine: newTestMemoryEngine(t)}
	exec := NewStorageExecutor(engine)
	node := &storage.Node{Properties: map[string]interface{}{"body": "raw"}}
	engine.discoveries.Store(0)
	for i := 0; i < 1000; i++ {
		require.Same(t, node, exec.queryVisibleValue(node))
	}
	require.Zero(t, engine.discoveries.Load(), "disabled reads must not walk storage decorators per value")
}

func TestInlinePatternsUseSuppressedPropertyView(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = be.Close() })
			schema := be.GetSchemaForNamespace("view")
			for _, edge := range []bool{false, true} {
				name, scope, target, property := "doc", knowledgepolicy.ScopeNode, "Doc", "body"
				if edge {
					name, scope, target, property = "link", knowledgepolicy.ScopeEdge, "LINK", "summary"
				}
				require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: property, HalfLifeSeconds: 1}}}
				if edge {
					binding.IsEdge = true
					binding.TargetEdgeType = target
				} else {
					binding.TargetLabels = []string{target}
				}
				require.NoError(t, schema.CreateDecayProfileBinding(binding))
			}
			be.SetDecayEnabled(true)
			old := time.Now().Add(-time.Hour)
			for _, id := range []storage.NodeID{"view:a", "view:b"} {
				_, err = be.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"body": "hidden", "title": string(id)}, CreatedAt: old, UpdatedAt: old})
				require.NoError(t, err)
			}
			require.NoError(t, be.CreateEdge(&storage.Edge{ID: "view:e", StartNode: "view:a", EndNode: "view:b", Type: "LINK", Properties: map[string]interface{}{"summary": "hidden"}, CreatedAt: old, UpdatedAt: old}))
			exec := NewStorageExecutor(storage.NewNamespacedEngine(be, "view"))
			ctx := context.Background()
			_, err = exec.Execute(ctx, "CREATE INDEX view_body FOR (d:Doc) ON (d.body)", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, query := range []string{
				"MATCH (d:Doc {body:'hidden'}) RETURN count(d)",
				"MATCH (d {body:'hidden'}) RETURN count(d)",
				"MATCH (:Doc)-[r:LINK {summary:'hidden'}]->() RETURN count(r)",
				"MATCH (a:Doc {title:'view:a'}) MATCH (a)-[:LINK]->(d:Doc {body:'hidden'}) RETURN count(d)",
			} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, query)
			}
			indexed, err := exec.Execute(ctx, "MATCH (d:Doc {body:'hidden'}) RETURN count(d)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, indexed.Rows, "index candidates need the same property-view recheck")
			_, err = exec.Execute(ctx, "MATCH (d:Doc {title:'view:a'}) SET d.title='updated' RETURN d.title", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			raw, err := be.GetNode("view:a")
			require.NoError(t, err)
			require.Equal(t, "hidden", raw.Properties["body"])
			require.Equal(t, "updated", raw.Properties["title"], "writes must retain the raw stored hidden fields")
		})
	}
}
