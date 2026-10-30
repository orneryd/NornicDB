package server

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNormalGraphQueriesProjectSuppressedProperties(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	schema := be.GetSchemaForNamespace("tenant")
	for _, edge := range []bool{false, true} {
		scope := knowledgepolicy.ScopeNode
		name := "doc"
		if edge {
			scope = knowledgepolicy.ScopeEdge
			name = "link"
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: name + "binding", ProfileRef: name, TargetLabels: []string{"Doc"}, TargetEdgeType: "LINK", IsEdge: edge, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}))
	}
	be.SetDecayEnabled(true)
	engine := storage.NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]interface{}{"public": "yes", "secret": "hidden"}, CreatedAt: old, UpdatedAt: old}))
	s := &Server{}
	for _, path := range []bool{false, true} {
		t.Run(map[bool]string{false: "neighborhood and expand", true: "path"}[path], func(t *testing.T) {
			var c graphCollection
			var err error
			if path {
				c, err = s.collectLatestPath(context.Background(), engine, "a", "b", 10, newGraphFilterSet(nil, nil))
			} else {
				c, err = s.collectLatestNeighborhood(context.Background(), engine, []string{"a"}, 1, 10, "both", newGraphFilterSet(nil, nil))
			}
			require.NoError(t, err)
			require.Len(t, c.nodes, 2)
			require.Len(t, c.edges, 1)
			for _, n := range c.nodes {
				require.NotContains(t, n.Properties, "secret")
				require.Equal(t, "yes", n.Properties["public"])
			}
			for _, e := range c.edges {
				require.NotContains(t, e.Properties, "secret")
				require.Equal(t, "yes", e.Properties["public"])
			}
		})
	}
	zero, err := s.collectLatestPath(context.Background(), engine, "a", "a", 10, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.NotContains(t, zero.nodes["a"].Properties, "secret")
	raw, err := engine.GetNode("a")
	require.NoError(t, err)
	require.Equal(t, "hidden", raw.Properties["secret"])
	history, err := s.collectLatestInducedSubgraph(engine, []string{"a", "b"}, newGraphFilterSet(nil, nil))
	require.NoError(t, err)
	require.Contains(t, history.nodes["a"].Properties, "secret")
}
