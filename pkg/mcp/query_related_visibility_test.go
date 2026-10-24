package mcp

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMCPRelatedNodesProjectSuppressedTitle(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, be.Close()) })
	schema := be.GetSchemaForNamespace("tenant")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "doc", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "doc", ProfileRef: "doc", TargetLabels: []string{"Doc"}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "title", HalfLifeSeconds: 1}, {PropertyPath: "name", HalfLifeSeconds: 1}}}))
	engine := storage.NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "private title", "name": "private fallback"}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ca", StartNode: "c", EndNode: "a", Type: "LINK"}))
	be.SetDecayEnabled(true)
	router := NewServer(nil, nil)
	router.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return engine, nil })
	normalCtx := ContextWithDatabase(context.Background(), "tenant")
	related := router.getRelatedNodes(normalCtx, "a", 2)
	require.Len(t, related, 2)
	for _, node := range related {
		require.Empty(t, node.Title)
	}
	require.ElementsMatch(t, []string{"outgoing", "incoming"}, []string{related[0].Direction, related[1].Direction})
	raw, err := engine.GetNode("b")
	require.NoError(t, err)
	require.Equal(t, "private title", raw.Properties["title"])
	// An already-owned REVEAL query preserves its own visibility without taking
	// a second read lock behind a queued writer.
	ctx, release := storage.BeginQueryRevealScopeContext(context.Background(), engine, true)
	defer release()
	revealed := router.getRelatedNodes(ContextWithDatabase(ctx, "tenant"), "a", 2)
	require.Len(t, revealed, 2)
	for _, node := range revealed {
		require.Equal(t, "private title", node.Title)
	}
}
