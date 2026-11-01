package server

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHeimdallRelatedNodesProjectSuppressedTitle(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, be.Close()) })
	schema := be.GetSchemaForNamespace("tenant")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "doc", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "doc", ProfileRef: "doc", TargetLabels: []string{"Doc"}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "title", HalfLifeSeconds: 1}}}))
	engine := storage.NewNamespacedEngine(be, "tenant")
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "private title"}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK"}))
	be.SetDecayEnabled(true)
	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(context.Background(), engine, "a", 1)
	require.Len(t, related, 1)
	require.Empty(t, related[0].Title)
	require.Equal(t, "b", related[0].ID)
	raw, err := engine.GetNode("b")
	require.NoError(t, err)
	require.Equal(t, "private title", raw.Properties["title"])
	// An already-owned REVEAL query preserves its own visibility without taking
	// a second read lock behind a queued writer.
	ctx, release := storage.BeginQueryRevealScopeContext(context.Background(), engine, true)
	defer release()
	revealed := router.getRelatedNodes(ctx, engine, "a", 1)
	require.Len(t, revealed, 1)
	require.Equal(t, "private title", revealed[0].Title)
}

// A single high-degree node must not bypass the documented related-result cap.
func TestHeimdallRelatedNodesCapsSingleAdjacency(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	engine := storage.NewNamespacedEngine(base, "related-cap")
	_, err := engine.CreateNode(&storage.Node{ID: "root", Labels: []string{"Doc"}})
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		id := storage.NodeID(fmt.Sprintf("neighbor-%02d", i))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("edge-%02d", i)), StartNode: "root", EndNode: id, Type: "LINK"}))
	}
	router := &heimdallDBRouter{}
	result := router.getRelatedNodes(context.Background(), engine, "root", 1)
	require.Len(t, result, 50, "one adjacency must obey the same cap as the outer traversal")
	for _, node := range result {
		require.Equal(t, 1, node.Distance)
		require.Equal(t, "outgoing", node.Direction)
		require.Equal(t, "LINK", node.Relationship)
	}
}
