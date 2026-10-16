package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// PublishStoredNode, the re-read used by deferred flushes and embedding
// completion, admits a node exactly as the normal index path does: the node
// must be readable (a hidden node reads as not found) and admissible
// (shouldIndexNode). Each producer below removes a node from search while it
// stays in storage; a later re-read must keep it removed, and a node that
// becomes admissible again is indexed again.

func publicationEngine(t *testing.T) (*storage.BadgerEngine, *storage.NamespacedEngine) {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return base, storage.NewNamespacedEngine(base, "test")
}

func searchableIDs(t *testing.T, svc *Service, term string) []string {
	t.Helper()
	resp, err := svc.Search(context.Background(), term, nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		ids = append(ids, r.ID)
	}
	return ids
}

// A temporal version that is no longer current stays in storage.
func TestPublishStoredNodeKeepsClosedTemporalVersionRemoved(t *testing.T) {
	base, ns := publicationEngine(t)
	require.NoError(t, base.GetSchemaForNamespace("test").AddConstraint(storage.Constraint{
		Name:       "fact_temporal",
		Type:       storage.ConstraintTemporal,
		Label:      "FactVersion",
		Properties: []string{"fact_key", "valid_from", "valid_to"},
	}))
	node := &storage.Node{ID: "fact", Labels: []string{"FactVersion"}, Properties: map[string]any{
		"fact_key": "k", "valid_from": time.Now().UTC().Add(-2 * time.Hour), "valid_to": nil, "title": "temporalterm",
	}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}
	_, err := ns.CreateNode(node)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(ns, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.PublishStoredNode("fact"))
	require.Equal(t, 1, svc.EmbeddingCount())

	closed := storage.CopyNode(node)
	closed.Properties["valid_to"] = time.Now().UTC().Add(-time.Hour)
	require.NoError(t, ns.UpdateNode(closed))
	require.NoError(t, svc.RemoveNode("fact"))
	// A late flush or embedding completion re-reads the closed version.
	require.NoError(t, svc.PublishStoredNode("fact"))
	require.Zero(t, svc.EmbeddingCount(), "a version that is no longer current must stay removed")
	require.Empty(t, searchableIDs(t, svc, "temporalterm"))
}

// A manually hidden node keeps its data in storage.
func TestPublishStoredNodeKeepsManuallyHiddenNodeRemoved(t *testing.T) {
	_, ns := publicationEngine(t)
	node := &storage.Node{ID: "hidden", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hiddenterm"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}}
	_, err := ns.CreateNode(node)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(ns, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.PublishStoredNode("hidden"))
	require.Equal(t, []string{"hidden"}, searchableIDs(t, svc, "hiddenterm"))

	// The suppress procedure: hide in storage, then drop from search.
	require.NoError(t, ns.SetNodeManuallySuppressed("hidden", true))
	require.NoError(t, svc.RemoveNode("hidden"))
	require.NoError(t, svc.PublishStoredNode("hidden"))
	require.Empty(t, searchableIDs(t, svc, "hiddenterm"), "a hidden node must stay removed")
	require.Zero(t, svc.EmbeddingCount())

	// Shown again, a re-read indexes it again.
	require.NoError(t, ns.SetNodeManuallySuppressed("hidden", false))
	require.NoError(t, svc.PublishStoredNode("hidden"))
	require.Equal(t, []string{"hidden"}, searchableIDs(t, svc, "hiddenterm"))
}

// A node hidden by decay keeps its data in storage.
func TestPublishStoredNodeKeepsDecayHiddenNodeRemoved(t *testing.T) {
	base, ns := publicationEngine(t)
	old := time.Now().Add(-72 * time.Hour)
	node := &storage.Node{ID: "stale", Labels: []string{"Doc"}, Properties: map[string]any{"text": "staleterm"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}, CreatedAt: old, UpdatedAt: old}
	_, err := ns.CreateNode(node)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(ns, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.PublishStoredNode("stale"))
	require.Equal(t, []string{"stale"}, searchableIDs(t, svc, "staleterm"))

	// A decay policy now hides it; the deindex cleanup drops it from search.
	base.SetDecayEnabled(true)
	schema := base.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "doc_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "doc_binding", ProfileRef: "doc_decay", TargetLabels: []string{"Doc"}}))
	_, err = ns.GetNode("stale")
	require.ErrorIs(t, err, storage.ErrNotFound, "precondition: decay hides the node from reads")
	require.NoError(t, svc.RemoveNode("stale"))
	require.NoError(t, svc.PublishStoredNode("stale"))
	require.Empty(t, searchableIDs(t, svc, "staleterm"), "a decay-hidden node must stay removed")
	require.Zero(t, svc.EmbeddingCount())
}
