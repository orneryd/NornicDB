package storage

import (
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSol1482R4SidecarPublishes(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })
	_, err := e.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"content": "text"}})
	require.NoError(t, err)
	n, err := e.GetNode("test:n")
	require.NoError(t, err)
	n.ChunkEmbeddings = [][]float32{{1, 0}}
	events := recordEvents(e)
	require.NoError(t, e.UpdateNodeEmbeddingSidecar(n))
	requirePublished(t, events, "node updated test:n", "sidecar")
}
func TestSol1482R4EdgeDecayPublishes(t *testing.T) {
	e := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = e.Close() })
	e.SetDecayEnabled(true)
	s := e.GetSchemaForNamespace("test")
	require.NoError(t, s.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "edge_decay", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, s.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", ProfileRef: "edge_decay", TargetEdgeType: "LINKS", IsEdge: true}))
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := e.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	old := time.Now().Add(-72 * time.Hour)
	require.NoError(t, e.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINKS", CreatedAt: old, UpdatedAt: old}))
	events := recordEvents(e)
	_, err := e.EnqueueDeindexIfSuppressed("test:e", true)
	require.NoError(t, err)
	requirePublished(t, events, "edge updated test:e", "edge decay hide")
	require.NoError(t, s.AlterDecayProfile("edge_decay", map[string]any{"decayEnabled": false}))
	_, err = e.EnqueueDeindexIfSuppressed("test:e", true)
	require.NoError(t, err)
	requirePublished(t, events, "edge updated test:e", "edge decay show")
}
