package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// publishedEvents records what an engine reports to its event consumers.
type publishedEvents struct {
	mu     sync.Mutex
	events []string
}

func (p *publishedEvents) add(event string) {
	p.mu.Lock()
	p.events = append(p.events, event)
	p.mu.Unlock()
}

func (p *publishedEvents) has(event string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if e == event {
			return true
		}
	}
	return false
}

func (p *publishedEvents) reset() {
	p.mu.Lock()
	p.events = nil
	p.mu.Unlock()
}

func recordEvents(engine *MemoryEngine) *publishedEvents {
	events := &publishedEvents{}
	engine.OnNodeCreated(func(n *Node) { events.add("node created " + string(n.ID)) })
	engine.OnNodeUpdated(func(n *Node) { events.add("node updated " + string(n.ID)) })
	engine.OnNodeDeleted(func(id NodeID) { events.add("node deleted " + string(id)) })
	engine.OnEdgeCreated(func(e *Edge) { events.add("edge created " + string(e.ID)) })
	engine.OnEdgeUpdated(func(e *Edge) { events.add("edge updated " + string(e.ID)) })
	engine.OnEdgeDeleted(func(id EdgeID) { events.add("edge deleted " + string(id)) })
	engine.OnNamespaceReset(func(prefix string) { events.add("namespace reset " + prefix) })
	return events
}

func requirePublished(t *testing.T, events *publishedEvents, event, why string) {
	t.Helper()
	require.Eventually(t, func() bool { return events.has(event) }, 2*time.Second, 5*time.Millisecond, "%s must publish %q", why, event)
	events.reset()
}

// Every persisted change that alters what a read returns is published, so a
// consumer that derives state from storage (search) never misses one (#1482
// round 3). A mutator added without its event fails here.
func TestStorageMutatorsPublishTheirEvents(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	events := recordEvents(engine)

	_, err := engine.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	requirePublished(t, events, "node created test:a", "CreateNode")
	_, err = engine.CreateNode(&Node{ID: "test:b"})
	require.NoError(t, err)
	events.reset()
	require.NoError(t, engine.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": 1}}))
	requirePublished(t, events, "node updated test:a", "UpdateNode")
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", Type: "R", StartNode: "test:a", EndNode: "test:b"}))
	requirePublished(t, events, "edge created test:r", "CreateEdge")
	require.NoError(t, engine.UpdateEdge(&Edge{ID: "test:r", Type: "R", StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"v": 1}}))
	requirePublished(t, events, "edge updated test:r", "UpdateEdge")

	for _, suppressed := range []bool{true, false} {
		require.NoError(t, engine.SetNodeManuallySuppressed("test:a", suppressed))
		requirePublished(t, events, "node updated test:a", "SetNodeManuallySuppressed")
		require.NoError(t, engine.SetEdgeManuallySuppressed("test:r", suppressed))
		requirePublished(t, events, "edge updated test:r", "SetEdgeManuallySuppressed")
	}

	stored, err := engine.GetNode("test:a")
	require.NoError(t, err)
	stored.ChunkEmbeddings = [][]float32{{1, 0}}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(stored))
	requirePublished(t, events, "node updated test:a", "UpdateNodeEmbeddingSidecar")

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": 2}}))
	require.NoError(t, tx.Commit())
	requirePublished(t, events, "node updated test:a", "a transaction commit")

	require.NoError(t, engine.UpdateNode(&Node{ID: "test:a", ChunkEmbeddings: [][]float32{{1, 0}}}))
	events.reset()
	cleared, err := engine.ClearAllEmbeddings()
	require.NoError(t, err)
	require.Positive(t, cleared)
	requirePublished(t, events, "node updated test:a", "ClearAllEmbeddings")
	require.NoError(t, engine.UpdateNode(&Node{ID: "test:a", ChunkEmbeddings: [][]float32{{1, 0}}}))
	events.reset()
	_, err = engine.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	requirePublished(t, events, "node updated test:a", "ClearAllEmbeddingsForPrefix")

	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:bulk"}}))
	requirePublished(t, events, "node created test:bulk", "BulkCreateNodes")
	require.NoError(t, engine.BulkCreateEdges([]*Edge{{ID: "test:bulkr", Type: "R", StartNode: "test:a", EndNode: "test:bulk"}}))
	requirePublished(t, events, "edge created test:bulkr", "BulkCreateEdges")
	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"test:bulkr"}))
	requirePublished(t, events, "edge deleted test:bulkr", "BulkDeleteEdges")
	require.NoError(t, engine.BulkDeleteNodes([]NodeID{"test:bulk"}))
	requirePublished(t, events, "node deleted test:bulk", "BulkDeleteNodes")

	require.NoError(t, engine.DeleteEdge("test:r"))
	requirePublished(t, events, "edge deleted test:r", "DeleteEdge")
	require.NoError(t, engine.DeleteNode("test:b"))
	requirePublished(t, events, "node deleted test:b", "DeleteNode")

	_, _, err = engine.DeleteByPrefix("test:")
	require.NoError(t, err)
	requirePublished(t, events, "namespace reset test:", "DeleteByPrefix")
}

// A decay flip rewrites the stored body without UpdateNode; it is published
// as an update in both directions.
func TestDecayFlipsPublishUpdates(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetDecayEnabled(true)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "node_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "docs", ProfileRef: "node_decay", TargetLabels: []string{"Doc"}}))
	old := time.Now().Add(-72 * time.Hour)
	_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"Doc"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	events := recordEvents(engine)

	hidden, err := engine.EnqueueDeindexIfSuppressed("test:n", false)
	require.NoError(t, err)
	require.True(t, hidden)
	requirePublished(t, events, "node updated test:n", "a decay flip to suppressed")

	require.NoError(t, schema.AlterDecayProfile("node_decay", map[string]any{"decayEnabled": false}))
	_, err = engine.EnqueueDeindexIfSuppressed("test:n", false)
	require.NoError(t, err)
	requirePublished(t, events, "node updated test:n", "a decay flip back to visible")

	// A relationship's flips, in both directions.
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "edge_decay", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", ProfileRef: "edge_decay", TargetEdgeType: "LINKS", IsEdge: true}))
	_, err = engine.CreateNode(&Node{ID: "test:m"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINKS", CreatedAt: old, UpdatedAt: old}))
	events.reset()
	_, err = engine.EnqueueDeindexIfSuppressed("test:e", true)
	require.NoError(t, err)
	requirePublished(t, events, "edge updated test:e", "a relationship decay flip to suppressed")
	require.NoError(t, schema.AlterDecayProfile("edge_decay", map[string]any{"decayEnabled": false}))
	_, err = engine.EnqueueDeindexIfSuppressed("test:e", true)
	require.NoError(t, err)
	requirePublished(t, events, "edge updated test:e", "a relationship decay flip back to visible")

	// A policy change reconciles (ReconcileDecaySuppression) what the policy
	// now hides, and shows again what it no longer hides.
	events.reset()
	require.NoError(t, schema.AlterDecayProfile("node_decay", map[string]any{"decayEnabled": true}))
	requirePublished(t, events, "node updated test:n", "a policy reconcile hiding a node")
	require.NoError(t, schema.AlterDecayProfile("node_decay", map[string]any{"decayEnabled": false}))
	requirePublished(t, events, "node updated test:n", "a policy reconcile showing a node")
}
