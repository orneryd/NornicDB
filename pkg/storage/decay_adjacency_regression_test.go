package storage

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecayRelaxAfterHiddenEdgeUpdateRestoresAdjacency(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-update-control", true: "updated-while-hidden"}[update], func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = engine.Close() })
			engine.SetDecayEnabled(true)
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "edge_decay", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", ProfileRef: "edge_decay", TargetEdgeType: "LINKS", IsEdge: true}))
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := engine.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			old := time.Now().Add(-72 * time.Hour)
			require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINKS", CreatedAt: old, UpdatedAt: old}))
			hidden, err := engine.EnqueueDeindexIfSuppressed("test:e", true)
			require.NoError(t, err)
			require.True(t, hidden)
			_, err = NewDeindexCleanupJob(engine.BadgerEngine, time.Hour).RunOnce(context.Background())
			require.NoError(t, err)
			if update {
				engine.SetRevealAll(true)
				edge, err := engine.GetEdge("test:e")
				require.NoError(t, err)
				edge.Properties = map[string]any{"changed": true}
				require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: edge}))
				engine.SetRevealAll(false)
			}
			// Warm both directions while suppressed before restoring visibility.
			edges, err := engine.GetOutgoingEdges("test:a")
			require.NoError(t, err)
			require.Empty(t, edges)
			edges, err = engine.GetIncomingEdges("test:b")
			require.NoError(t, err)
			require.Empty(t, edges)
			edges, err = engine.GetEdgesByType("LINKS")
			require.NoError(t, err)
			require.Empty(t, edges)
			require.NoError(t, schema.AlterDecayProfile("edge_decay", map[string]any{"decayEnabled": false}))
			edge, err := engine.GetEdge("test:e")
			require.NoError(t, err)
			assert.False(t, edge.VisibilitySuppressed)
			edges, err = engine.GetEdgesByType("LINKS")
			require.NoError(t, err)
			assert.Len(t, edges, 1)
			edges, err = engine.GetOutgoingEdges("test:a")
			require.NoError(t, err)
			assert.Len(t, edges, 1)
			edges, err = engine.GetIncomingEdges("test:b")
			require.NoError(t, err)
			assert.Len(t, edges, 1)
		})
	}
}

func TestDecayRelaxAfterHiddenNodeUpdateRefreshesCache(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetDecayEnabled(true)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "node_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "docs", ProfileRef: "node_decay", TargetLabels: []string{"Doc"}}))
	old := time.Now().Add(-72 * time.Hour)
	_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"Doc"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	hidden, err := engine.EnqueueDeindexIfSuppressed("test:n", false)
	require.NoError(t, err)
	require.True(t, hidden)
	_, err = NewDeindexCleanupJob(engine.BadgerEngine, time.Hour).RunOnce(context.Background())
	require.NoError(t, err)
	engine.SetRevealAll(true)
	node, err := engine.GetNode("test:n")
	require.NoError(t, err)
	node.Properties = map[string]any{"changed": true}
	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: node}))
	engine.SetRevealAll(false)
	require.NoError(t, schema.AlterDecayProfile("node_decay", map[string]any{"decayEnabled": false}))
	node, err = engine.GetNode("test:n")
	require.NoError(t, err)
	assert.False(t, node.RuntimeMetadata().VisibilitySuppressed)
	nodes, err := engine.GetNodesByLabel("Doc")
	require.NoError(t, err)
	assert.Len(t, nodes, 1)
}

// TestSuppressionTransactionRollbackDoesNotPublishCaches drives both
// entities through the single suppression owner inside one transaction.
// The transaction takes the write order the public
// EnqueueDeindexIfSuppressed path takes for the same entities; a rollback
// publishes neither cache, and the stored bodies keep the flags they had.
func TestSuppressionTransactionRollbackDoesNotPublishCaches(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetDecayEnabled(true)
	_, err := engine.CreateNode(&Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}}, ID: "test:n"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:b", Type: "LINKS", VisibilitySuppressed: true}))
	nodeGen, edgeGen := engine.nodeCacheGen.current(), engine.edgeCacheGen.current()
	rollback := errors.New("injected rollback after suppression writes")
	var changedNode *Node
	var changedEdge *Edge
	err = engine.withUpdate(func(txn *badger.Txn) error {
		evaluation1, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: "test:n", isEdge: false})
		changed, mutation := evaluation1.suppressed, evaluation1.node
		require.False(t, changed)
		require.NoError(t, err)
		changedNode = mutation
		require.NotNil(t, changedNode)
		evaluation2, err := engine.evaluateSuppressionInTxn(txn, suppressionEvaluation{id: "test:e", isEdge: true})
		changed, mutationEdge := evaluation2.suppressed, evaluation2.edge
		require.False(t, changed)
		require.NoError(t, err)
		changedEdge = mutationEdge
		require.NotNil(t, changedEdge)
		return rollback
	}, writtenEntities{nodes: []NodeID{"test:n"}, edges: []EdgeID{"test:e"}})
	require.ErrorIs(t, err, rollback)
	suppressed, _, err := engine.readNodeSuppressionState("test:n")
	require.NoError(t, err)
	assert.True(t, suppressed)
	suppressed, _, err = engine.readEdgeSuppressionState("test:e")
	require.NoError(t, err)
	assert.True(t, suppressed)
	assert.Equal(t, nodeGen, engine.nodeCacheGen.current())
	assert.Equal(t, edgeGen, engine.edgeCacheGen.current())
	engine.nodeCacheMu.RLock()
	cachedNode := engine.nodeCache["test:n"]
	engine.nodeCacheMu.RUnlock()
	require.NotNil(t, cachedNode)
	assert.True(t, cachedNode.RuntimeMetadata().VisibilitySuppressed)
	cachedEdge, ok := engine.cacheLoadEdge("test:e")
	require.True(t, ok)
	assert.True(t, cachedEdge.VisibilitySuppressed)
}
