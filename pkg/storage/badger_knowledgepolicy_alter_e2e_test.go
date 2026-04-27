package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestE2E_AlterDecayProfileUnsuppressesExistingNodeEdgeAndProperties(t *testing.T) {
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)
	namespace := "test"
	schema := eng.GetSchemaForNamespace(namespace)

	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "node_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "edge_decay",
		Scope:               knowledgepolicy.ScopeEdge,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "doc_binding",
		ProfileRef:   "node_decay",
		TargetLabels: []string{"Doc"},
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
			{PropertyPath: "body", ProfileRef: "node_decay"},
		},
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:           "rel_binding",
		ProfileRef:     "edge_decay",
		TargetEdgeType: "LINKS",
		IsEdge:         true,
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
			{PropertyPath: "summary", ProfileRef: "edge_decay"},
		},
	}))

	old := time.Now().Add(-72 * time.Hour)
	_, err := eng.CreateNode(&Node{ID: NodeID("test:source"), Labels: []string{"Doc"}, Properties: map[string]any{"body": "hidden", "title": "kept"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	_, err = eng.CreateNode(&Node{ID: NodeID("test:target"), Labels: []string{"Doc"}, Properties: map[string]any{"body": "hidden-target"}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&Edge{ID: EdgeID("test:edge-1"), StartNode: NodeID("test:source"), EndNode: NodeID("test:target"), Type: "LINKS", Properties: map[string]any{"summary": "hidden-edge"}, CreatedAt: old, UpdatedAt: old}))

	becameSuppressed, err := eng.EnqueueDeindexIfSuppressed("test:source", false)
	require.NoError(t, err)
	require.True(t, becameSuppressed)
	becameSuppressed, err = eng.EnqueueDeindexIfSuppressed("test:edge-1", true)
	require.NoError(t, err)
	require.True(t, becameSuppressed)

	_, err = eng.GetNode("test:source")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = eng.GetEdge("test:edge-1")
	require.ErrorIs(t, err, ErrNotFound)
	require.True(t, eng.FilterPropertyByDecay("test:source", []string{"Doc"}, "body", old.UnixNano(), old.UnixNano(), DecayScoringTime()))
	require.True(t, eng.FilterEdgePropertyByDecay("test:edge-1", "LINKS", "summary", old.UnixNano(), old.UnixNano(), DecayScoringTime()))

	require.NoError(t, schema.AlterDecayProfile("node_decay", map[string]interface{}{"decayEnabled": false}))
	require.NoError(t, schema.AlterDecayProfile("edge_decay", map[string]interface{}{"decayEnabled": false}))

	node, err := eng.GetNode("test:source")
	require.NoError(t, err)
	require.False(t, node.VisibilitySuppressed)
	edge, err := eng.GetEdge("test:edge-1")
	require.NoError(t, err)
	require.False(t, edge.VisibilitySuppressed)
	require.False(t, eng.FilterPropertyByDecay("test:source", []string{"Doc"}, "body", old.UnixNano(), old.UnixNano(), DecayScoringTime()))
	require.False(t, eng.FilterEdgePropertyByDecay("test:edge-1", "LINKS", "summary", old.UnixNano(), old.UnixNano(), DecayScoringTime()))

	nodes, err := eng.GetNodesByLabel("Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	edges, err := eng.GetEdgesByType("LINKS")
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

func TestE2E_AlterPromotionPolicyUnsuppressesExistingNode(t *testing.T) {
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)
	namespace := "test"
	schema := eng.GetSchemaForNamespace(namespace)

	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "fact_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "fact_binding",
		ProfileRef:   "fact_decay",
		TargetLabels: []string{"Fact"},
	}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       "lifeline",
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 10,
		ScoreFloor: 0.50,
		ScoreCap:   1.0,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         "fact_policy",
		TargetLabels: []string{"Fact"},
		Enabled:      false,
		WhenClauses:  []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: "n.promote = true", ProfileRef: "lifeline", Order: 0}},
	}))

	old := time.Now().Add(-72 * time.Hour)
	_, err := eng.CreateNode(&Node{ID: NodeID("test:fact-1"), Labels: []string{"Fact"}, Properties: map[string]any{"promote": true}, CreatedAt: old, UpdatedAt: old})
	require.NoError(t, err)

	becameSuppressed, err := eng.EnqueueDeindexIfSuppressed("test:fact-1", false)
	require.NoError(t, err)
	require.True(t, becameSuppressed)
	_, err = eng.GetNode("test:fact-1")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, schema.AlterPromotionPolicy("fact_policy", map[string]interface{}{"enabled": true}))

	node, err := eng.GetNode("test:fact-1")
	require.NoError(t, err)
	require.False(t, node.VisibilitySuppressed)
}
