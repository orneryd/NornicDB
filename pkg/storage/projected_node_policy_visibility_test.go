package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// projectedPolicyVisibilityRule is one promotion-policy WHEN predicate under
// test together with the stored properties that make it hold (kept) or fail
// (gone). Each rule owns its label, so no binding or policy of one rule can
// score another rule's nodes.
type projectedPolicyVisibilityRule struct {
	label     string
	predicate string
	kept      map[string]any
	gone      map[string]any
}

// projectedPolicyVisibilityEngine builds an engine whose decay hides every aged
// node of the rule's label and whose promotion policy lifts only the nodes the
// WHEN predicate holds for back above the visibility threshold. The predicate
// reads a stored property, so it holds only while the scoring input is the
// whole node: the same node scored from a projected image — which lacks that
// property — is either promoted although it should be hidden or left below the
// threshold although it should be kept.
func projectedPolicyVisibilityEngine(t *testing.T, rule projectedPolicyVisibilityRule) *BadgerEngine {
	t.Helper()
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)
	schema := eng.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                rule.label + "_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.3,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         rule.label + "_binding",
		ProfileRef:   rule.label + "_decay",
		TargetLabels: []string{rule.label},
	}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       rule.label + "_promotion",
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 1.25,
		ScoreFloor: 0.9,
		ScoreCap:   0.95,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         rule.label + "_policy",
		TargetLabels: []string{rule.label},
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{
			Predicate:  rule.predicate,
			ProfileRef: rule.label + "_promotion",
		}},
		Enabled: true,
	}))
	return eng
}

// createPolicyVisibilityNode stores one node of the rule's label with the
// property set the rule describes, stamped at a fixed age.
func createPolicyVisibilityNode(t *testing.T, eng *BadgerEngine, id NodeID, label, key string, at time.Time, policyProperties map[string]any) {
	t.Helper()
	properties := make(map[string]any, len(policyProperties)+2)
	properties["key"] = key
	properties["text"] = "t-" + key
	for name, value := range policyProperties {
		properties[name] = value
	}
	_, err := eng.CreateNode(t.Context(), &Node{ID: id, Labels: []string{label}, Properties: properties, CreatedAt: at, UpdatedAt: at})
	require.NoError(t, err)
}

// TestProjectedNodeVisibilityAgreesWithWholeNodePolicy pins the product rule
// that a promotion-policy WHEN clause is decided on the stored node, not on the
// image a projected read happens to carry. Whole-node reads and projected reads
// — point and label scan — must therefore return the same nodes, the hidden node
// must not appear on any of them, and a projected row must still carry only the
// properties it was asked for.
func TestProjectedNodeVisibilityAgreesWithWholeNodePolicy(t *testing.T) {
	rules := []projectedPolicyVisibilityRule{
		{label: "PolicyEquality", predicate: "n.keep = true",
			kept: map[string]any{"keep": true}, gone: map[string]any{"keep": false}},
		{label: "PolicyComparison", predicate: "n.importance >= 3",
			kept: map[string]any{"importance": 5}, gone: map[string]any{"importance": 1}},
		{label: "PolicyCoalesce", predicate: "coalesce(n.archived, 0) = 0",
			gone: map[string]any{"archived": 1}},
	}
	for _, rule := range rules {
		t.Run(rule.label, func(t *testing.T) {
			eng := projectedPolicyVisibilityEngine(t, rule)
			aged := time.Now().Add(-72 * time.Hour)
			keptID, goneID, freshID := NodeID("test:kept"), NodeID("test:gone"), NodeID("test:fresh")
			createPolicyVisibilityNode(t, eng, keptID, rule.label, "kept", aged, rule.kept)
			createPolicyVisibilityNode(t, eng, goneID, rule.label, "gone", aged, rule.gone)
			// The fresh node keeps a score above the threshold without promotion,
			// so the fixture also proves the label scans are not simply empty.
			createPolicyVisibilityNode(t, eng, freshID, rule.label, "fresh", time.Now(), nil)

			// Precondition: storage holds three nodes, so any missing row below is
			// the visibility layer's decision and not a fixture that never wrote it.
			stored, err := eng.GetStoredNodesByLabel(t.Context(), rule.label)
			require.NoError(t, err)
			require.Equal(t, []NodeID{freshID, goneID, keptID}, sortedNodeIDs(stored),
				"precondition: the fixture stores the kept, hidden and fresh nodes")

			// Whole-node reads are the reference answer: the promotion policy keeps
			// the node whose WHEN holds, decay hides the other aged node, and the
			// fresh node is above the threshold on its own.
			whole, err := eng.GetNodesByLabel(t.Context(), rule.label)
			require.NoError(t, err)
			require.Equal(t, []NodeID{freshID, keptID}, sortedNodeIDs(whole),
				"whole-node label read keeps the promoted and the fresh node only")
			keptNode, err := eng.GetNode(t.Context(), keptID)
			require.NoError(t, err)
			for name, value := range rule.kept {
				require.EqualValues(t, value, keptNode.Properties[name],
					"whole-node read still returns the stored property the policy reads")
			}
			_, err = eng.GetNode(t.Context(), goneID)
			require.ErrorIs(t, err, ErrNotFound, "whole-node read hides the node its WHEN fails for")

			// Projected point read: the decision must match the whole node, and the
			// returned row must not carry the policy input as a side effect of the
			// scoring read.
			projectedPoint, err := eng.GetNodeProjected(t.Context(), keptID, []string{"text"})
			require.NoError(t, err, "projected read keeps the node the recorded policy promotes")
			require.Equal(t, map[string]any{"text": "t-kept"}, projectedPoint.Properties,
				"the projected row carries the selected property only")
			_, err = eng.GetNodeProjected(t.Context(), goneID, []string{"text"})
			require.ErrorIs(t, err, ErrNotFound, "projected read hides the node its WHEN fails for")

			// Projected label scan: the same answer as the whole-node label read.
			projectedRows := make([]*Node, 0, len(whole))
			require.NoError(t, eng.StreamNodesByLabelProjected(t.Context(), rule.label, []string{"text"}, func(node *Node) error {
				projectedRows = append(projectedRows, node)
				return nil
			}))
			require.Equal(t, sortedNodeIDs(whole), sortedNodeIDs(projectedRows),
				"projected label scan and whole-node label read agree on which nodes survive")
			projectedByID := make(map[NodeID]*Node, len(projectedRows))
			for _, row := range projectedRows {
				projectedByID[row.ID] = row
				require.NotEqual(t, "t-gone", row.Properties["text"], "no row returns the hidden node's data")
			}
			require.NotContains(t, projectedByID, goneID, "the hidden node is absent from projected rows")
			for _, node := range whole {
				require.Equal(t, map[string]any{"text": node.Properties["text"]}, projectedByID[node.ID].Properties,
					"projected row %s equals the whole node's selected properties", node.ID)
			}

			// Control: with decay off no policy can hide anything, so both read
			// shapes return every stored node and the projected row stays projected.
			// The policy read above is therefore conditional, not a permanent cost.
			eng.SetDecayEnabled(false)
			unfiltered, err := eng.GetNodesByLabel(t.Context(), rule.label)
			require.NoError(t, err)
			require.Equal(t, []NodeID{freshID, goneID, keptID}, sortedNodeIDs(unfiltered))
			unfilteredRows := make([]*Node, 0, len(unfiltered))
			require.NoError(t, eng.StreamNodesByLabelProjected(t.Context(), rule.label, []string{"text"}, func(node *Node) error {
				unfilteredRows = append(unfilteredRows, node)
				return nil
			}))
			require.Equal(t, sortedNodeIDs(unfiltered), sortedNodeIDs(unfilteredRows))
			for _, row := range unfilteredRows {
				require.Equal(t, map[string]any{"text": row.Properties["text"]}, row.Properties)
			}
		})
	}
}
