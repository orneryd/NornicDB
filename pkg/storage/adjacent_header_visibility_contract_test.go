package storage

// Contract regression: a transaction's header listing of one node's adjacency
// returns the relationships its full listing returns at the same snapshot.
//
// The adjacent-edge migration made Headers a property of one context-first
// request (AdjacentEdgeRead). Before it, the transaction's header reader
// declined under visibility filtering (ba02c1be5, tx.edgeHeaders: "declines
// without a pinned snapshot and under decay filtering"), so a caller that asked
// for headers fell back to relationship records and each record's own
// properties decided decay. The context-first request has no such decline:
// readCachedSnapshotAdjacentEdgesLocked appends the compact adjacency header of
// every entry whose MVCC head is in effect at the read version, and
// decodeAdjacencyHeader restores id, type, endpoints, timestamps and the
// suppression flag from that value but not Properties. ReadEdges then applies
// its terminal visibility filter to whatever the listing produced, so a header
// reaches filterEntityByDecay with nil Properties and is scored by
// ScoreEdgeWithProperties without them.
//
// The difference is observable for a supported property-dependent promotion
// policy: the record listing promotes a protected relationship above the
// visibility threshold from its properties, while a header of the same
// relationship is scored with no properties, is not promoted, and decays out.
// Two listings of one snapshot then disagree.
//
// The fixture uses documented schema operations only: an edge-scoped
// exponential decay bundle, an edge binding, a promotion profile that floors a
// matching relationship's score, and a promotion policy whose WHEN predicate
// reads a relationship property. Both fixture relationships carry the same
// timestamps, so the decay curve alone scores them identically and the only
// difference between them is the property the policy reads.
//
// Every case asserts only the required outcome - the exact relationship IDs.
// No assertion inspects how an entry was answered, so any correct owner (record
// read, or a header that carries what the policy needs) satisfies it.

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

const (
	headerVisibilityEdgeType = "LINKS"
	headerVisibilityProperty = "protected"
)

func TestProjectedNodeKeepsPolicyPromotedRow(t *testing.T) {
	for _, rule := range []struct {
		name, predicate string
		kept, gone      map[string]any
	}{
		{name: "equality", predicate: "n.pinned = true", kept: map[string]any{"pinned": true}},
		{name: "comparison", predicate: "n.importance >= 3", kept: map[string]any{"importance": 5}, gone: map[string]any{"importance": 1}},
		{name: "coalesce", predicate: "coalesce(n.archived, 0) = 0", gone: map[string]any{"archived": 1}},
	} {
		t.Run(rule.name, func(t *testing.T) {
			ctx := t.Context()
			engine := newTestEngine(t)
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
				Name: "fades", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
				HalfLifeSeconds: 3600, VisibilityThreshold: 0.5, ScoreFrom: knowledgepolicy.ScoreFromVersion,
				Enabled: true, DecayEnabled: true,
			}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
				Name: "article_decay", TargetLabels: []string{"Article"}, ProfileRef: "fades",
			}))
			require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
				Name: "protected", Scope: knowledgepolicy.ScopeNode, Multiplier: 1, ScoreFloor: 1, ScoreCap: 1, Enabled: true,
			}))
			require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
				Name: "protect_articles", TargetLabels: []string{"Article"}, Enabled: true,
				WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: rule.predicate, ProfileRef: "protected", Order: 1}},
			}))
			engine.SetDecayEnabled(true)
			stale := time.Now().Add(-100 * time.Hour)
			keptProperties := map[string]any{"title": "Article"}
			for name, value := range rule.kept {
				keptProperties[name] = value
			}
			goneProperties := map[string]any{"title": "Faded"}
			for name, value := range rule.gone {
				goneProperties[name] = value
			}
			id, err := engine.CreateNode(ctx, &Node{ID: "test:article", Labels: []string{"Article"},
				Properties: keptProperties, CreatedAt: stale, UpdatedAt: stale})
			require.NoError(t, err)
			_, err = engine.CreateNode(ctx, &Node{ID: "test:faded-article", Labels: []string{"Article"},
				Properties: goneProperties, CreatedAt: stale, UpdatedAt: stale})
			require.NoError(t, err)
			full, err := engine.GetNode(ctx, id)
			require.NoError(t, err)
			for name, value := range rule.kept {
				require.EqualValues(t, value, full.Properties[name])
			}
			_, err = engine.GetNode(ctx, "test:faded-article")
			require.ErrorIs(t, err, ErrNotFound)

			wanted := []string{"title"}
			check := func(node *Node) error {
				require.Equal(t, id, node.ID)
				require.Equal(t, map[string]any{"title": "Article"}, node.Properties)
				return nil
			}
			projected, err := engine.GetNodeProjected(ctx, id, wanted)
			require.NoError(t, err)
			require.NoError(t, check(projected))
			_, err = engine.GetNodeProjected(ctx, "test:faded-article", wanted)
			require.ErrorIs(t, err, ErrNotFound)
			scanned := 0
			require.NoError(t, engine.StreamNodesByLabelProjected(ctx, "Article", wanted, func(node *Node) error {
				scanned++
				return check(node)
			}))
			require.Equal(t, 1, scanned)
			tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
			projected, err = tx.GetNodeProjected(ctx, id, wanted)
			require.NoError(t, err)
			require.NoError(t, check(projected))
			for range 2 {
				scanned = 0
				require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Article", wanted, func(node *Node) error {
					scanned++
					return check(node)
				}))
				require.Equal(t, 1, scanned, "transaction scan and cached replay must retain the promoted row")
			}
			engine.SetDecayEnabled(false)
			unfiltered, err := engine.GetNodesByLabel(ctx, "Article")
			require.NoError(t, err)
			require.Len(t, unfiltered, 2, "decay-off control returns both stored nodes")
		})
	}
}

// newHeaderVisibilityContractEngine builds the policy fixture and the graph the
// contract cases list from test:a:
//
//	test:kept   (test:a)->(test:b)  properties{protected: true}  visible
//	test:faded  (test:a)->(test:c)  no properties                decayed out
//
// Both relationships are old enough that the decay curve alone puts either one
// below the visibility threshold; only the promotion policy's property
// predicate separates them.
func newHeaderVisibilityContractEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "fades",
		Scope:               knowledgepolicy.ScopeEdge,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.5,
		ScoreFrom:           knowledgepolicy.ScoreFromVersion,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:           "links_fade",
		TargetEdgeType: headerVisibilityEdgeType,
		IsEdge:         true,
		ProfileRef:     "fades",
	}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       "protect",
		Scope:      knowledgepolicy.ScopeEdge,
		Multiplier: 1,
		ScoreFloor: 1,
		ScoreCap:   1,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:           "protect_protected_links",
		TargetEdgeType: headerVisibilityEdgeType,
		IsEdge:         true,
		Enabled:        true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{
			Predicate:  "r." + headerVisibilityProperty + " = true",
			ProfileRef: "protect",
			Order:      1,
		}},
	}))
	engine.SetDecayEnabled(true)

	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d", "test:e"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	stale := time.Now().Add(-1000 * time.Hour)
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID:         "test:kept",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       headerVisibilityEdgeType,
		CreatedAt:  stale,
		UpdatedAt:  stale,
		Properties: map[string]any{headerVisibilityProperty: true},
	}))
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID:        "test:faded",
		StartNode: "test:a",
		EndNode:   "test:c",
		Type:      headerVisibilityEdgeType,
		CreatedAt: stale,
		UpdatedAt: stale,
	}))

	// The cases below are only meaningful when the policy really does decide
	// these two relationships differently from their records.
	_, err := engine.GetEdge(t.Context(), "test:kept")
	require.NoError(t, err, "the protection policy must keep a protected relationship visible from its record")
	_, err = engine.GetEdge(t.Context(), "test:faded")
	require.ErrorIs(t, err, ErrNotFound, "an unprotected relationship of the same age must decay out")
	return engine
}

// requireHeaderVisibilityContract lists node both ways through tx at its one
// snapshot: the full listing must return exactly want, and the header listing
// must return exactly the full listing's relationships.
func requireHeaderVisibilityContract(t *testing.T, tx *BadgerTransaction, node NodeID, want []string) {
	t.Helper()
	full, err := tx.ReadEdges(t.Context(), AdjacentEdgeRead{Node: node, Types: []string{headerVisibilityEdgeType}})
	require.NoError(t, err)
	require.Equal(t, want, sortedEdgeIDStrings(full), "the full listing at this snapshot")

	headers, err := tx.ReadEdges(t.Context(), AdjacentEdgeRead{Node: node, Types: []string{headerVisibilityEdgeType}, Headers: true})
	require.NoError(t, err)
	require.Equal(t, sortedEdgeIDStrings(full), sortedEdgeIDStrings(headers),
		"a header listing must return the relationships the full listing returns at the same snapshot")
	require.Equal(t, want, sortedEdgeIDStrings(headers))
}

// TestTransactionAdjacentHeadersMatchRecordVisibilityUnderDecay covers the
// views a native transaction adjacency read supports: the pinned snapshot, the
// same snapshot overlaid with the transaction's own pending writes, and a
// pinned snapshot whose relationship a peer has since changed. Each case lists
// test:a through ReadEdges with Headers false and true and requires the same
// relationship IDs.
func TestTransactionAdjacentHeadersMatchRecordVisibilityUnderDecay(t *testing.T) {
	t.Run("pinned snapshot", func(t *testing.T) {
		engine := newHeaderVisibilityContractEngine(t)
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("test"))

		requireHeaderVisibilityContract(t, tx, "test:a", []string{"test:kept"})
	})

	t.Run("pending overlay", func(t *testing.T) {
		engine := newHeaderVisibilityContractEngine(t)
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("test"))
		// An own write away from test:a makes ReadEdges merge its committed
		// adjacency through the pending overlay, which is where the terminal
		// visibility filter runs for committed relationships too.
		require.NoError(t, tx.CreateEdge(t.Context(), &Edge{
			ID:        "test:pending",
			StartNode: "test:d",
			EndNode:   "test:e",
			Type:      headerVisibilityEdgeType,
		}))

		requireHeaderVisibilityContract(t, tx, "test:a", []string{"test:kept"})
	})

	t.Run("pinned historical", func(t *testing.T) {
		engine := newHeaderVisibilityContractEngine(t)
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		require.NoError(t, tx.SetNamespace("test"))
		// A peer clears the protecting property after the snapshot, so the
		// read version no longer agrees with the current adjacency entry and
		// the snapshot record decides.
		require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{
			ID:         "test:kept",
			StartNode:  "test:a",
			EndNode:    "test:b",
			Type:       headerVisibilityEdgeType,
			Properties: map[string]any{headerVisibilityProperty: false},
		}}))
		require.NoError(t, tx.CreateEdge(t.Context(), &Edge{
			ID:        "test:pending",
			StartNode: "test:d",
			EndNode:   "test:e",
			Type:      headerVisibilityEdgeType,
		}))

		requireHeaderVisibilityContract(t, tx, "test:a", []string{"test:kept"})
	})
}
