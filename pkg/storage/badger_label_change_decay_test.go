package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// labelChangeDecayFixture stands up a BadgerEngine with decay enabled and a
// single binding "doc_binding" that targets label "Doc" via a strongly
// suppressing exponential profile. Any label outside that binding has no
// decay attached, so a labels=["Untargeted"] node should never be
// suppressed.
//
// Returns the engine and the absolute time used to backdate "old" entities
// far past the half-life so a single Score evaluation produces a value below
// the visibility threshold.
func labelChangeDecayFixture(t *testing.T) (*BadgerEngine, time.Time) {
	t.Helper()
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)

	schema := eng.GetSchemaForNamespace("test")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "doc_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     1,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "doc_binding",
		ProfileRef:   "doc_decay",
		TargetLabels: []string{"Doc"},
	}))

	old := time.Now().Add(-72 * time.Hour)
	return eng, old
}

// TestLabelChange_TargetedToUntargeted_RescoresAndUnsuppresses pins down the
// Cypher SET label semantics: a node that was suppressed by a Doc-only
// decay binding must become visible again after its labels change to
// "Untargeted" (no binding). Mirrors `MATCH (n:Doc) SET n:Untargeted
// REMOVE n:Doc`.
func TestLabelChange_TargetedToUntargeted_RescoresAndUnsuppresses(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	// Step 1: create as "Doc" and let decay drop it below threshold.
	_, err := eng.CreateNode(&Node{
		ID:         "test:n1",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)

	// Force the suppression evaluator. Confirms the test setup is
	// suppression-eligible before we move labels.
	became, err := eng.EnqueueDeindexIfSuppressed("test:n1", false)
	require.NoError(t, err)
	require.True(t, became, "precondition: node must start suppressed under Doc binding")

	_, err = eng.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "Doc-bound stale node must read as not-found")

	// Step 2: relabel away from Doc. Cypher: MATCH (n) SET n:Untargeted
	// REMOVE n:Doc — which translates to UpdateNode with new labels.
	require.NoError(t, eng.UpdateNode(&Node{
		ID:         "test:n1",
		Labels:     []string{"Untargeted"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	}))

	// Step 3: the rescore must clear suppression. Without rebind,
	// the persisted VisibilitySuppressed flag would still be true
	// and filterNodeByDecay short-circuits → not-found.
	got, err := eng.GetNode("test:n1")
	require.NoError(t, err,
		"rebind: relabeling away from a suppressing binding must un-suppress on next read")
	require.Equal(t, []string{"Untargeted"}, got.Labels)
	require.False(t, got.VisibilitySuppressed,
		"the suppressed flag must be cleared because the new labels have no binding")
}

// TestLabelChange_UntargetedToTargeted_RescoresAndSuppresses is the
// vice-versa case: a node that was visible under labels with no decay
// binding must become suppressed after relabeling to "Doc" if its age
// already places it below the threshold. Mirrors `MATCH (n:Untargeted)
// SET n:Doc REMOVE n:Untargeted`.
func TestLabelChange_UntargetedToTargeted_RescoresAndSuppresses(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	// Untargeted → no decay bound → visible regardless of age.
	_, err := eng.CreateNode(&Node{
		ID:         "test:n2",
		Labels:     []string{"Untargeted"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)
	got, err := eng.GetNode("test:n2")
	require.NoError(t, err, "precondition: untargeted stale node is visible")
	require.False(t, got.VisibilitySuppressed)

	// Relabel into Doc — now the binding applies and the age should
	// suppress it on the next read.
	require.NoError(t, eng.UpdateNode(&Node{
		ID:         "test:n2",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	}))

	_, err = eng.GetNode("test:n2")
	require.ErrorIs(t, err, ErrNotFound,
		"rebind: relabeling into a suppressing binding must suppress on next read")
}

// TestLabelChange_TargetedToTargeted_StaysSuppressed sanity-checks that the
// fix doesn't unsuppress a node whose new labels still match a suppressing
// binding.
func TestLabelChange_TargetedToTargeted_StaysSuppressed(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	// Add a second binding for "Article" with the same suppressing profile.
	require.NoError(t, eng.GetSchemaForNamespace("test").CreateDecayProfileBinding(
		knowledgepolicy.DecayProfileBinding{
			Name:         "article_binding",
			ProfileRef:   "doc_decay",
			TargetLabels: []string{"Article"},
		}))

	_, err := eng.CreateNode(&Node{
		ID:         "test:n3",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)
	became, err := eng.EnqueueDeindexIfSuppressed("test:n3", false)
	require.NoError(t, err)
	require.True(t, became)

	require.NoError(t, eng.UpdateNode(&Node{
		ID:         "test:n3",
		Labels:     []string{"Article"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	}))

	_, err = eng.GetNode("test:n3")
	require.ErrorIs(t, err, ErrNotFound,
		"a node moving between two suppressing bindings must remain suppressed")
}

// TestLabelChange_ReadModifyWrite_TargetedToUntargeted is the realistic
// Cypher SET label flow: the application reads the suppressed node out via
// the *unfiltered* path (e.g. an admin reveal query, or via the lower-level
// engine that surfaces the persisted struct), changes its labels, and
// writes it back. The persisted VisibilitySuppressed flag travels along
// in the round-trip.
//
// Without label-change rescore, the rewritten node carries the stale
// "true" flag forever and filterNodeByDecay short-circuits to suppress
// on every future read — even though the new labels don't bind to any
// suppressing profile. That's the actual bug the user is asking about.
func TestLabelChange_ReadModifyWrite_TargetedToUntargeted_RescoresOnUpdate(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	_, err := eng.CreateNode(&Node{
		ID:         "test:rmw",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)
	became, err := eng.EnqueueDeindexIfSuppressed("test:rmw", false)
	require.NoError(t, err)
	require.True(t, became, "precondition: must suppress under Doc binding")

	// Read the persisted node directly through badger to bypass the
	// in-memory cache (the cache still holds the pre-suppress copy from
	// CreateNode and would mask the persisted true flag).
	var persisted *Node
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:rmw"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			var decodeErr error
			persisted, decodeErr = eng.decodeNodeWithEmbeddings(txn, val, "test:rmw")
			return decodeErr
		})
	}))
	require.True(t, persisted.VisibilitySuppressed,
		"the persisted struct must reflect suppressed state for the test to be meaningful")

	// Modify labels (Cypher: REMOVE :Doc SET :Untargeted) and write back.
	// VisibilitySuppressed=true tags along in the round-trip.
	persisted.Labels = []string{"Untargeted"}
	require.NoError(t, eng.UpdateNode(persisted))

	// The new labels have no decay binding, so on the next read the score
	// must NOT be suppressed. We re-fetch the persisted struct directly
	// from badger (bypassing the in-memory cache) to verify the flag has
	// actually been cleared on disk — the cache-hit path can hide the
	// stale persisted flag because copyNode does not preserve it, but
	// every restart, secondary read path, or cross-process reader sees
	// the persisted flag and would mis-suppress.
	var afterUpdate *Node
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:rmw"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			var decodeErr error
			afterUpdate, decodeErr = eng.decodeNodeWithEmbeddings(txn, val, "test:rmw")
			return decodeErr
		})
	}))
	require.False(t, afterUpdate.VisibilitySuppressed,
		"persisted suppression flag must be cleared on disk by UpdateNode after a label rebind")
	require.Equal(t, []string{"Untargeted"}, afterUpdate.Labels)

	// And the GetNode read path must also see the node visible.
	got, err := eng.GetNode("test:rmw")
	require.NoError(t, err)
	require.False(t, got.VisibilitySuppressed)
	require.Equal(t, []string{"Untargeted"}, got.Labels)
}

// TestLabelChange_ReadModifyWrite_UntargetedToTargeted_RescoresOnUpdate
// is the vice-versa case: a node with no binding (visible, flag=false on
// disk) is round-tripped, relabeled to a suppressing binding, and written
// back. The persisted flag must flip to true on the same Update so the
// node is suppressed on the very next read.
func TestLabelChange_ReadModifyWrite_UntargetedToTargeted_RescoresOnUpdate(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	_, err := eng.CreateNode(&Node{
		ID:         "test:rmw2",
		Labels:     []string{"Untargeted"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)

	// Read persisted struct directly — flag should be false to start.
	var persisted *Node
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:rmw2"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			var decodeErr error
			persisted, decodeErr = eng.decodeNodeWithEmbeddings(txn, val, "test:rmw2")
			return decodeErr
		})
	}))
	require.False(t, persisted.VisibilitySuppressed)

	// Cypher SET label :Doc REMOVE label :Untargeted — labels move INTO
	// the suppressing binding; the rescore on UpdateNode must flip the
	// persisted flag immediately.
	persisted.Labels = []string{"Doc"}
	require.NoError(t, eng.UpdateNode(persisted))

	var afterUpdate *Node
	require.NoError(t, eng.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:rmw2"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			var decodeErr error
			afterUpdate, decodeErr = eng.decodeNodeWithEmbeddings(txn, val, "test:rmw2")
			return decodeErr
		})
	}))
	require.True(t, afterUpdate.VisibilitySuppressed,
		"label change INTO a suppressing binding must mark the persisted flag suppressed")
	require.Equal(t, []string{"Doc"}, afterUpdate.Labels)

	_, err = eng.GetNode("test:rmw2")
	require.ErrorIs(t, err, ErrNotFound,
		"GetNode must return not-found because the node is now suppressed under the Doc binding")
}

// TestLabelChange_NoDecayLabelToNoDecayLabel_StaysVisible is the trivial
// vice-versa companion: untargeted on both sides, must stay visible.
func TestLabelChange_NoDecayLabelToNoDecayLabel_StaysVisible(t *testing.T) {
	eng, old := labelChangeDecayFixture(t)

	_, err := eng.CreateNode(&Node{
		ID:         "test:n4",
		Labels:     []string{"Untargeted"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	})
	require.NoError(t, err)

	require.NoError(t, eng.UpdateNode(&Node{
		ID:         "test:n4",
		Labels:     []string{"Other"},
		Properties: map[string]any{"title": "stale"},
		CreatedAt:  old,
		UpdatedAt:  old,
	}))

	got, err := eng.GetNode("test:n4")
	require.NoError(t, err)
	require.False(t, got.VisibilitySuppressed)
	require.Equal(t, []string{"Other"}, got.Labels)
}
