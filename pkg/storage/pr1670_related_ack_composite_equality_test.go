package storage

// Read-only audit reproduction for NOR-375 on release f0074ec55, extended to
// the merged read the public paths use: at buffered ACK (before any drain) the
// final node body is visible to every read, but the composite equality index's
// full and prefix postings are only maintained at drain. A reader that
// resolves an indexed MATCH/MERGE candidate set through those postings alone
// therefore omits the acknowledged final membership.
//
// CompositeIndexLookup is the schema-owned read that merges the attached
// pending writes with the index's committed postings, so the acknowledged
// final membership is a candidate exactly when the stored body says it is: the
// previous key is dropped, the pending nodes holding the same values are
// added, and a drained generation is retired from the pending population while
// its committed postings take over.
//
// The fixtures use the existing manual-drain write-behind engine, so nothing
// drains between the acknowledged commit and the lookups. No product hook or
// operator overlay is used.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func relatedAckedCompositeEngine(t *testing.T, options ...writeBehindTestOptions) (*BadgerEngine, *SchemaManager, *CompositeIndex) {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, options...)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddCompositeIndex("doc_tenant_id", "Doc", []string{"tenant", "id"}))
	require.NoError(t, sm.BackfillCompositeIndex(t.Context(), "doc_tenant_id", nil))
	indexes := sm.GetCompositeIndexesForLabel("Doc")
	require.Len(t, indexes, 1)
	return engine, sm, indexes[0]
}

// relatedAckedCompositeSeed writes the node the pending update renames. It is
// a durable write, so the index's committed postings describe it.
func relatedAckedCompositeSeed(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
		"tenant": "before", "id": "stable", "c": int64(0),
	}})
	require.NoError(t, err)
}

// relatedAckedCompositeUpdate stages the two successive key changes from the
// issue in one implicit transaction and returns the acknowledged transaction.
func relatedAckedCompositeUpdate(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
		After: &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
			"tenant": "temporary", "id": "temporary", "c": int64(1),
		}},
		Policy: ClientNodeEdits,
	}))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
		After: &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
			"tenant": "changed", "id": "changed", "c": int64(2),
		}},
		Policy: ClientNodeEdits,
	}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the reproduction must observe a buffered ACK, not a drained apply")
	return tx
}

// relatedAckedCompositeLookup is the merged composite read under test.
func relatedAckedCompositeLookup(t *testing.T, sm *SchemaManager, idx *CompositeIndex, full bool, values ...interface{}) []NodeID {
	t.Helper()
	ids, err := sm.CompositeIndexLookup(t.Context(), idx, values, full)
	require.NoError(t, err)
	return ids
}

func TestRelatedAckedCompositeEqualityLookupSeesFinalMembership(t *testing.T) {
	engine, sm, idx := relatedAckedCompositeEngine(t, writeBehindTestOptions{manualDrain: true})
	relatedAckedCompositeSeed(t, engine)

	seed, err := idx.LookupFull(t.Context(), "before", "stable")
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:doc"}, seed, "committed membership control")

	ack := relatedAckedCompositeUpdate(t, engine)
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "nothing may drain before the lookups")
	require.True(t, ack.AcknowledgedBuffered())

	// Control: the acknowledged final image is already the node every read
	// returns.
	final, err := engine.GetNode(t.Context(), "test:doc")
	require.NoError(t, err)
	require.Equal(t, "changed", final.Properties["tenant"])
	require.Equal(t, "changed", final.Properties["id"])
	require.Equal(t, int64(2), final.Properties["c"])

	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, true, "changed", "changed"),
		"a full composite equality lookup must see the acknowledged final membership exactly once")
	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, false, "changed"),
		"a composite prefix lookup must see the acknowledged final membership")

	// The previous key and the intermediate body the same transaction staged
	// are not candidates: the acknowledged final image is the only membership.
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"),
		"the superseded committed key must not stay a candidate")
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, false, "before"),
		"the superseded committed prefix must not stay a candidate")
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "temporary", "temporary"),
		"only the final acknowledged body is a candidate, not a superseded staged version")
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, false, "temporary"))
}

// A composite index's properties are part of the pending value population a
// merged read draws its candidates from, so a lookup needs no label scan.
func TestRelatedCompositePairsArePendingTracked(t *testing.T) {
	engine, sm, _ := relatedAckedCompositeEngine(t, writeBehindTestOptions{manualDrain: true})
	var tracked []nodePropertyKey
	engine.writeBehind.mu.RLock()
	tracked = append(tracked, engine.writeBehind.trackedPairs...)
	engine.writeBehind.mu.RUnlock()
	require.Contains(t, tracked, nodePropertyKey{label: "Doc", property: "tenant"},
		"every property of a multi-property composite index is a tracked pending pair")
	require.Contains(t, tracked, nodePropertyKey{label: "Doc", property: "id"})
	require.False(t, sm.HasUniqueValueTracking())
}

func TestRelatedAckedCompositeLookupDropsSupersededMembership(t *testing.T) {
	t.Run("label change", func(t *testing.T) {
		engine, sm, idx := relatedAckedCompositeEngine(t, writeBehindTestOptions{manualDrain: true})
		relatedAckedCompositeSeed(t, engine)
		require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"))

		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
			After: &Node{ID: "test:doc", Labels: []string{"Other"}, Properties: map[string]any{
				"tenant": "before", "id": "stable", "c": int64(3),
			}},
			Policy: ClientNodeEdits,
		}))
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())

		final, err := engine.GetNode(t.Context(), "test:doc")
		require.NoError(t, err)
		require.Equal(t, []string{"Other"}, final.Labels, "control: the acknowledged labels are the stored labels")

		require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"),
			"a node whose acknowledged labels no longer carry the index's label is not a candidate")
		require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, false, "before"))
	})

	t.Run("deletion", func(t *testing.T) {
		engine, sm, idx := relatedAckedCompositeEngine(t, writeBehindTestOptions{manualDrain: true})
		relatedAckedCompositeSeed(t, engine)
		require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"))

		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:doc", CascadeEdges))
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())

		_, err = engine.GetNode(t.Context(), "test:doc")
		require.Error(t, err, "control: the acknowledged deletion is already what every read returns")

		require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"),
			"a deleted node's committed posting must not stay a candidate")
		require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, false, "before"))
	})
}

// A prefix lookup is served by the leading indexed properties alone, so a
// pending body that holds only those is a prefix candidate and no full-key
// candidate.
func TestRelatedAckedCompositeLookupPrefixWithIncompleteProperties(t *testing.T) {
	engine, sm, idx := relatedAckedCompositeEngine(t, writeBehindTestOptions{manualDrain: true})
	relatedAckedCompositeSeed(t, engine)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{
		After: &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
			"tenant": "changed", "c": int64(4),
		}},
		Policy: ClientNodeEdits,
	}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())

	final, err := engine.GetNode(t.Context(), "test:doc")
	require.NoError(t, err)
	_, holdsID := final.Properties["id"]
	require.False(t, holdsID, "control: the acknowledged body holds no second indexed value")

	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, false, "changed"),
		"the leading indexed value makes the pending node a prefix candidate")
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "changed", "changed"),
		"a full lookup needs the pending node to hold every indexed value")
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "changed", "stable"))
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "changed", []byte("raw")),
		"a value no index entry can hold has no candidate")
}

// The drain retires a generation from the pending population exactly once, and
// the committed postings it lands are then the only membership.
func TestRelatedAckedCompositeLookupSeesMembershipAfterDrain(t *testing.T) {
	engine, sm, idx := relatedAckedCompositeEngine(t)
	relatedAckedCompositeSeed(t, engine)

	ack := relatedAckedCompositeUpdate(t, engine)
	require.True(t, ack.AcknowledgedBuffered())
	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, true, "changed", "changed"))

	require.NoError(t, engine.FlushWriteBehind(t.Context()))
	require.Zero(t, engine.writeBehind.PendingOps(), "the acknowledged update must have drained")

	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, true, "changed", "changed"),
		"the drained generation's committed postings are the only membership, listed once")
	require.Equal(t, []NodeID{"test:doc"}, relatedAckedCompositeLookup(t, sm, idx, false, "changed"))
	require.Empty(t, relatedAckedCompositeLookup(t, sm, idx, true, "before", "stable"),
		"the replayed rename removed the previous committed posting")
}

func TestRelatedCompositeLookupFullRequiresEveryProperty(t *testing.T) {
	engine, sm, idx := relatedAckedCompositeEngine(t)
	bufferedCreate(t, engine, &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	ids, err := sm.CompositeIndexLookup(t.Context(), idx, []any{"t"}, true)
	require.NoError(t, err)
	require.Empty(t, ids, "an incomplete full key must not become a pending prefix match")
	ids, err = sm.CompositeIndexLookup(t.Context(), idx, []any{"t"}, false)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:doc"}, ids)
}
