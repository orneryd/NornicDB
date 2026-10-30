package storage

import (
	"errors"
	"math"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestPR1655StorageUniqueClaimConflict(t *testing.T) {
	for _, tc := range []struct {
		name         string
		before       interface{}
		after        interface{}
		wantUnique   bool
		intermediate interface{}
	}{
		{"unchanged_string", "original", "original", false, nil},
		{"unchanged_number", int64(7), float64(7), false, nil},
		{"unchanged_list", []interface{}{int64(7), "a"}, []interface{}{int64(7), "a"}, false, nil},
		{"changed_key", "original", "claimed", true, nil},
		{"added_key", nil, "claimed", true, nil},
		{"restored_key", "original", "original", false, "temporary"},
		{"repeated_new_key", "original", "claimed", true, "temporary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddConstraint(Constraint{
				Name: "doc_id", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"},
			}))
			// The engine is empty: this mirrors the complete cache published
			// by CREATE CONSTRAINT, which the Execute/Bolt tests use.
			constraint := schema.uniqueConstraints["Doc:id"]
			constraint.mu.Lock()
			constraint.valuesCacheComplete = true
			constraint.mu.Unlock()
			nodeID, err := engine.CreateNode(&Node{
				ID: "test:doc", Labels: []string{"Doc"},
				Properties: map[string]interface{}{"id": tc.before, "summary_input": "before"},
			})
			require.NoError(t, err)
			loser, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = loser.Rollback() })
			peer, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = peer.Rollback() })
			for i, tx := range []*BadgerTransaction{loser, peer} {
				node, err := tx.GetNode(nodeID)
				require.NoError(t, err)
				if tc.intermediate != nil {
					node.Properties["id"] = tc.intermediate
					require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
				}
				node.Properties["id"] = tc.after
				node.Properties["summary_input"] = []string{"loser", "peer"}[i]
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
			}
			require.NoError(t, peer.Commit())
			err = loser.Commit()
			require.ErrorIs(t, err, ErrConflict)
			var violation *ConstraintViolationError
			require.Equal(t, tc.wantUnique, errors.As(err, &violation), "commit error: %v", err)
			if tc.wantUnique {
				require.Equal(t, ConstraintUnique, violation.Type)
				require.Contains(t, violation.Message, "claimed by concurrent commit")
			} else {
				require.Contains(t, err.Error(), "changed after transaction start")
			}
			stored, err := engine.GetNode(nodeID)
			require.NoError(t, err)
			require.Equal(t, "peer", stored.Properties["summary_input"])
		})
	}
}

func TestPR1655UncontendedUniqueClaimIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  interface{}
	}{
		{"nan", math.NaN()},
		{"list_nan", []interface{}{math.NaN(), "key"}},
		{"zoned_time", time.Date(2026, 10, 30, 10, 15, 0, 0, time.FixedZone("test-zone", -7*60*60))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			schema := engine.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			id, err := engine.CreateNode(&Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": tc.key}})
			require.NoError(t, err)
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			node, err := tx.GetNode(id)
			require.NoError(t, err)
			node.Properties["note"] = "changed"
			require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
			require.NoError(t, tx.Commit(), "an unchanged claim without a peer must not conflict")
			stored, err := engine.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, "changed", stored.Properties["note"])
		})
	}
}
