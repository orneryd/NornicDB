package storage

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// An entity created with the same ID as one a rolled-back transaction
// created, and committed, survives a reopen: the rollback left the ID's
// dictionary entry in memory only, and the committed transaction writes its
// keys (#907, I29: a MERGEd relationship's ID is derived from its endpoints
// and properties, so a retry reuses it).
func TestEntityRecreatedAfterRolledBackCreateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	inTx := func(commit bool) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		_, err = tx.CreateNode(&Node{ID: "test:fixed", Labels: []string{"Fixed"}})
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:merge-x", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"id": "new"}}))
		if commit {
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
		}
	}
	inTx(false)
	inTx(true)
	require.NoError(t, engine.Close())

	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	edge, err := engine.GetEdge("test:merge-x")
	require.NoError(t, err)
	require.Equal(t, "new", edge.Properties["id"])
	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("test:merge-x"), outgoing[0].ID)
	_, err = engine.GetNode("test:fixed")
	require.NoError(t, err)
	fixed, err := engine.GetNodesByLabel("Fixed")
	require.NoError(t, err)
	require.Len(t, fixed, 1)
}
