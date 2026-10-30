package storagefault_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/auth"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// One decorator over a real engine: only the selected operation fails, only on
// the selected call of it, and a failure inside a staged transaction leaves
// every write before it uncommitted.
func TestMutationFailureEngineSelectsOneCallAndRollsBackStagedWrites(t *testing.T) {
	ctx := context.Background()
	real := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	system := storage.NewNamespacedEngine(real, "system")
	injected := errors.New("injected mutation failure")
	node := func(id string) *storage.Node {
		return &storage.Node{ID: storage.NodeID(id), Labels: []string{"_Test"}, Properties: map[string]any{"id": id}}
	}

	fixture := storagefault.NewMutationFailureEngine(system, storagefault.MutationCreate, injected, 2, auth.WithStoreTransaction)
	_, err := fixture.CreateNode(node("first"))
	require.NoError(t, err)
	_, err = fixture.CreateNode(node("second"))
	require.ErrorIs(t, err, injected, "the second create of the selection fails")
	_, err = fixture.CreateNode(node("third"))
	require.NoError(t, err, "later creates of the selection pass through")
	created, err := system.AllNodes()
	require.NoError(t, err)
	require.Len(t, created, 2)

	fixture.Select(storagefault.MutationGet, "first", 0)
	_, err = fixture.GetNode("first")
	require.ErrorIs(t, err, injected)
	nodeRead, err := fixture.GetNode("third")
	require.NoError(t, err, "an unselected ID is not a failure")
	require.Equal(t, storage.NodeID("third"), nodeRead.ID, "the real engine's read passes through")

	fixture.Select(storagefault.MutationScan, "", 0)
	_, err = fixture.GetNodesByLabel("_Test")
	require.ErrorIs(t, err, injected)
	fixture.Select(storagefault.MutationList, "", 0)
	_, err = fixture.AllNodes()
	require.ErrorIs(t, err, injected)
	fixture.Select("", "", 0)
	listed, err := fixture.AllNodes()
	require.NoError(t, err)
	require.Len(t, listed, 2, "the empty operation injects nothing")

	var staged int
	err = fixture.WithStoreTransaction(ctx, func(view storage.Engine) error {
		for _, id := range []string{"staged-a", "staged-b"} {
			if _, err := view.CreateNode(node(id)); err != nil {
				return err
			}
			staged++
		}
		return injected
	})
	require.ErrorIs(t, err, injected)
	require.Equal(t, 2, staged, "writes before the failure reach the real transaction")
	surviving, err := system.AllNodes()
	require.NoError(t, err)
	require.Len(t, surviving, 2, "a failed transaction commits nothing")

	fixture.Select(storagefault.MutationUpdate, "first", 0)
	err = fixture.WithStoreTransaction(ctx, func(view storage.Engine) error {
		if _, err := view.CreateNode(node("staged")); err != nil {
			return err
		}
		after := node("first")
		after.Properties["changed"] = true
		return view.UpdateNode(storage.NodeUpdateRequest{Before: node("first"), After: after})
	})
	require.ErrorIs(t, err, injected, "the selected update fails inside the real transaction")
	stored, err := system.GetNode("first")
	require.NoError(t, err)
	require.NotContains(t, stored.Properties, "changed", "the update before the failure rolled back")
	_, err = system.GetNode("staged")
	require.ErrorIs(t, err, storage.ErrNotFound)

	fixture.Select(storagefault.MutationDelete, "third", 0)
	err = fixture.WithStoreTransaction(ctx, func(view storage.Engine) error {
		return view.DeleteNode("third")
	})
	require.ErrorIs(t, err, injected)
	_, err = system.GetNode("third")
	require.NoError(t, err, "the selected delete rolled back")
}
