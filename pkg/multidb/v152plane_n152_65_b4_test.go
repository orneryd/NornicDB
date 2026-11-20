package multidb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-210 reproduction-only audit: the supported manager DROP consumer over the
// actual Badger storage owner. A manager DROP runs the same DeleteByPrefix that
// clears the relationship-type cache, so the audit checks that the unrelated
// database keeps valid relationship facts across drop, drop/recreate and a
// failed (fenced) drop, and that the failure is completed by the same owner.

// managerDropPurgeFault rejects the physical namespace purge once while leaving
// the native engine (catalog writes, Sync, accepted work) intact.
type managerDropPurgeFault struct {
	*storage.BadgerEngine
	fail atomic.Bool
}

var errManagerDropPurgeUnavailable = errors.New("DROP purge unavailable")

func (e *managerDropPurgeFault) GetInnerEngine() storage.Engine { return e.BadgerEngine }

func (e *managerDropPurgeFault) DeleteByPrefix(prefix string) (int64, int64, error) {
	if e.fail.Load() {
		return 0, 0, errManagerDropPurgeUnavailable
	}
	return e.BadgerEngine.DeleteByPrefix(prefix)
}

func newManagerDropRelationshipFixture(t *testing.T) (*DatabaseManager, *storage.BadgerEngine) {
	t.Helper()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:   t.TempDir(),
		LowMemory: true,
	}))
	require.NoError(t, err)
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	return manager, base
}

func seedManagerRelationship(t *testing.T, view storage.Engine, edgeType, edgeName string, edges int) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []storage.NodeID{"start", "end"} {
		_, err := view.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	for i := 0; i < edges; i++ {
		require.NoError(t, view.CreateEdge(ctx, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("%s-%d", edgeName, i)),
			StartNode: "start",
			EndNode:   "end",
			Type:      edgeType,
		}))
	}
}

func warmManagerRelationship(t *testing.T, view storage.Engine, edgeType string, want int) {
	t.Helper()
	edges, err := view.GetEdgesByType(context.Background(), edgeType)
	require.NoError(t, err)
	require.Len(t, edges, want)
}

func managerRelationshipIDs(edges []*storage.Edge) []storage.EdgeID {
	ids := make([]storage.EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}

// TestManagerDropDatabaseKeepsUnrelatedRelationshipFacts warms two databases'
// relationship types through the manager's own views, drops one database, and
// asserts the unrelated database still reads its relationships from the actual
// storage owner.
func TestManagerDropDatabaseKeepsUnrelatedRelationshipFacts(t *testing.T) {
	manager, base := newManagerDropRelationshipFixture(t)
	ctx := context.Background()
	for _, name := range []string{"victim", "peer"} {
		require.NoError(t, manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: name}))
	}
	victim, err := manager.GetStorage("victim")
	require.NoError(t, err)
	peer, err := manager.GetStorage("peer")
	require.NoError(t, err)

	seedManagerRelationship(t, victim, "VICTIM_REL", "edge", 2)
	seedManagerRelationship(t, peer, "PEER_REL", "edge", 2)
	warmManagerRelationship(t, victim, "VICTIM_REL", 2)
	warmManagerRelationship(t, peer, "PEER_REL", 2)

	require.NoError(t, manager.DropDatabase(ctx, "victim"))
	require.False(t, manager.Exists("victim"))
	_, err = manager.GetStorage("victim")
	require.ErrorIs(t, err, ErrDatabaseNotFound)

	kept, err := peer.GetEdgesByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"edge-0", "edge-1"}, managerRelationshipIDs(kept), "unrelated relationship facts are retained")
	_, err = peer.GetNode(ctx, "start")
	require.NoError(t, err)

	all, err := base.GetEdgesByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"peer:edge-0", "peer:edge-1"}, managerRelationshipIDs(all))
	count, err := base.EdgeCountByTypeInNamespace(ctx, "peer", "PEER_REL")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = base.EdgeCountByTypeInNamespace(ctx, "victim", "VICTIM_REL")
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = base.EdgeCountByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = base.EdgeCountByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestManagerDropDatabaseRecreateServesFreshRelationshipFacts covers the
// drop/recreate consumer path: the previous generation's relationships never
// reappear and the unrelated database keeps its facts.
func TestManagerDropDatabaseRecreateServesFreshRelationshipFacts(t *testing.T) {
	manager, base := newManagerDropRelationshipFixture(t)
	ctx := context.Background()
	for _, name := range []string{"victim", "peer"} {
		require.NoError(t, manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: name}))
	}
	victim, err := manager.GetStorage("victim")
	require.NoError(t, err)
	peer, err := manager.GetStorage("peer")
	require.NoError(t, err)
	seedManagerRelationship(t, victim, "VICTIM_REL", "old", 2)
	seedManagerRelationship(t, peer, "PEER_REL", "edge", 2)
	warmManagerRelationship(t, victim, "VICTIM_REL", 2)
	warmManagerRelationship(t, peer, "PEER_REL", 2)

	require.NoError(t, manager.DropDatabase(ctx, "victim"))
	require.NoError(t, manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: "victim"}))
	recreated, err := manager.GetStorage("victim")
	require.NoError(t, err)
	seedManagerRelationship(t, recreated, "VICTIM_REL", "fresh", 2)

	fresh, err := recreated.GetEdgesByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"fresh-0", "fresh-1"}, managerRelationshipIDs(fresh))
	kept, err := peer.GetEdgesByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"edge-0", "edge-1"}, managerRelationshipIDs(kept))
	count, err := base.EdgeCountByTypeInNamespace(ctx, "victim", "VICTIM_REL")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	all, err := base.GetEdgesByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"victim:fresh-0", "victim:fresh-1"}, managerRelationshipIDs(all), "the dropped generation never reappears")
}

// TestManagerDropDatabasePurgeFailureKeepsUnrelatedRelationshipFacts interrupts
// the DROP at the supported manager boundary (the physical purge). The owner
// reports an accepted pending outcome and keeps the name fenced; the unrelated
// database keeps reading its relationships, the victim rows are untouched, and
// the same owner completes the retry.
func TestManagerDropDatabasePurgeFailureKeepsUnrelatedRelationshipFacts(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:   t.TempDir(),
		LowMemory: true,
	}))
	require.NoError(t, err)
	faults := &managerDropPurgeFault{BadgerEngine: base}
	manager, err := NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	ctx := context.Background()
	for _, name := range []string{"victim", "peer"} {
		require.NoError(t, manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: name}))
	}
	victim, err := manager.GetStorage("victim")
	require.NoError(t, err)
	peer, err := manager.GetStorage("peer")
	require.NoError(t, err)
	seedManagerRelationship(t, victim, "VICTIM_REL", "edge", 2)
	seedManagerRelationship(t, peer, "PEER_REL", "edge", 2)
	warmManagerRelationship(t, victim, "VICTIM_REL", 2)
	warmManagerRelationship(t, peer, "PEER_REL", 2)

	faults.fail.Store(true)
	err = manager.DropDatabase(ctx, "victim")
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.ErrorIs(t, accepted.Err, errManagerDropPurgeUnavailable)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	require.True(t, manager.dropFenceHeld("victim"), "the failed purge keeps the unavailable-name fence")
	require.True(t, manager.Exists("victim"))
	_, err = manager.GetStorage("victim")
	require.ErrorIs(t, err, ErrDatabaseOffline)

	// Unrelated facts stay available while the victim DROP is unfinished. The
	// purge never reached storage, so the victim rows also remain.
	kept, err := peer.GetEdgesByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"edge-0", "edge-1"}, managerRelationshipIDs(kept))
	count, err := base.EdgeCountByTypeInNamespace(ctx, "peer", "PEER_REL")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	unstripped, err := base.GetEdgesByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"victim:edge-0", "victim:edge-1"}, managerRelationshipIDs(unstripped))

	// The same DROP owner finishes the retry.
	faults.fail.Store(false)
	require.NoError(t, manager.DropDatabase(ctx, "victim"))
	require.False(t, manager.dropFenceHeld("victim"))
	require.False(t, manager.Exists("victim"))
	kept, err = peer.GetEdgesByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.ElementsMatch(t, []storage.EdgeID{"edge-0", "edge-1"}, managerRelationshipIDs(kept))
	unstripped, err = base.GetEdgesByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.Empty(t, unstripped)
	count, err = base.EdgeCountByType(ctx, "PEER_REL")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = base.EdgeCountByType(ctx, "VICTIM_REL")
	require.NoError(t, err)
	require.Zero(t, count)
}
