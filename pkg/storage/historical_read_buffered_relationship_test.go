package storage_test

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipAckFixture struct {
	base *storage.BadgerEngine
	eng  storage.Engine
	ns   string
}

// newRelationshipAckFixture builds the fixture with the requested retained-version
// budget and, when buffered is set, the engine's documented write-behind
// acknowledgement path (buffered commits, background flush).
func newRelationshipAckFixture(t *testing.T, retentionVersions int, buffered bool) *relationshipAckFixture {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	baseDir := t.TempDir()
	walDir := t.TempDir()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             baseDir,
		WriteBehind:         buffered,
		WriteBehindInterval: time.Hour,
		EngineOptions:       storage.EngineOptions{RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: retentionVersions}},
	}))
	require.NoError(t, err)
	if buffered {
		t.Cleanup(func() { _ = base.FlushWriteBehind(context.Background()) })
	}
	wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	manager, err := multidb.NewDatabaseManager(context.Background(), storage.NewWALEngine(base, wal), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	name := manager.DefaultDatabaseName()
	eng, err := manager.GetStorage(name)
	require.NoError(t, err)
	return &relationshipAckFixture{base: base, eng: eng, ns: name}
}

// qualified is the stored ID the native store and the historical view use. The
// namespace-scoped engine accepts the bare ID; OpenHistoricalRead pins the
// native store and its scope filter, so the tests name both forms explicitly
// instead of relying on an implicit transport.
func (f *relationshipAckFixture) qualified(id storage.NodeID) storage.NodeID {
	return storage.NodeID(f.ns + ":" + string(id))
}

func TestHistoricalAckCutoffBufferedRelationship(t *testing.T) {
	ctx := t.Context()
	f := newRelationshipAckFixture(t, 100, true)
	const start storage.NodeID = "buffered-rel-start"
	const end storage.NodeID = "buffered-rel-end"
	for _, id := range []storage.NodeID{start, end} {
		_, err := f.eng.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"HistoricalEndpoint"}})
		require.NoError(t, err)
	}
	require.NoError(t, f.base.FlushWriteBehind(ctx))
	const bareID storage.EdgeID = "buffered-rel"
	id := storage.EdgeID(f.ns + ":" + string(bareID))
	tx, err := f.base.BeginTransaction(ctx, storage.OrdinaryBegin)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace(f.ns))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.CreateEdge(ctx, &storage.Edge{
		ID: id, StartNode: f.qualified(start), EndNode: f.qualified(end), Type: "KNOWS",
		Properties: map[string]any{"status": "acknowledged"},
	}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	cutoff := time.Now().UTC()

	live, err := f.eng.GetEdge(ctx, bareID)
	require.NoError(t, err)
	require.Equal(t, "acknowledged", live.Properties["status"])

	check := func(t *testing.T) {
		view, err := storage.OpenHistoricalRead(ctx, f.eng, cutoff)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, view.Close()) })
		t.Run("point", func(t *testing.T) {
			result, err := view.GetEdge(ctx, id)
			require.NoError(t, err)
			require.Equal(t, "acknowledged", result.Properties["status"])
		})
		t.Run("type_scan", func(t *testing.T) {
			results, err := view.GetEdgesByType(ctx, "KNOWS")
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, id, results[0].ID)
		})
		t.Run("adjacency", func(t *testing.T) {
			link, err := view.GetEdgeBetween(ctx, f.qualified(start), f.qualified(end), "KNOWS")
			require.NoError(t, err)
			require.Equal(t, id, link.ID)
			degree, err := view.GetInDegree(ctx, f.qualified(end))
			require.NoError(t, err)
			require.Equal(t, 1, degree)
			count, err := view.EdgeCountByStartLabel(ctx, "HistoricalEndpoint", "KNOWS")
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
		})
	}
	t.Run("before_flush", check)
	require.NoError(t, f.base.FlushWriteBehind(ctx))
	t.Run("after_flush", check)
}
