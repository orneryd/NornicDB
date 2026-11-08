package search

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type checkpointLifetimeSnapshot struct {
	storage.ReadSnapshotView
	closed atomic.Bool
}

func (s *checkpointLifetimeSnapshot) Close() { s.closed.Store(true); s.ReadSnapshotView.Close() }
func (s *checkpointLifetimeSnapshot) IndexSourceStamp(ctx context.Context) (string, error) {
	return storage.ReadIndexSourceStamp(ctx, s.ReadSnapshotView)
}

type checkpointLifetimeEngine struct {
	*storage.BadgerEngine
	captured *checkpointLifetimeSnapshot
	compared atomic.Bool
}

func (e *checkpointLifetimeEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.BadgerEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	e.captured = &checkpointLifetimeSnapshot{ReadSnapshotView: inner}
	return e.captured, nil
}
func (e *checkpointLifetimeEngine) IndexSourceStamp(ctx context.Context) (string, error) {
	if e.captured != nil {
		if e.captured.closed.Load() {
			return "", errors.New("population snapshot released before source comparison")
		}
		e.compared.Store(true)
	}
	return e.BadgerEngine.IndexSourceStamp(ctx)
}

func TestCheckpointSourceComparisonRetainsPopulationSnapshot(t *testing.T) {
	dir := t.TempDir()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), storage.CopyNode(compatStoreNodes[0]))
	require.NoError(t, err)
	engine := &checkpointLifetimeEngine{BadgerEngine: base}
	svc := compatStoreService(engine, dir)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(t.Context()))
	require.True(t, engine.compared.Load(), "public build must compare the completed population source")
	require.True(t, engine.captured.closed.Load(), "public build must release its pinned source")
}
