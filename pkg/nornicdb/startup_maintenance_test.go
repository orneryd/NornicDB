package nornicdb

import (
	"context"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type startupMaintenanceEngine struct {
	storage.Engine
	clean              bool
	consumeErr         error
	temporalErr        error
	mvccErr            error
	flushErr           error
	markErr            error
	consumeCalls       int
	temporalCalls      int
	mvccCalls          int
	markCalls          int
	shutdownOperations []string
}

func (e *startupMaintenanceEngine) ConsumeCleanShutdownMarker(context.Context) (bool, error) {
	e.consumeCalls++
	return e.clean, e.consumeErr
}

func (e *startupMaintenanceEngine) MarkCleanShutdown(context.Context) error {
	e.markCalls++
	e.shutdownOperations = append(e.shutdownOperations, "mark")
	return e.markErr
}

func (e *startupMaintenanceEngine) RebuildTemporalIndexes(context.Context) error {
	e.temporalCalls++
	return e.temporalErr
}

func (e *startupMaintenanceEngine) PruneTemporalHistory(context.Context, storage.TemporalPruneOptions) (int64, error) {
	return 0, nil
}

func (e *startupMaintenanceEngine) RebuildMVCCHeads(context.Context) error {
	e.mvccCalls++
	return e.mvccErr
}

func (e *startupMaintenanceEngine) PruneMVCCVersions(context.Context, storage.MVCCPruneOptions) (int64, error) {
	return 0, nil
}

func (e *startupMaintenanceEngine) Flush() error {
	e.shutdownOperations = append(e.shutdownOperations, "flush")
	return e.flushErr
}

func TestStartupSkipsDerivedIndexRebuildAfterCleanShutdown(t *testing.T) {
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), clean: true}

	require.True(t, prepareDerivedIndexes(context.Background(), engine))
	require.Equal(t, 1, engine.consumeCalls)
	require.Zero(t, engine.temporalCalls)
	require.Zero(t, engine.mvccCalls)
}

func TestStartupRebuildsDerivedIndexesWithoutCleanShutdown(t *testing.T) {
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}

	require.True(t, prepareDerivedIndexes(context.Background(), engine))
	require.Equal(t, 1, engine.consumeCalls)
	require.Equal(t, 1, engine.temporalCalls)
	require.Equal(t, 1, engine.mvccCalls)
}

func TestStartupRebuildFailurePreventsCleanShutdownMarker(t *testing.T) {
	rebuildErr := errors.New("rebuild failed")
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), temporalErr: rebuildErr}

	healthy := prepareDerivedIndexes(context.Background(), engine)
	require.False(t, healthy)
	require.NoError(t, markStorageCleanShutdown(t.Context(), engine, healthy))
	require.Zero(t, engine.markCalls)
}

func TestStartupRebuildsAfterMarkerCheckFailure(t *testing.T) {
	markerErr := errors.New("marker unavailable")
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), consumeErr: markerErr}

	require.True(t, prepareDerivedIndexes(context.Background(), engine))
	require.Equal(t, 1, engine.temporalCalls)
	require.Equal(t, 1, engine.mvccCalls)
}

func TestStartupMVCCRebuildFailurePreventsCleanShutdownMarker(t *testing.T) {
	rebuildErr := errors.New("mvcc rebuild failed")
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), mvccErr: rebuildErr}

	require.False(t, prepareDerivedIndexes(context.Background(), engine))
	require.NoError(t, markStorageCleanShutdown(t.Context(), engine, false))
	require.Zero(t, engine.markCalls)
}

func TestCleanShutdownFlushesWritesBeforeRecordingMarker(t *testing.T) {
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}

	require.NoError(t, markStorageCleanShutdown(t.Context(), engine, true))
	require.Equal(t, []string{"flush", "mark"}, engine.shutdownOperations)
}

func TestDerivedIndexPersistenceStartsAfterCleanStorageMarker(t *testing.T) {
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}

	require.NoError(t, persistDerivedAfterStorageBoundary(t.Context(), engine, true, func() {
		engine.shutdownOperations = append(engine.shutdownOperations, "persist-search")
	}))
	require.Equal(t, []string{"flush", "mark", "persist-search"}, engine.shutdownOperations)
}

func TestCleanShutdownDoesNotRecordMarkerAfterFlushFailure(t *testing.T) {
	flushErr := errors.New("flush failed")
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), flushErr: flushErr}

	require.ErrorIs(t, markStorageCleanShutdown(t.Context(), engine, true), flushErr)
	require.Equal(t, []string{"flush"}, engine.shutdownOperations)
	require.Zero(t, engine.markCalls)
}

func TestCleanShutdownReturnsMarkerFailure(t *testing.T) {
	markerErr := errors.New("marker failed")
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), markErr: markerErr}

	require.ErrorIs(t, markStorageCleanShutdown(t.Context(), engine, true), markerErr)
	require.Equal(t, []string{"flush", "mark"}, engine.shutdownOperations)
}

func TestCleanShutdownIgnoresUnsupportedMarker(t *testing.T) {
	engine := &startupMaintenanceEngine{Engine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), markErr: storage.ErrNotImplemented}

	require.NoError(t, markStorageCleanShutdown(t.Context(), engine, true))
}

func TestCleanShutdownWithoutMarkerCapabilityIsNoOp(t *testing.T) {
	require.NoError(t, markStorageCleanShutdown(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), true))
}

func BenchmarkStartupDerivedIndexPreparation(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	for offset := 0; offset < 2000; offset += 250 {
		nodes := make([]*storage.Node, 0, 250)
		for index := offset; index < offset+250; index++ {
			nodes = append(nodes, &storage.Node{
				ID:         storage.NodeID(fmt.Sprintf("nornic:startup-bench-%d", index)),
				Labels:     []string{"Document"},
				Properties: map[string]any{"index": index},
			})
		}
		require.NoError(b, engine.BulkCreateNodes(nodes))
	}

	b.Run("unclean_rebuild", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := engine.RebuildTemporalIndexes(context.Background()); err != nil {
				b.Fatal(err)
			}
			if err := engine.RebuildMVCCHeads(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("clean_marker", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := engine.MarkCleanShutdown(context.Background()); err != nil {
				b.Fatal(err)
			}
			clean, err := engine.ConsumeCleanShutdownMarker(context.Background())
			if err != nil || !clean {
				b.Fatalf("consume clean marker: clean=%v err=%v", clean, err)
			}
		}
	})
}
