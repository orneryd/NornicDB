package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCatalogV4TransformsPendingWALImage(t *testing.T) {
	for _, version := range []int{storageVersionPropKeyDictV2, storageVersionEdgeAdjacencyV3} {
		t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
			testCatalogV4TransformsPendingWALImage(t, version)
		})
	}
}

func testCatalogV4TransformsPendingWALImage(t *testing.T, version int) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "system:databases:metadata", Properties: map[string]any{"data": "old"}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(nil))
	engine.writeBehind.Close()
	engine.writeBehind = NewWriteBehindBuffer(time.Hour, 1000000, engine.replayWriteBehind)
	require.NoError(t, engine.writeSchemaVersion(version))
	engine.storageVersion = version
	walDir := filepath.Join(dir, "wal")
	wal, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	engine.SetBufferedCommitWAL(wal)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("system"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "system:databases:metadata", Properties: map[string]any{"data": "pending"}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	// The first fixture must be torn down the way the process crash left it:
	// no write-behind drain, no second engine lifetime owner at cleanup.
	crashClose(t, engine, wal)
	var observed []string
	plan := &CatalogUpgradeV4{NodeID: "system:databases:metadata", Transform: func(_ context.Context, data string) (string, bool, error) {
		observed = append(observed, data)
		return "sealed:" + data, true, nil
	}}
	engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, AllowStorageUpgrade: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	require.Equal(t, []string{"pending"}, observed, "V4 must transform the acknowledged image, not stale Badger data")
	node, err := engine.GetNode("system:databases:metadata")
	require.NoError(t, err)
	require.Equal(t, "sealed:pending", node.Properties["data"])
	wal, err = NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	defer wal.Close()
	replayed, err := RecoverBufferedCommitsAtOpen(engine, wal)
	require.NoError(t, err)
	require.Zero(t, replayed, "the pending write was settled before the transform")
	node, err = engine.GetNode("system:databases:metadata")
	require.NoError(t, err)
	require.Equal(t, "sealed:pending", node.Properties["data"])
}

func TestCatalogV4ResumesPendingReplayAfterTransformFailure(t *testing.T) {
	for _, version := range []int{storageVersionPropKeyDictV2, storageVersionEdgeAdjacencyV3, storageVersionCatalogV4} {
		t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
			testCatalogV4ResumesPendingReplayAfterTransformFailure(t, version)
		})
	}
}

func testCatalogV4ResumesPendingReplayAfterTransformFailure(t *testing.T, version int) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "system:databases:metadata", Properties: map[string]any{"data": "old"}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(nil))
	engine.writeBehind.Close()
	engine.writeBehind = NewWriteBehindBuffer(time.Hour, 1000000, engine.replayWriteBehind)
	capture, err := NewWAL(filepath.Join(dir, "capture"), DefaultWALConfig())
	require.NoError(t, err)
	engine.SetBufferedCommitWAL(capture)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("system"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "system:databases:metadata", Properties: map[string]any{"data": "pending"}}, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	entry := entries[0]
	entry.PropertyEpoch = nil
	old := priorWALRecord(t, 3, entry)
	require.NoError(t, engine.writeSchemaVersion(version))
	// The first fixture must be torn down the way the process crash left it:
	// no write-behind drain, no second engine lifetime owner at cleanup.
	crashClose(t, engine, capture)
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), old, 0600))
	injected := errors.New("transform temporarily unavailable")
	var seen []string
	plan := &CatalogUpgradeV4{NodeID: "system:databases:metadata", Transform: func(_ context.Context, data string) (string, bool, error) {
		seen = append(seen, data)
		return "", false, injected
	}}
	failed, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, AllowStorageUpgrade: true, CatalogUpgradeV4: plan}))
	require.ErrorIs(t, err, injected)
	if failed != nil {
		// A failed open that still returned an engine is registered, not left
		// open. Its cleanup stops the flusher without draining so the recovery
		// state this test reopens next is preserved.
		buffered := failed.writeBehind
		t.Cleanup(func() {
			if buffered != nil {
				buffered.Close()
			}
			_ = failed.Close()
		})
	}
	require.Nil(t, failed, "a failed upgrade must not expose an open engine")
	require.Equal(t, []string{"pending"}, seen)
	plan.Transform = func(_ context.Context, data string) (string, bool, error) {
		seen = append(seen, data)
		return "sealed:" + data, true, nil
	}
	// The previously authorized recovery marker is enough to finish after a
	// crash or transform failure; replay receipts prevent applying it twice.
	engine, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	require.Equal(t, []string{"pending", "pending"}, seen)
	node, err := engine.GetNode("system:databases:metadata")
	require.NoError(t, err)
	require.Equal(t, "sealed:pending", node.Properties["data"])
	pending, formatPending, err := engine.v4WALRecoveryState()
	require.NoError(t, err)
	require.False(t, pending)
	require.False(t, formatPending)
	// The explicit close below is the lifetime boundary this fixture is
	// inspected through, so it is registered without a second owner.
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, CatalogUpgradeV4: plan}))
	require.NoError(t, err)
	t.Cleanup(func() {
		if buffered := engine.writeBehind; buffered != nil {
			buffered.Close()
		}
		_ = engine.Close()
	})
	node, err = engine.GetNode("system:databases:metadata")
	require.NoError(t, err)
	require.Equal(t, "sealed:pending", node.Properties["data"])
	require.Len(t, seen, 2, "a completed transform must not run twice")
}
