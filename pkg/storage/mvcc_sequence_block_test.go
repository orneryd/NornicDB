package storage

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestMVCCSequenceBlockSurvivesRetainedTombstone(t *testing.T) {
	checkMVCCSequenceBlockAfterDelete(t, false)
}

func TestMVCCSequenceBlockSurvivesPrunedTombstone(t *testing.T) {
	checkMVCCSequenceBlockAfterDelete(t, true)
}

func checkMVCCSequenceBlockAfterDelete(t *testing.T, prune bool) {
	t.Helper()
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	id := NodeID("test:only-node")
	_, err = engine.CreateNode(t.Context(), &Node{ID: id})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode(t.Context(), id, CascadeEdges))
	require.NoError(t, tx.Commit())
	deleted, err := engine.loadNodeMVCCHead(id)
	require.NoError(t, err)
	require.True(t, deleted.Tombstoned)
	if prune {
		count, err := engine.PruneMVCCVersions(t.Context(), MVCCPruneOptions{})
		require.NoError(t, err)
		require.Positive(t, count)
		_, err = engine.loadNodeMVCCHead(id)
		require.ErrorIs(t, err, ErrNotFound)
	}
	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	tombstone, err := reopened.loadNodeMVCCHead(id)
	if prune {
		require.ErrorIs(t, err, ErrNotFound)
	} else {
		require.NoError(t, err)
		require.Equal(t, deleted, tombstone)
	}
	require.NoError(t, reopened.EnsureNamespaceMVCC("test"))
	state, err := reopened.namespaceMVCC("test")
	require.NoError(t, err)
	t.Logf("durable tombstone=%s recovered sequence=%d", deleted.Version, state.seq.Load())
	require.Equal(t, uint64(mvccSequenceBlockSize), state.seq.Load())
	_, err = reopened.CreateNode(t.Context(), &Node{ID: id})
	require.NoError(t, err)
	created, err := reopened.loadNodeMVCCHead(id)
	require.NoError(t, err)
	require.Equal(t, uint64(mvccSequenceBlockSize+1), created.Version.CommitSequence)
}

func TestMVCCSequenceBlockSurvivesProcessExit(t *testing.T) {
	const crashDir = "NORNICDB_TEST_MVCC_CRASH_DIR"
	const crashCode = 37
	if dir := os.Getenv(crashDir); dir != "" {
		engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
		require.NoError(t, err)
		// Simulate exhaustion and exit after the next reservation, before
		// the user transaction commits or the engine closes.
		state, err := engine.namespaceMVCC("test")
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: "test:before-crash"})
		require.NoError(t, err)
		state.seq.Store(mvccSequenceBlockSize)
		tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
		require.NoError(t, err)
		version, err := engine.allocateMVCCVersion(t.Context(), tx.badgerTx, "test", tx.StartTime)
		require.NoError(t, err)
		require.Equal(t, uint64(mvccSequenceBlockSize+1), version.CommitSequence)
		os.Exit(crashCode)
	}

	dir := t.TempDir()
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMVCCSequenceBlockSurvivesProcessExit$")
	child.Env = append(os.Environ(), crashDir+"="+dir)
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%s", output)
	require.Equal(t, crashCode, exit.ExitCode(), "%s", output)

	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:after-crash"})
	require.NoError(t, err)
	head, err := engine.loadNodeMVCCHead("test:after-crash")
	require.NoError(t, err)
	require.Equal(t, uint64(2*mvccSequenceBlockSize+1), head.Version.CommitSequence)
}

func TestMVCCSequenceBlockRefillsAfterNamespaceDrop(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:before"})
	require.NoError(t, err)
	_, _, err = engine.DeleteByPrefix("test:")
	require.NoError(t, err)
	ceiling, err := engine.loadPersistedNamespaceSequence("test")
	require.NoError(t, err)
	require.Zero(t, ceiling)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:after"})
	require.NoError(t, err)
	ceiling, err = engine.loadPersistedNamespaceSequence("test")
	require.NoError(t, err)
	require.Greater(t, ceiling, uint64(mvccSequenceBlockSize))
}

func TestMVCCSequenceBlockWithholdsVersionsUntilDirectorySync(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "store")
	if err := os.Symlink(dir, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(link))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	state, err := engine.namespaceMVCC("test")
	require.NoError(t, err)
	// The active WAL stays open. Hiding its directory path makes the
	// durability step fail after Badger has written the reservation.
	require.NoError(t, os.Remove(link))
	t.Cleanup(func() {
		if _, err := os.Lstat(link); os.IsNotExist(err) {
			require.NoError(t, os.Symlink(dir, link))
		}
	})
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:retry"})
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, "reserve durable MVCC sequence block")
	require.Zero(t, state.seq.Load())
	require.Zero(t, state.reservedThrough.Load())
	require.NoError(t, os.Symlink(dir, link))
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:retry"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.seq.Load())
}
