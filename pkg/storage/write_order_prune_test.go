package storage

// Author: L4 fixture ownership migration (Codex), 1 November 2026.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPruneOrdersAfterBufferedRecreate: a node deleted (its head a tombstone)
// and created again by a commit the write-behind buffer acknowledged but has
// not applied. A prune that reclaims tombstoned entities (their head, ID and
// dictionary entry) orders after that commit: it must not reclaim the node
// under it, and the node reads back as created again, also from storage.
func TestPruneOrdersAfterBufferedRecreate(t *testing.T) {
	dir := t.TempDir()
	b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	var closeB func()
	if b != nil {
		_, closeB = runtimeFixtureOwner.OwnWithClose(t, b)
	}
	require.NoError(t, err)

	_, err = b.CreateNode(&Node{ID: "test:p", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind(nil))
	require.NoError(t, b.DeleteNode("test:p"))
	require.NoError(t, b.FlushWriteBehind(nil))

	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "test:p", Properties: map[string]any{"v": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the create is acknowledged, not yet applied")

	_, err = b.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind(nil))

	got, err := b.GetNode("test:p")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
	b.invalidateCachesAfterRestore()
	got, err = b.GetNode("test:p")
	require.NoError(t, err, "the node created again is gone from storage after the prune")
	require.Equal(t, int64(2), got.Properties["v"])
	latest, err := b.GetNodeLatestVisible("test:p")
	require.NoError(t, err, "the node created again has no latest version after the prune")
	require.Equal(t, int64(2), latest.Properties["v"])
	require.Zero(t, countKeysWithPrefix(t, b, prefixIDFreelist), "the prune freed the ID of a node created again")

	closeB()
	b, err = runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	latest, err = b.GetNodeLatestVisible("test:p")
	require.NoError(t, err, "after reopen, the node created again has no latest version")
	require.Equal(t, int64(2), latest.Properties["v"])
}
