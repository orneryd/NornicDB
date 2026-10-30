package storage

// Author: Codex (#1636 lane). Public engine construction, transaction admission
// and Close. Only the final native apply result is injected at its existing seam.

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestOwnerClassStorageFinalDrainOutcome(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	// Stop periodic scheduling before installing the native apply failure. The
	// public Close still performs its real final drain on the admitted commit.
	engine.writeBehind.Close()
	failure := errors.New("r4 final write-behind drain failure")
	engine.writeBehind.apply = func(*CommitBuffer) error { return failure }
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("r4"))
	_, err = tx.CreateNode(&Node{ID: "r4:final-drain", Properties: map[string]any{"value": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.ErrorIs(t, engine.Close(), failure, "engine Close lost its final admitted drain outcome")
}

func TestOwnerClassStorageNativeFinalOutcomes(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	// Native I/O failure setup, as with the WAL descriptor controls: close the
	// actual underlying handle, then retire through the public composed owner.
	require.NoError(t, engine.db.Close())
	result := engine.Close()
	require.ErrorIs(t, result, badger.ErrDBClosed)
	require.ErrorContains(t, result, "label-count final fold")
	require.ErrorContains(t, result, "edge-type-count final fold")
}
