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
	var finalOutcome error
	t.Cleanup(func() {
		if finalOutcome == nil {
			require.NoError(t, engine.Close())
			return
		}
		require.ErrorIs(t, engine.Close(), finalOutcome, "repeated Close must retain its actual final outcome")
	})
	// Stop periodic scheduling before installing the native apply failure. The
	// public Close still performs its real final drain on the admitted commit.
	engine.writeBehind.Close()
	failure := errors.New("r4 final write-behind drain failure")
	engine.writeBehind.apply = func(*CommitBuffer) error { return failure }
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("r4"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: "r4:final-drain", Properties: map[string]any{"value": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	finalOutcome = engine.Close()
	require.ErrorIs(t, finalOutcome, failure, "engine Close lost its final admitted drain outcome")
	require.ErrorIs(t, engine.Close(), finalOutcome, "repeated Close lost the retained final outcome")
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
	require.ErrorIs(t, engine.Close(), result, "repeated Close must retain every native final failure")
}
