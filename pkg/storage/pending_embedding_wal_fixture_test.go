package storage

// Author: L4 fixture ownership migration (Codex), 1 November 2026.

// Fixture cleanup migration: L4 implementation lane (Codex), 1 November 2026.

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestPendingCandidatesBufferedFixtureOwnsWALState(t *testing.T) {
	t.Cleanup(config.WithWALDisabled())
	e, _ := newWriteBehindTestEngine(t, t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	wal := e.bufferedCommitWAL
	require.True(t, config.IsWALEnabled(), "the shared WAL fixture must enable its required feature independently of prior tests")
	tx, err := e.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:fixture", Labels: []string{"Doc"}, Properties: map[string]any{"content": "WAL fixture"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "the acknowledged buffered write must actually reach the WAL")
}
