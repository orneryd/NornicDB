package lifecycle

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// One open transaction must not stop pruning everywhere. Before this fix the
// engine's prune skipped its whole cycle while any snapshot reader was
// registered, so a busy server never pruned and built a backlog. The reader
// now only protects what it can see: its own database's versions from the one
// visible at its snapshot onward. Regression of 7b14b9133 (fix for GitHub
// #732); no NOR issue, recorded on NOR-405.

import (
	"bytes"
	"context"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagerPrunesPastLongLivedReader(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{
		RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100},
	}}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	config := DefaultLifecycleConfig()
	config.Enabled = true
	config.MaxVersionsPerKey = 2
	manager := NewMVCCLifecycleManager(config, engine)
	engine.SetLifecycleController(manager)

	update := func(id storage.NodeID, from, to int) {
		for v := from; v <= to; v++ {
			require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: id, Properties: map[string]any{"v": v}}}))
		}
	}
	held := storage.NodeID("dba:n")
	other := storage.NodeID("dbb:n")
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: held, Properties: map[string]any{"v": 0}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: other, Properties: map[string]any{"v": 0}})
	require.NoError(t, err)
	update(held, 1, 4)
	head4, err := engine.GetNodeCurrentHead(context.Background(), held)
	require.NoError(t, err)

	// A long-lived transaction in dba sees v=4.
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("dba"))
	update(held, 5, 10)
	update(other, 1, 10)

	// Node heads are listed in numeric-ID order: dba:n was created first.
	var logical [][]byte
	require.NoError(t, engine.IterateMVCCHeads(context.Background(), func(key []byte, _ storage.MVCCHead) error {
		if key[0] == 0x0c {
			logical = append(logical, bytes.Clone(key))
		}
		return nil
	}))
	require.Len(t, logical, 2)
	versions := func(key []byte) int {
		count := 0
		require.NoError(t, engine.IterateMVCCVersions(context.Background(), key, func(storage.MVCCVersion, bool, int64) error {
			count++
			return nil
		}))
		return count
	}
	require.Equal(t, 10, versions(logical[0]))
	require.Equal(t, 10, versions(logical[1]))

	deleted, err := manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.NoError(t, err)
	// dbb has no reader: retention alone applies (2 kept of 10).
	require.Equal(t, 2, versions(logical[1]), "pruning runs in a database without readers")
	// dba keeps v4 (the reader's version) and everything newer: v4..v9.
	require.Equal(t, 6, versions(logical[0]), "below the reader's version, its own database is pruned too")
	require.Equal(t, int64(8+4), deleted)

	node, err := tx.GetNode(context.Background(), held)
	require.NoError(t, err)
	require.EqualValues(t, 4, node.Properties["v"], "the reader keeps its view")
	at, err := engine.GetNodeVisibleAt(t.Context(), held, head4.Version)
	require.NoError(t, err)
	require.EqualValues(t, 4, at.Properties["v"], "its version stays readable as of its snapshot")

	require.NoError(t, tx.Rollback())
	_, err = manager.RunPruneNow(context.Background(), storage.MVCCPruneOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, versions(logical[0]), "after the reader ends, retention alone applies")
}
