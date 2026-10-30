package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestBufferedRecoveryMultipleCommitsPreserveHistory(t *testing.T) {
	dir := t.TempDir()
	open := func() (*BadgerEngine, *WAL) {
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}}))
		if b != nil {
			fixtureowner.Own(t, b)
		}
		require.NoError(t, err)
		cfg := DefaultWALConfig()
		cfg.Dir = filepath.Join(dir, "wal")
		w, err := NewWAL(cfg.Dir, cfg)
		require.NoError(t, err)
		b.SetBufferedCommitWAL(w)
		return b, w
	}
	b, w := open()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:b"})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"v": int64(1)}}))
	require.NoError(t, tx.Commit())
	h, err := b.loadNodeMVCCHead("test:a")
	require.NoError(t, err)
	versions := []MVCCVersion{h.Version}
	for _, v := range []int64{2, 3} {
		tx, err = b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.SetImplicit(true))
		n, err := tx.GetNode("test:a")
		require.NoError(t, err)
		n.Properties = map[string]any{"v": v}
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: n, Policy: ClientNodeEdits}))
		e, err := tx.GetEdge("test:r")
		require.NoError(t, err)
		e.Properties = map[string]any{"v": v}
		require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: e}))
		require.NoError(t, tx.Commit())
		require.True(t, tx.AcknowledgedBuffered())
	}
	entries, err := ReadWALEntriesFromDir(w.Config().Dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Operation == OpBufferedCommit {
			payload, err := decompressWALBufferedCommitPayload(entry.Data)
			require.NoError(t, err)
			var d WALBufferedCommitData
			require.NoError(t, decodeRecoveryPayload(payload, &d))
			versions = append(versions, d.Version)
		}
	}
	require.Len(t, versions, 3)
	crashClose(t, b, w)
	b, w = open()
	t.Cleanup(func() { _ = w.Close(); _ = b.Close() })
	count, err := RecoverBufferedCommits(b, w)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	check := func(stage string) {
		t.Helper()
		for i, v := range versions {
			n, err := b.GetNodeVisibleAt("test:a", v)
			require.NoError(t, err, stage)
			require.Equal(t, int64(i+1), n.Properties["v"], stage+" node")
			e, err := b.GetEdgeVisibleAt("test:r", v)
			require.NoError(t, err, stage)
			require.Equal(t, int64(i+1), e.Properties["v"], stage+" edge")
		}
	}
	check("warm")
	b.nodeCacheMu.Lock()
	clear(b.nodeCache)
	b.nodeCacheMu.Unlock()
	b.edgeCacheMu.Lock()
	clear(b.edgeCache)
	b.edgeCacheMu.Unlock()
	check("evicted")
	require.NoError(t, w.Close())
	require.NoError(t, b.Close())
	b, w = open()
	check("reopened")
}
