package storage_test

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// Derived from the #1658 TestEpochParentSnapshotHistoryDiagnostic retained in
// test-only f37f269a0. Both snapshot frame orders and the retained-history-only
// control use the same buffered WAL. No product implementation is copied here.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNOR140RetainedHistoryRestoreCacheAgreement(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	source.SetDecayEnabled(false)
	storage.NOR140StopReplayWorkerForTest(source)
	initial := []*storage.Node{
		{ID: "binding:a", Labels: []string{"NOR140Alpha"}, Properties: map[string]any{"alpha": "before-alpha"}},
		{ID: "binding:b", Properties: map[string]any{"beta": "before-beta"}},
		{ID: "separator:n", Properties: map[string]any{"separator": "batch-boundary"}},
	}
	for _, n := range initial {
		_, err = source.CreateNode(n)
		require.NoError(t, err)
	}
	edge := &storage.Edge{ID: "binding:r", StartNode: "binding:a", EndNode: "binding:b", Type: "R", Properties: map[string]any{"alpha": "before-alpha"}}
	require.NoError(t, source.CreateEdge(edge))
	require.NoError(t, source.FlushWriteBehind())
	for i, n := range initial {
		initial[i], err = source.GetNode(n.ID)
		require.NoError(t, err)
	}
	walDir := t.TempDir()
	wal, err := storage.NewWAL(walDir, storage.DefaultWALConfig())
	require.NoError(t, err)
	journal := storage.NewWALEngine(source, wal)
	t.Cleanup(func() { _ = journal.Close() })
	_, err = nor140Executor(source, "binding").Execute(context.Background(), "MATCH (a:NOR140Alpha)-[r:R]->() SET a.alpha = 'after-alpha', a.beta = 'after-beta', r.alpha = 'after-alpha', r.beta = 'after-beta'", nil)
	require.NoError(t, err)
	require.True(t, storage.NOR140BufferedNodeForTest(source, "binding:a"), "the source update must use buffered acknowledgement")
	require.NoError(t, source.FlushWriteBehind())
	require.NoError(t, journal.Close())
	entries, err := storage.ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	buffered := 0
	for _, entry := range entries {
		if entry.Operation == storage.OpBufferedCommit {
			buffered++
		}
	}
	require.Equal(t, 1, buffered, "the unchanged WAL carries one acknowledged node/relationship update")
	want := map[string]any{"alpha": "after-alpha", "beta": "after-beta"}
	for _, writeBehind := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("write-behind=%t/reversed-order=%t", writeBehind, reverse), func(t *testing.T) {
				first, last := initial[0], initial[1]
				if reverse {
					first, last = last, first
				}
				snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
				require.NoError(t, storage.SaveSnapshot(&storage.Snapshot{Timestamp: time.Now(), Nodes: []*storage.Node{first, initial[2], last}, Edges: []*storage.Edge{edge}, Version: "1.0"}, snapshotPath))
				dir := t.TempDir()
				opts := storage.BadgerOptions{DataDir: dir, WriteBehind: writeBehind, WriteBehindInterval: time.Hour, EngineOptions: storage.EngineOptions{RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100}}}
				fresh, err := storage.NewBadgerEngineWithOptions(opts)
				require.NoError(t, err)
				t.Cleanup(func() { _ = fresh.Close() })
				fresh.SetDecayEnabled(false)
				if writeBehind {
					storage.NOR140StopReplayWorkerForTest(fresh)
				}
				result, _, err := storage.RecoverIntoEngine(fresh, walDir, snapshotPath)
				require.NoError(t, err)
				require.Zero(t, result.Failed, "%s", result.Summary())
				check := func(phase string) {
					t.Helper()
					cached, err := fresh.GetNode("binding:a")
					require.NoError(t, err)
					queryNode, err := fresh.GetQueryNode("binding:a")
					require.NoError(t, err)
					rows, err := nor140Executor(fresh, "binding").Execute(context.Background(), "MATCH (n:NOR140Alpha) RETURN n.alpha AS alpha, n.beta AS beta", nil)
					require.NoError(t, err)
					stored, err := nor140ScannedNode(context.Background(), fresh, "binding:a")
					require.NoError(t, err)
					t.Logf("%s: cached=%v query-node=%v Cypher=%v stored=%v", phase, cached.Properties, queryNode.Properties, rows.Rows, stored.Properties)
					assert.Equal(t, stored.Properties, cached.Properties, "%s: cached/stored agreement", phase)
					assert.Equal(t, stored.Properties, queryNode.Properties, "%s: query-node/stored agreement", phase)
					assert.Equal(t, want, cached.Properties, "%s: latest acknowledged node value", phase)
					assert.Equal(t, want, stored.Properties, "%s: replayed stored value", phase)
					assert.Equal(t, [][]any{{"after-alpha", "after-beta"}}, rows.Rows, "%s: public Cypher result", phase)
				}
				check("after-recovery")
				require.NoError(t, fresh.FlushWriteBehind())
				if writeBehind {
					require.False(t, storage.NOR140BufferedNodeForTest(fresh, "binding:a"))
				}
				check("after-explicit-flush")
				require.NoError(t, fresh.Close())
				fresh, err = storage.NewBadgerEngineWithOptions(opts)
				require.NoError(t, err)
				fresh.SetDecayEnabled(false)
				if writeBehind {
					storage.NOR140StopReplayWorkerForTest(fresh)
				}
				check("after-reopen")
			})
		}
	}
}
