package storage_test

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// Derived from the #1658 TestEpochParentSnapshotHistoryDiagnostic retained in
// test-only f37f269a0. Both snapshot frame orders and the retained-history-only
// control use the same buffered WAL. No product implementation is copied here.
//
// Ported from PR1690 commit 56b1e8e24 onto b098f7e70. The acceptance assertions
// are unchanged; the storage entry points took the caller's context, and each
// engine is registered with the repository's test-fixture owner. Every ported
// context is context.Background() because the original calls had none, so the
// recorded recovery schedule is preserved. The reopened engine is a separate
// fixture variable so its registered close runs before its lifetime check.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNOR140RetainedHistoryRestoreCacheAgreement(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
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
		_, err = source.CreateNode(context.Background(), n)
		require.NoError(t, err)
	}
	edge := &storage.Edge{ID: "binding:r", StartNode: "binding:a", EndNode: "binding:b", Type: "R", Properties: map[string]any{"alpha": "before-alpha"}}
	require.NoError(t, source.CreateEdge(context.Background(), edge))
	require.NoError(t, source.FlushWriteBehind(context.Background()))
	for i, n := range initial {
		initial[i], err = source.GetNode(context.Background(), n.ID)
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
	require.NoError(t, source.FlushWriteBehind(context.Background()))
	require.NoError(t, journal.Close())
	entries, err := storage.ReadWALEntriesFromDir(t.Context(), walDir)
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
				fresh, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(opts))
				require.NoError(t, err)
				t.Cleanup(func() { _ = fresh.Close() })
				fresh.SetDecayEnabled(false)
				if writeBehind {
					storage.NOR140StopReplayWorkerForTest(fresh)
				}
				result, _, err := storage.RecoverIntoEngine(t.Context(), fresh, walDir, snapshotPath)
				require.NoError(t, err)
				require.Zero(t, result.Failed, "%s", result.Summary())
				check := func(target *storage.BadgerEngine, phase string) {
					t.Helper()
					cached, err := target.GetNode(context.Background(), "binding:a")
					require.NoError(t, err)
					queryNode, err := target.GetQueryNode(context.Background(), "binding:a")
					require.NoError(t, err)
					rows, err := nor140Executor(target, "binding").Execute(context.Background(), "MATCH (n:NOR140Alpha) RETURN n.alpha AS alpha, n.beta AS beta", nil)
					require.NoError(t, err)
					stored, err := nor140ScannedNode(context.Background(), target, "binding:a")
					require.NoError(t, err)
					t.Logf("%s: cached=%v query-node=%v Cypher=%v stored=%v", phase, cached.Properties, queryNode.Properties, rows.Rows, stored.Properties)
					assert.Equal(t, stored.Properties, cached.Properties, "%s: cached/stored agreement", phase)
					assert.Equal(t, stored.Properties, queryNode.Properties, "%s: query-node/stored agreement", phase)
					assert.Equal(t, want, cached.Properties, "%s: latest acknowledged node value", phase)
					assert.Equal(t, want, stored.Properties, "%s: replayed stored value", phase)
					assert.Equal(t, [][]any{{"after-alpha", "after-beta"}}, rows.Rows, "%s: public Cypher result", phase)
					relationship, err := target.GetEdge(t.Context(), "binding:r")
					require.NoError(t, err)
					assert.Equal(t, want, relationship.Properties, "%s: latest acknowledged relationship value", phase)
					count, err := target.NodeCountByLabelInNamespace(t.Context(), "binding", "NOR140Alpha")
					require.NoError(t, err)
					assert.EqualValues(t, 1, count, "%s: restored label membership counted once", phase)
				}
				check(fresh, "after-recovery")
				require.NoError(t, fresh.FlushWriteBehind(context.Background()))
				if writeBehind {
					require.False(t, storage.NOR140BufferedNodeForTest(fresh, "binding:a"))
				}
				check(fresh, "after-explicit-flush")
				require.NoError(t, fresh.Close())
				reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(opts))
				require.NoError(t, err)
				t.Cleanup(func() { _ = reopened.Close() })
				reopened.SetDecayEnabled(false)
				if writeBehind {
					storage.NOR140StopReplayWorkerForTest(reopened)
				}
				check(reopened, "after-reopen")
			})
		}
	}
}
