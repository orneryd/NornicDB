package storage

import (
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The snapshots contain the same logical graph in two legal frame orders.
// The intervening namespace makes each single-node snapshot batch explicit,
// so property-token allocation is deterministic rather than map-order dependent.
func TestSnapshotBufferedWALPropertyBindingsControl(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	initial := []*Node{
		{ID: "binding:a", Properties: map[string]any{"alpha": "before-alpha"}},
		{ID: "binding:b", Properties: map[string]any{"beta": "before-beta"}},
		{ID: "separator:n", Properties: map[string]any{"separator": "batch-boundary"}},
	}
	for _, node := range initial {
		_, err = source.CreateNode(node)
		require.NoError(t, err)
	}
	initialEdge := &Edge{ID: "binding:r", StartNode: "binding:a", EndNode: "binding:b", Type: "R", Properties: map[string]any{"alpha": "before-alpha"}}
	require.NoError(t, source.CreateEdge(initialEdge))
	require.NoError(t, source.FlushWriteBehind())
	for i, node := range initial {
		initial[i], err = source.GetNode(node.ID)
		require.NoError(t, err)
	}
	walDir := t.TempDir()
	wal, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	journal := NewWALEngine(source, wal)
	t.Cleanup(func() { _ = journal.Close() })
	tx, err := source.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("binding"))
	require.NoError(t, tx.SetImplicit(true))
	final := &Node{ID: "binding:a", Properties: map[string]any{"alpha": "after-alpha", "beta": "after-beta"}}
	require.NoError(t, tx.UpdateGraphNode(final, ClientNodeEdits))
	finalEdge := copyEdge(initialEdge)
	finalEdge.Properties = final.Properties
	require.NoError(t, tx.UpdateEdge(finalEdge))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, source.FlushWriteBehind())
	require.NoError(t, journal.Close())
	entries, err := ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	buffered := 0
	for _, entry := range entries {
		if entry.Operation == OpBufferedCommit {
			buffered++
		}
	}
	require.Equal(t, 1, buffered, "the log carries the acknowledged physical update")
	for _, reverse := range []bool{false, true} {
		name := "same-token-order"
		first, last := initial[0], initial[1]
		if reverse {
			name = "reversed-token-order"
			first, last = last, first
		}
		t.Run(name, func(t *testing.T) {
			snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
			require.NoError(t, SaveSnapshot(&Snapshot{Timestamp: time.Now(), Nodes: []*Node{first, initial[2], last}, Edges: []*Edge{initialEdge}, Version: "1.0"}, snapshotPath))
			dir := t.TempDir()
			fresh, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = fresh.Close() })
			result, _, err := RecoverIntoEngine(fresh, walDir, snapshotPath)
			require.NoError(t, err)
			require.Zero(t, result.Failed, "%s", result.Summary())
			got, err := fresh.GetNode("binding:a")
			require.NoError(t, err)
			require.Equal(t, final.Properties, got.Properties, "physical WAL must retain property names after semantic snapshot restoration")
			// Reopen to exercise persisted bodies rather than the replay cache.
			require.NoError(t, fresh.Close())
			fresh, err = fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
			require.NoError(t, err)
			got, err = fresh.GetNode("binding:a")
			require.NoError(t, err)
			require.Equal(t, final.Properties, got.Properties)
			edge, err := fresh.GetEdge("binding:r")
			require.NoError(t, err)
			require.Equal(t, final.Properties, edge.Properties)
			sentinel, err := fresh.GetNode("binding:b")
			require.NoError(t, err)
			require.Equal(t, initial[1].Properties, sentinel.Properties, "restored bodies keep their own token bindings")
		})
	}
}
