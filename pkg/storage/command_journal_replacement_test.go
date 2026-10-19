package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCommandJournalRecoveryPreservesHiddenReplacement(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("replicated=%v", versioned), func(t *testing.T) {
			base := NewMemoryEngine()
			dir := t.TempDir()
			wal, err := NewWAL(dir, DefaultWALConfig())
			require.NoError(t, err)
			journal := NewWALEngine(base, wal)
			defer journal.Close()
			view := NewNamespacedEngine(journal, "replacement_redo")
			ctx := context.Background()
			write := func(op Operation) error { return ExecuteNativeCommand(ctx, view, op, nil) }
			for _, id := range []NodeID{"a", "b"} {
				require.NoError(t, write(Operation{Type: OpCreateNode, Node: &Node{ID: id}}))
			}
			require.NoError(t, write(Operation{Type: OpCreateEdge, Edge: &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", VisibilitySuppressed: true, Properties: map[string]any{"payload": "old"}}}))
			require.NoError(t, base.SetEdgeManuallySuppressed("replacement_redo:e", true))
			if versioned {
				ctx = WithCommandVersion(ctx, [16]byte{1})
			}
			require.NoError(t, ApplyManagedReplay(ctx, journal, ManagedReplay{Namespace: "replacement_redo", Transaction: true, Operations: []Operation{{Type: OpCreateEdge, ReplacesExisting: true, Edge: &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "replacement"}}}}}))
			live, err := GetStoredEdge(view, "e")
			require.NoError(t, err)
			require.Equal(t, "replacement", live.Properties["payload"])
			require.NoError(t, wal.Sync())
			entries, err := ReadWALEntriesFromDir(dir)
			require.NoError(t, err)
			plain := NewMemoryEngine()
			defer plain.Close()
			result := ReplayWALEntries(plain, entries)
			require.False(t, result.HasCriticalErrors(), "%+v", result.Errors)
			transactional, stats, err := RecoverWithTransactions(dir, "")
			require.NoError(t, err)
			defer transactional.Close()
			require.Empty(t, stats.FailedTransactions)
			for _, recovered := range []Engine{plain, transactional} {
				edge, err := GetStoredEdge(NewNamespacedEngine(recovered, "replacement_redo"), "e")
				require.NoError(t, err)
				require.Equal(t, live, edge, "accepted hidden replacement survives recovery with its final payload")
			}
		})
	}
}
