package storage

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCommandJournalRecoveryUsesPublishedOutcomes(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := NewMemoryEngine()
	dir := t.TempDir()
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.NoError(t, err)
	journal := NewWALEngine(base, wal)
	defer journal.Close()
	view := NewNamespacedEngine(journal, "command_recovery")
	ctx := context.Background()
	write := func(op Operation) error { return ExecuteNativeCommand(ctx, view, op, nil) }
	require.NoError(t, write(Operation{Type: OpCreateNode, Node: &Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"key": "a", "payload": "initial"}}}))
	require.NoError(t, write(Operation{Type: OpCreateNode, Node: &Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"key": "b"}}}))
	require.ErrorIs(t, write(Operation{Type: OpCreateNode, Node: &Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"payload": "rejected-node"}}}), ErrAlreadyExists)
	require.NoError(t, write(Operation{Type: OpUpdateNode, Node: &Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"key": "a", "payload": "winner"}}}))
	require.NoError(t, write(Operation{Type: OpCreateEdge, Edge: &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "winner"}}}))
	require.ErrorIs(t, write(Operation{Type: OpCreateEdge, Edge: &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "rejected-edge"}}}), ErrAlreadyExists)
	schema := NewSchemaManager()
	require.NoError(t, schema.AddPropertyIndex("doc_key", "Doc", []string{"key"}))
	require.NoError(t, ApplyManagedReplay(ctx, journal, ManagedReplay{Namespace: "command_recovery", Transaction: true, Schema: schema.ExportDefinition(), SchemaTransaction: true}))
	// An interrupted attempt has no outcome and is never replayable.
	attempt, release, err := journal.prepareCommandJournal("command_recovery", []Operation{{Type: OpUpdateNode, Node: &Node{ID: "command_recovery:a", Properties: map[string]any{"payload": "incomplete"}}}}, false)
	require.NoError(t, err)
	require.NotNil(t, attempt)
	// Model process exit without committing: keep the begin/body but omit an
	// abort marker too. The next read sees that incomplete attempt.
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	defer release()
	plain := NewMemoryEngine()
	defer plain.Close()
	require.False(t, ReplayWALEntries(plain, entries).HasCriticalErrors())
	transactional, stats, err := RecoverWithTransactions(dir, "")
	require.NoError(t, err)
	defer transactional.Close()
	require.Empty(t, stats.FailedTransactions)
	for _, engine := range []Engine{plain, transactional} {
		restored := NewNamespacedEngine(engine, "command_recovery")
		node, err := restored.GetNode("a")
		require.NoError(t, err)
		require.Equal(t, "winner", node.Properties["payload"])
		edge, err := restored.GetEdge("e")
		require.NoError(t, err)
		require.Equal(t, "winner", edge.Properties["payload"])
		require.Equal(t, []NodeID{"command_recovery:a"}, restored.GetSchema().PropertyIndexLookup("Doc", "key", "a"), "schema redo rebuilds its committed runtime index")
	}
}
