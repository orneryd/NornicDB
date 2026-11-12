package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412: a transaction's record that is journaled and not applied is applied
// by the recovery at open, and the nodes, relationships and label counts the
// test captures equal those of the same transaction committed live. Three
// transactions: one updates and then deletes a node, one creates and then
// updates a node, one changes labels and properties in separate writes.

type journalReplayStage func(t *testing.T, ctx context.Context, tx *BadgerTransaction)

func journalReplayEngine(t *testing.T, dir string) (*BadgerEngine, *WAL) {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: WALRecordsLarge})
	return b, b.bufferedCommitWAL
}

func journalReplayPrepare(t *testing.T, b *BadgerEngine, stage journalReplayStage) *PreparedEntityCommit {
	t.Helper()
	ctx := context.Background()
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	stage(t, ctx, tx)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	return prepared
}

type journalReplayState struct {
	Nodes  map[NodeID]string
	Edges  map[EdgeID]string
	Counts map[string]int64
}

func journalReplayCapture(t *testing.T, b *BadgerEngine, nodes []NodeID, edges []EdgeID, labels []string) journalReplayState {
	t.Helper()
	state := journalReplayState{Nodes: map[NodeID]string{}, Edges: map[EdgeID]string{}, Counts: map[string]int64{}}
	for _, id := range nodes {
		node, err := b.GetNode(t.Context(), id)
		if err != nil {
			require.ErrorIs(t, err, ErrNotFound)
			state.Nodes[id] = "absent"
			continue
		}
		labels := append([]string(nil), node.Labels...)
		sort.Strings(labels)
		state.Nodes[id] = journalReplayImage(labels, node.Properties)
	}
	for _, id := range edges {
		edge, err := b.GetEdge(t.Context(), id)
		if err != nil {
			require.ErrorIs(t, err, ErrNotFound)
			state.Edges[id] = "absent"
			continue
		}
		state.Edges[id] = journalReplayImage([]string{edge.Type, string(edge.StartNode), string(edge.EndNode)}, edge.Properties)
	}
	for _, label := range labels {
		count, err := b.NodeCountByLabelInNamespace(t.Context(), "test", label)
		require.NoError(t, err)
		state.Counts[label] = count
	}
	return state
}

func journalReplayImage(parts []string, properties map[string]any) string {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString(strings.Join(parts, "|"))
	for _, key := range keys {
		fmt.Fprintf(&out, ";%s=%T:%v", key, properties[key], properties[key])
	}
	return out.String()
}

// runJournalReplay commits stage live on one engine. On a second engine it
// journals the same transaction's record without applying it, closes the engine
// with crashClose and runs the recovery at open: the first run applies one
// record and the captured state equals the live one; a second run applies none.
func runJournalReplay(t *testing.T, seed []NodeID, stage journalReplayStage, nodes []NodeID, edges []EdgeID, labels []string) {
	t.Helper()
	seedEngine := func(b *BadgerEngine) {
		for _, id := range seed {
			_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
		}
	}

	live, liveWAL := journalReplayEngine(t, t.TempDir())
	seedEngine(live)
	receipt, err := NewWALEngine(live, liveWAL).SubmitEntityCommit(context.Background(), journalReplayPrepare(t, live, stage), nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(context.Background()))
	want := journalReplayCapture(t, live, nodes, edges, labels)

	dir := t.TempDir()
	b, w := journalReplayEngine(t, dir)
	seedEngine(b)
	before := journalReplayCapture(t, b, nodes, edges, labels)
	prepared := journalReplayPrepare(t, b, stage)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	prepared.Abort()
	_, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.NotEmpty(t, ops)
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	require.Equal(t, before, journalReplayCapture(t, b, nodes, edges, labels), "nothing was applied before crashClose")
	crashClose(t, b, w)

	b, w = journalReplayEngine(t, dir)
	n, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, want, journalReplayCapture(t, b, nodes, edges, labels))
	n, err = RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	require.Zero(t, n, "a second recovery applies no record")
	require.Equal(t, want, journalReplayCapture(t, b, nodes, edges, labels))
}

func journalReplayUpdate(t *testing.T, ctx context.Context, tx *BadgerTransaction, id NodeID, edit func(*Node)) {
	t.Helper()
	node, err := tx.GetNode(ctx, id)
	require.NoError(t, err)
	node = CopyNode(node)
	edit(node)
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
}

func TestEntityTransactionReplayUpdateThenDelete(t *testing.T) {
	runJournalReplay(t, []NodeID{"test:a", "test:keep"}, func(t *testing.T, ctx context.Context, tx *BadgerTransaction) {
		journalReplayUpdate(t, ctx, tx, "test:a", func(n *Node) { n.Properties["v"] = int64(2) })
		journalReplayUpdate(t, ctx, tx, "test:a", func(n *Node) { n.Properties["v"] = int64(3) })
		require.NoError(t, tx.DeleteGraphNode(ctx, "test:a", CascadeEdges))
		journalReplayUpdate(t, ctx, tx, "test:keep", func(n *Node) { n.Properties["v"] = int64(4) })
		journalReplayUpdate(t, ctx, tx, "test:keep", func(n *Node) { n.Properties["v"] = int64(5) })
	}, []NodeID{"test:a", "test:keep"}, nil, []string{"Doc"})
}

func TestEntityTransactionReplayCreateThenUpdate(t *testing.T) {
	runJournalReplay(t, []NodeID{"test:seed"}, func(t *testing.T, ctx context.Context, tx *BadgerTransaction) {
		_, err := tx.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
		journalReplayUpdate(t, ctx, tx, "test:b", func(n *Node) { n.Properties["v"] = int64(2) })
		journalReplayUpdate(t, ctx, tx, "test:b", func(n *Node) { n.Labels = append(n.Labels, "New") })
		journalReplayUpdate(t, ctx, tx, "test:seed", func(n *Node) { n.Properties["v"] = int64(6) })
		journalReplayUpdate(t, ctx, tx, "test:seed", func(n *Node) { n.Properties["w"] = "x" })
	}, []NodeID{"test:b", "test:seed"}, nil, []string{"Doc", "New"})
}

func TestEntityTransactionReplayLabelsAndPropertiesInSeparateWrites(t *testing.T) {
	runJournalReplay(t, []NodeID{"test:c", "test:d"}, func(t *testing.T, ctx context.Context, tx *BadgerTransaction) {
		journalReplayUpdate(t, ctx, tx, "test:c", func(n *Node) { n.Labels = append(n.Labels, "Archived") })
		journalReplayUpdate(t, ctx, tx, "test:c", func(n *Node) { n.Properties["x"] = int64(1) })
		journalReplayUpdate(t, ctx, tx, "test:c", func(n *Node) { n.Labels = []string{"Archived"} })
		require.NoError(t, tx.CreateEdge(ctx, &Edge{ID: "test:e", StartNode: "test:c", EndNode: "test:d", Type: "LINKS", Properties: map[string]any{"w": int64(1)}}))
		journalReplayUpdate(t, ctx, tx, "test:d", func(n *Node) { n.Properties["v"] = int64(7) })
		journalReplayUpdate(t, ctx, tx, "test:d", func(n *Node) { n.Labels = append(n.Labels, "Linked") })
	}, []NodeID{"test:c", "test:d"}, []EdgeID{"test:e"}, []string{"Doc", "Archived", "Linked"})
}
