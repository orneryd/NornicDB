package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412 round 4: the replication journal, which keeps every entry in one
// frame, decides which transactions a cluster commits, before anything is
// queued, logged or applied. These are the round-3 review's two cluster
// probes, with the outcome the journal now owns.

// admissionBoundaryNodes is the reviewed transaction: 700 nodes of 19 KiB
// text, a 13.8 MB record whose journal entry is 18.4 MB.
func admissionBoundaryNodes(group int) []*storage.Node {
	nodes := make([]*storage.Node, 700)
	text := strings.Repeat("x", 19<<10)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:boundary-%d-%d", group, i)), Labels: []string{"Boundary"}, Properties: map[string]any{"text": text}}
	}
	return nodes
}

// The journal refuses the reviewed transaction's entry exactly as on main,
// through the leader preparer and ApplyCommand; through the replicated
// engine it is refused at commit, before the journal or storage hold it.
func TestReplicationJournalRefusesTheReviewedTransaction(t *testing.T) {
	fixture := setupTestAdapter(t)
	raw, err := encodeEntityCommandPayload(CmdBulkCreateNodes, admissionBoundaryNodes(0))
	require.NoError(t, err)
	normalized, settle, handled, err := fixture.adapter.PrepareEntityCommand(context.Background(), &Command{Type: CmdBulkCreateNodes, Data: raw, Timestamp: time.Now()})
	require.True(t, handled)
	require.NoError(t, err)
	require.Less(t, len(normalized.Data), 16<<20, "the record itself is under one frame")
	require.ErrorIs(t, fixture.adapter.admitReplicatedCommand(normalized), ErrTransactionTooLargeToReplicate)
	_, applyErr := fixture.adapter.ApplyCommand(context.Background(), normalized, WALPublished)
	if settle != nil {
		settle(storage.CommitOutcome{State: storage.CommitApplied})
	}
	require.ErrorContains(t, applyErr, "wal: entry exceeds maximum WAL frame size of 16777216 bytes", "the journal refuses the entry, as on main")
	entries, err := fixture.adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Empty(t, entries)

	engine, leader, follower := startHALimitPair(t, SyncAsync)
	err = engine.BulkCreateNodes(context.Background(), admissionBoundaryNodes(1))
	require.ErrorIs(t, err, ErrTransactionTooLargeToReplicate)
	require.Zero(t, leader.count(t, "Boundary"))
	require.Zero(t, follower.count(t, "Boundary"))
}

// A standby that is away while the primary is offered transactions its
// journal cannot hold catches up when it connects: those transactions were
// refused at commit, so nothing acknowledged is missing from the journal.
func TestHAReconnectAfterRefusedTransactionsCatchesUp(t *testing.T) {
	leader, follower := newLimitNode(t), newLimitNode(t)
	standbyAddr := limitFreeAddr(t)
	cfg := DefaultConfig()
	cfg.Mode, cfg.NodeID, cfg.BindAddr = ModeHAStandby, "admission-primary", "127.0.0.1:0"
	cfg.HAStandby.Role, cfg.HAStandby.PeerAddr, cfg.HAStandby.SyncMode = "primary", standbyAddr, SyncAsync
	cfg.HAStandby.AutoFailover = false
	cfg.HAStandby.WALBatchTimeout, cfg.HAStandby.HeartbeatInterval = 50*time.Millisecond, 50*time.Millisecond
	cfg.HAStandby.ReconnectInterval, cfg.HAStandby.MaxReconnectBackoff = 20*time.Millisecond, 50*time.Millisecond
	primary, err := NewHAStandbyReplicator(cfg, leader.fixture.adapter)
	require.NoError(t, err)
	limitTransport(t, primary, cfg.NodeID, "")
	engine := NewReplicatedEngine(leader.fixture.adapter.engine, primary, 30*time.Second)

	require.NoError(t, engine.BulkCreateNodes(t.Context(), limitTestNodes("Before", 50, 1<<10)))
	for i := 0; i < 5; i++ {
		err := engine.BulkCreateNodes(t.Context(), admissionBoundaryNodes(i))
		require.True(t, errors.Is(err, ErrTransactionTooLargeToReplicate), "refused at commit: %v", err)
	}
	require.Zero(t, leader.count(t, "Boundary"), "no refused transaction was applied")
	require.NoError(t, leader.fixture.adapter.FlushWAL())
	entries, err := leader.fixture.adapter.GetWALEntries(0, 1000)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the journal holds only the acknowledged transaction")

	standbyCfg := DefaultConfig()
	standbyCfg.Mode, standbyCfg.NodeID, standbyCfg.BindAddr = ModeHAStandby, "admission-standby", standbyAddr
	standbyCfg.HAStandby.Role, standbyCfg.HAStandby.PeerAddr = "standby", "127.0.0.1:0"
	standbyCfg.HAStandby.AutoFailover, standbyCfg.HAStandby.HeartbeatInterval = false, 50*time.Millisecond
	standby, err := NewHAStandbyReplicator(standbyCfg, follower.fixture.adapter)
	require.NoError(t, err)
	limitTransport(t, standby, standbyCfg.NodeID, standbyAddr)
	require.NoError(t, engine.BulkCreateNodes(t.Context(), limitTestNodes("After", 1, 10)))
	require.NoError(t, leader.fixture.adapter.FlushWAL())
	require.Eventually(t, func() bool {
		return follower.count(t, "Before") == 50 && follower.count(t, "After") == 1
	}, 30*time.Second, 50*time.Millisecond, "the standby catches up with everything acknowledged")
	require.Zero(t, follower.count(t, "Boundary"))
	position, err := leader.fixture.adapter.GetWALPosition()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return primary.walStreamer.LastAcked() >= position }, 30*time.Second, 50*time.Millisecond)
}
