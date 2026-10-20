package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Drive public Raft admission after an election, controlling peer acknowledgments
// independently of the state-machine apply loop. No transport or peer server is used.
func namespacePriorTermLeader(t *testing.T, priorType CommandType, singleNode bool) (*RaftReplicator, *storage.MemoryEngine) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, err := base.CreateNode(&storage.Node{ID: "prior_a:old"})
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{ID: "prior_b:peer"})
	require.NoError(t, err)
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	cfg := DefaultConfig()
	cfg.Mode, cfg.NodeID = ModeRaft, "new-leader"
	cfg.Raft.CommitTimeout = 2 * time.Second
	cfg.Raft.HeartbeatTimeout = time.Hour
	cfg.Raft.Bootstrap = singleNode
	if !singleNode {
		cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:1"}}
	}
	raft, err := NewRaftReplicator(cfg, adapter)
	require.NoError(t, err)
	var data []byte
	if priorType == CmdDeleteByPrefix {
		data, err = encodeGob(namespaceDropCommand{Prefix: "prior_a:", Retirement: 1, RevisionAssigned: true})
	} else {
		data, err = encodeNodePayload(&storage.Node{ID: "prior_a:inherited"})
	}
	require.NoError(t, err)
	raft.log = append(raft.log, &RaftLogEntry{Index: 1, Term: 1, Command: &Command{Type: priorType, Data: data}})
	raft.state, raft.currentTerm = StateCandidate, 2
	ctx, cancel := context.WithCancel(context.Background())
	raft.becomeLeader(ctx, 2)
	raft.started.Store(true)
	raft.wg.Add(1)
	go raft.runApplyLoop(ctx)
	t.Cleanup(func() {
		cancel()
		raft.wg.Wait()
	})
	return raft, base
}

func namespacePriorTermDrop(t *testing.T, raft *RaftReplicator, ctx context.Context) (*storage.NamespaceDrop, <-chan error) {
	t.Helper()
	data, err := encodeGob(namespaceDropCommand{Prefix: "prior_a:"})
	require.NoError(t, err)
	op := storage.NewNamespaceDrop(nil)
	cmd := &Command{Type: CmdDeleteByPrefix, Data: data, namespaceDrop: op}
	result := make(chan error, 1)
	go func() { result <- raft.ApplyContext(ctx, cmd, 3*time.Second) }()
	return op, result
}

func namespacePriorTermLogLength(raft *RaftReplicator) int {
	raft.logMu.RLock()
	defer raft.logMu.RUnlock()
	return len(raft.log)
}

func namespacePriorTermAcknowledge(raft *RaftReplicator, index uint64) {
	raft.peerMu.Lock()
	raft.matchIndex["peer"] = index
	raft.peerMu.Unlock()
	raft.advanceCommitIndex()
}

func TestNamespaceDropAfterLeadershipChange(t *testing.T) {
	for _, priorType := range []CommandType{CmdCreateNode, CmdDeleteByPrefix} {
		t.Run(map[CommandType]string{CmdCreateNode: "write", CmdDeleteByPrefix: "drop"}[priorType], func(t *testing.T) {
			raft, base := namespacePriorTermLeader(t, priorType, false)
			namespacePriorTermAcknowledge(raft, 1)
			raft.logMu.RLock()
			committed := raft.commitIndex
			raft.logMu.RUnlock()
			require.Zero(t, committed, "prior-term replication alone cannot advance commit")
			op, result := namespacePriorTermDrop(t, raft, context.Background())
			require.Eventually(t, func() bool { return namespacePriorTermLogLength(raft) >= 3 }, time.Second, time.Millisecond,
				"new leader must admit a current-term ordering entry before waiting for its inherited frontier")
			raft.logMu.RLock()
			ordering := *raft.log[2]
			raft.logMu.RUnlock()
			require.Equal(t, uint64(2), ordering.Term)
			require.Nil(t, ordering.Command, "progress entry must not admit DROP before preparation")
			require.False(t, op.Committed())
			_, err := base.CreateNode(&storage.Node{ID: "prior_a:before-frontier"})
			require.NoError(t, err, "the inherited entry must remain unfenced until applied")

			namespacePriorTermAcknowledge(raft, 2)
			require.Eventually(t, func() bool { return namespacePriorTermLogLength(raft) == 4 }, time.Second, time.Millisecond)
			if priorType == CmdCreateNode {
				_, err = base.GetNode("prior_a:inherited")
				require.NoError(t, err, "inherited writes apply before DROP installs its fence")
			}
			namespacePriorTermAcknowledge(raft, 3)
			require.NoError(t, <-result)
			require.True(t, op.Committed())
			for _, id := range []storage.NodeID{"prior_a:old", "prior_a:inherited", "prior_a:before-frontier"} {
				_, err = base.GetNode(id)
				require.ErrorIs(t, err, storage.ErrNotFound)
			}
			_, err = base.GetNode("prior_b:peer")
			require.NoError(t, err)
			_, err = base.CreateNode(&storage.Node{ID: "prior_a:fresh"})
			require.NoError(t, err, "completed DROP releases the new namespace lifetime")

			// A follower receives an ordinary nil-command ordering entry and applies
			// the inherited command followed by DROP in exactly the same order.
			followerBase := storage.NewMemoryEngine()
			defer followerBase.Close()
			_, err = followerBase.CreateNode(&storage.Node{ID: "prior_a:old"})
			require.NoError(t, err)
			_, err = followerBase.CreateNode(&storage.Node{ID: "prior_b:peer"})
			require.NoError(t, err)
			followerAdapter, err := NewStorageAdapterWithWAL(followerBase, t.TempDir())
			require.NoError(t, err)
			defer followerAdapter.Close()
			follower, err := NewRaftReplicator(raft.config, followerAdapter)
			require.NoError(t, err)
			raft.logMu.RLock()
			entries := make([]*RaftLogEntry, 0, 3)
			for _, entry := range raft.log[1:] {
				copyEntry := &RaftLogEntry{Index: entry.Index, Term: entry.Term}
				if entry.Command != nil {
					copyEntry.Command = &Command{Type: entry.Command.Type, Data: append([]byte(nil), entry.Command.Data...)}
				}
				entries = append(entries, copyEntry)
			}
			raft.logMu.RUnlock()
			response := follower.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "new-leader", Entries: entries, LeaderCommit: 3})
			require.True(t, response.Success)
			_, err = followerBase.GetNode("prior_a:old")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = followerBase.GetNode("prior_a:inherited")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = followerBase.GetNode("prior_b:peer")
			require.NoError(t, err)
		})
	}
}

func TestNamespaceDropPriorTermCancellationReusesOrderingEntry(t *testing.T) {
	raft, base := namespacePriorTermLeader(t, CmdCreateNode, false)
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		// The public route is covered above. Drive admission directly here so
		// cancellation of a reused ordering entry cannot race queue submission.
		data, err := encodeGob(namespaceDropCommand{Prefix: "prior_a:"})
		require.NoError(t, err)
		op := storage.NewNamespaceDrop(nil)
		future := &applyFuture{caller: ctx, cmd: &Command{Type: CmdDeleteByPrefix, Data: data, namespaceDrop: op}, errCh: make(chan error, 1), doneCh: make(chan struct{})}
		raft.wg.Add(1)
		go func() {
			defer raft.wg.Done()
			raft.processApply(context.Background(), future)
		}()
		require.Eventually(t, func() bool { return namespacePriorTermLogLength(raft) >= 3 }, time.Second, time.Millisecond)
		cancel()
		require.ErrorIs(t, <-future.errCh, context.Canceled)
		<-op.Done()
		require.False(t, op.Committed())
		require.Equal(t, 3, namespacePriorTermLogLength(raft), "reuse one current-term entry; cancelled DROP never enters the log")
		_, err = base.GetNode("prior_a:old")
		require.NoError(t, err)
		_, err = base.CreateNode(&storage.Node{ID: storage.NodeID([]string{"prior_a:first", "prior_a:second"}[attempt])})
		require.NoError(t, err)
	}
	raft.logMu.RLock()
	defer raft.logMu.RUnlock()
	require.Zero(t, raft.commitIndex, "cancellation must not bypass quorum")
}

func TestNamespaceDropPriorTermSingleNode(t *testing.T) {
	raft, base := namespacePriorTermLeader(t, CmdCreateNode, true)
	op, result := namespacePriorTermDrop(t, raft, context.Background())
	require.NoError(t, <-result)
	require.True(t, op.Committed())
	_, err := base.GetNode("prior_a:inherited")
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = base.GetNode("prior_b:peer")
	require.NoError(t, err)
	raft.logMu.RLock()
	defer raft.logMu.RUnlock()
	require.Equal(t, uint64(3), raft.lastApplied, "the inherited entry and ordering entry apply before DROP")
}
