package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise the real consensus publication/apply owners without peer networking.
// The test decides exactly when a proposal commits, survives a leader change,
// or is overwritten, independently of its caller's lifetime.
func TestNamespaceDropConsensusLifecycle(t *testing.T) {
	for _, outcome := range []string{"commit", "follower-commit", "discard"} {
		t.Run(outcome, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			_, err := base.CreateNode(&storage.Node{ID: "consensus_a:old"})
			require.NoError(t, err)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			cfg := DefaultConfig()
			cfg.Mode = ModeRaft
			cfg.NodeID = "lifecycle"
			cfg.AdvertiseAddr = "127.0.0.1:17000"
			cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
			cfg.Raft.CommitTimeout = time.Second
			raft, err := NewRaftReplicator(cfg, adapter)
			require.NoError(t, err)
			raft.state, raft.currentTerm = StateLeader, 1
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			completions := make(chan struct{}, 1)
			op := storage.NewNamespaceDrop(func() error { completions <- struct{}{}; return nil })
			data, err := encodeGob(struct{ Prefix string }{"consensus_a:"})
			require.NoError(t, err)
			cmd := &Command{Type: CmdDeleteByPrefix, Data: data, namespaceDrop: op}
			future := &applyFuture{caller: ctx, cmd: cmd, errCh: make(chan error, 1), doneCh: make(chan struct{})}
			raft.processApply(context.Background(), future)
			require.False(t, op.Committed())
			_, err = base.GetNode("consensus_a:old")
			require.NoError(t, err, "proposal alone must not delete")
			_, err = base.CreateNode(&storage.Node{ID: "consensus_a:blocked"})
			require.ErrorIs(t, err, storage.ErrConflict)
			_, err = base.CreateNode(&storage.Node{ID: "consensus_b:peer"})
			require.NoError(t, err)
			cancel() // Stops the client, never the eventual committed entry.
			if outcome == "discard" {
				// Retain the original backing slice to verify the upstream log
				// truncation releases unreachable entries after lifecycle aborts.
				raft.log = append(raft.log, &RaftLogEntry{Index: 2, Term: 1, Command: &Command{Type: CmdCreateNode, Data: []byte("discarded")}})
				discardedBacking := raft.log
				response := raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "peer", Entries: []*RaftLogEntry{{Index: 1, Term: 2}}})
				require.True(t, response.Success)
				require.Nil(t, discardedBacking[2], "truncation must not retain the discarded suffix")
				require.ErrorIs(t, <-future.errCh, ErrNotLeader, "discard must resolve the waiting proposal before clearing entries")
				select {
				case <-op.Done():
				case <-time.After(time.Second):
					t.Fatal("discarded proposal retained admission")
				}
				require.False(t, op.Committed())
				_, err = base.GetNode("consensus_a:old")
				require.NoError(t, err)
				_, err = base.CreateNode(&storage.Node{ID: "consensus_a:after-discard"})
				require.NoError(t, err)
				select {
				case <-completions:
					t.Fatal("discard ran committed completion")
				default:
				}
				return
			}
			if outcome == "follower-commit" {
				response := raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "peer", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1})
				require.True(t, response.Success)
			} else {
				raft.peerMu.Lock()
				raft.matchIndex["peer"] = 1
				raft.peerMu.Unlock()
				raft.advanceCommitIndex()
			}
			select {
			case <-op.Done():
			case <-time.After(time.Second):
				t.Fatal("committed completion did not finish")
			}
			require.True(t, op.Committed())
			select {
			case <-completions:
			default:
				t.Fatal("committed completion omitted")
			}
			_, err = base.GetNode("consensus_a:old")
			require.True(t, errors.Is(err, storage.ErrNotFound))
			_, err = base.GetNode("consensus_b:peer")
			require.NoError(t, err)
			_, err = base.CreateNode(&storage.Node{ID: "consensus_a:fresh"})
			require.NoError(t, err)
		})
	}
}
