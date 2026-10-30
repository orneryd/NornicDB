// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestAcceptedRetryPreservesNativeDurability(t *testing.T) {
	for _, mode := range []EntityCommitMode{CommitModeExplicit, CommitModeImplicit, CommitModeDirect} {
		t.Run(map[EntityCommitMode]string{CommitModeExplicit: "explicit", CommitModeImplicit: "implicit", CommitModeDirect: "direct"}[mode], func(t *testing.T) {
			base, wal, owner, dir := acceptedCloseFixture(t)
			_, err := owner.CreateNode(&Node{ID: "test:old", Properties: map[string]any{"v": "before"}})
			require.NoError(t, err)
			require.NoError(t, base.PutAccessMeta("test:old", &knowledgepolicy.AccessMetaEntry{TargetID: "test:old", TargetScope: knowledgepolicy.ScopeNode}))
			tx, err := BeginEntityTransaction(base, mode)
			require.NoError(t, err)
			require.NoError(t, tx.DeleteGraphNode("test:old", CascadeEdges))
			_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
			require.NoError(t, err)
			p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			t.Cleanup(p.Abort)
			// A public policy write invalidates the native snapshot after prepare.
			require.NoError(t, base.PutAccessMeta("test:old", &knowledgepolicy.AccessMetaEntry{TargetID: "test:old", TargetScope: knowledgepolicy.ScopeNode, MutationCount: 1}))
			entered, release := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(release) }) })
			var syncs atomic.Int32
			base.nativeSyncHook = func() {
				if syncs.Add(1) == 1 {
					close(entered)
				}
				<-release // Fence immediately before the real native db.Sync call.
			}
			sequence := wal.Sequence()
			r, err := owner.SubmitEntityCommit(context.Background(), p, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == CommitModeExplicit {
				select {
				case <-entered:
				case <-r.Done():
					t.Fatal("Explicit accepted retry returned success without native sync")
				case <-ctx.Done():
					t.Fatal("Explicit accepted retry never reached native sync")
				}
				select {
				case <-r.Done():
					t.Fatal("client success escaped the native sync fence")
				default:
				}
			}
			unblock.Do(func() { close(release) })
			require.NoError(t, r.Wait(ctx))
			require.Equal(t, CommitDiscarded, p.terminal.State, "first application really conflicted")
			require.ErrorIs(t, p.terminal.Err, ErrConflict)
			if mode == CommitModeExplicit {
				require.EqualValues(t, 1, syncs.Load(), "same accepted command retains Explicit durability")
			} else {
				require.Zero(t, syncs.Load(), "retry preserves the original producer policy")
			}
			require.EqualValues(t, sequence+1, wal.Sequence(), "application never appends another accepted command")
			node, err := base.GetStoredNode("test:accepted")
			require.NoError(t, err)
			require.Equal(t, "committed", node.Properties["v"])
			count, err := base.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			require.NoError(t, base.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:accepted", Properties: map[string]any{"v": "writable"}}}))
			require.NoError(t, owner.Close())
			requireAcceptedCloseReplay(t, dir)
		})
	}
}
