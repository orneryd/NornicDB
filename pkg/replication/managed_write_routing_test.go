package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type managedRouteReplicator struct {
	Replicator
	peer    *StorageAdapter
	reject  bool
	calls   int
	delay   bool
	pending *Command
}

func (r *managedRouteReplicator) Apply(cmd *Command, timeout time.Duration) error {
	r.calls++
	if r.reject {
		return ErrNotLeader
	}
	if r.delay {
		r.pending = cmd
		return context.DeadlineExceeded
	}
	if err := r.Replicator.Apply(cmd, timeout); err != nil {
		return err
	}
	// Only wire fields cross to the peer, as with replication transport/WAL.
	return r.peer.ApplyCommand(&Command{Type: cmd.Type, Data: cmd.Data, Timestamp: cmd.Timestamp, RequestID: cmd.RequestID})
}

func TestManagedReplicationLateApplyUsesAccountingOwner(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	peer := storage.NewMemoryEngine()
	defer peer.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	defer remote.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	r := &managedRouteReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("late_apply"))
	view, err := manager.GetStorage("late_apply")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}})
	require.NoError(t, err)
	before, _, _ := manager.GetStorageSize("late_apply")
	r.delay = true
	err = view.UpdateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"payload": "late committed body"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, nornicerrors.IsCommitRolledBack(err), "replication timeout cannot certify logical rollback")
	require.NotNil(t, r.pending)
	unchanged, _, _ := manager.GetStorageSize("late_apply")
	require.Equal(t, before, unchanged)
	// Consensus can apply after the originating staged transaction is gone.
	require.NoError(t, adapter.ApplyCommand(r.pending))
	node, err := view.GetNode("n")
	require.NoError(t, err)
	require.Equal(t, "late committed body", node.Properties["payload"])
	tracked, _, _ := manager.GetStorageSize("late_apply")
	recount, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	exact, _, _ := recount.GetStorageSize("late_apply")
	require.Equal(t, exact, tracked)
	require.Greater(t, tracked, before)
}

func TestManagedReplicationModesPreserveNativeJournal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, mode := range []string{"standalone", "ha", "raft"} {
		t.Run(mode, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			journalDir := t.TempDir()
			wal, err := storage.NewWAL(journalDir, storage.DefaultWALConfig())
			require.NoError(t, err)
			journal := storage.NewWALEngine(base, wal)
			defer journal.Close()
			adapter, err := NewStorageAdapterWithWAL(journal, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			cfg := DefaultConfig()
			var r Replicator
			switch mode {
			case "standalone":
				r = NewStandaloneReplicator(cfg, adapter)
			case "ha":
				cfg.Mode, cfg.NodeID = ModeHAStandby, "managed-primary"
				cfg.HAStandby.Role, cfg.HAStandby.PeerAddr, cfg.HAStandby.SyncMode = "primary", "managed-peer:7000", SyncAsync
				ha, err := NewHAStandbyReplicator(cfg, adapter)
				require.NoError(t, err)
				ha.SetTransport(NewMockTransport())
				r = ha
			case "raft":
				cfg.Mode, cfg.NodeID, cfg.Raft.Bootstrap = ModeRaft, "managed-leader", true
				raft, err := NewRaftReplicator(cfg, adapter)
				require.NoError(t, err)
				raft.SetTransport(NewMockTransport())
				r = raft
			}
			require.NoError(t, r.Start(context.Background()))
			defer r.Shutdown()
			manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(journal, r, 5*time.Second), nil)
			require.NoError(t, err)
			require.NoError(t, manager.CreateDatabase("mode_writes"))
			view, err := manager.GetStorage("mode_writes")
			require.NoError(t, err)
			manager.GetStorageSize("mode_writes")
			before := wal.Sequence()
			_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}})
			require.NoError(t, err)
			require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a"}, {ID: "b"}}))
			require.NoError(t, view.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
			require.NoError(t, view.UpdateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "changed"}}))
			require.NoError(t, view.UpdateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "changed"}}))
			require.NoError(t, view.DeleteEdge("e"))
			require.NoError(t, view.DeleteNode("n"))
			require.NoError(t, wal.Sync())
			entries, err := storage.ReadWALEntriesAfterFromDir(journalDir, before)
			require.NoError(t, err)
			committed := make(map[string]bool)
			bodies := 0
			for _, entry := range entries {
				if entry.Operation == storage.OpTxCommit {
					committed[storage.GetEntryTxID(entry)] = true
				}
			}
			for _, entry := range entries {
				switch entry.Operation {
				case storage.OpCreateNode, storage.OpUpdateNode, storage.OpDeleteNode, storage.OpCreateEdge, storage.OpUpdateEdge, storage.OpDeleteEdge, storage.OpBulkNodes:
					require.True(t, committed[storage.GetEntryTxID(entry)], "every accepted redo body has a successful outcome")
					bodies++
				}
			}
			require.Len(t, committed, 7)
			require.Equal(t, 7, bodies, "one redo envelope per accepted command, with separate outcome markers")
			recovered := storage.NewMemoryEngine()
			defer recovered.Close()
			require.False(t, storage.ReplayWALEntries(recovered, entries).HasCriticalErrors())
			recoveredView := storage.NewNamespacedEngine(recovered, "mode_writes")
			count, err := recoveredView.NodeCount()
			require.NoError(t, err)
			require.Equal(t, int64(2), count)
			_, err = recoveredView.GetNode("n")
			require.ErrorIs(t, err, storage.ErrNotFound)
			tracked, _, _ := manager.GetStorageSize("mode_writes")
			recount, err := multidb.NewDatabaseManager(journal, nil)
			require.NoError(t, err)
			exact, _, _ := recount.GetStorageSize("mode_writes")
			require.Equal(t, exact, tracked)
		})
	}
}

func TestManagedDirectWritesPreserveReplication(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, reject := range []bool{false, true} {
			for _, route := range []string{"create-node", "update-node", "delete-node", "create-edge", "update-edge", "delete-edge", "bulk-node", "bulk-edge"} {
				t.Run(fmt.Sprintf("buffered=%v/reject=%v/%s", buffered, reject, route), func(t *testing.T) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer base.Close()
					peer := storage.NewMemoryEngine()
					defer peer.Close()
					adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
					require.NoError(t, err)
					defer adapter.Close()
					remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
					require.NoError(t, err)
					defer remote.Close()
					local := NewStandaloneReplicator(DefaultConfig(), adapter)
					require.NoError(t, local.Start(context.Background()))
					r := &managedRouteReplicator{Replicator: local, peer: remote}
					manager, err := multidb.NewDatabaseManager(storage.NewTracedEngine(NewReplicatedEngine(base, r, time.Second)), nil)
					require.NoError(t, err)
					defer manager.Close()
					require.NoError(t, manager.CreateDatabase("routed"))
					view, err := manager.GetStorage("routed")
					require.NoError(t, err)
					// Both copies start from the same accepted command history.
					// Reset admission observations below so this still checks the
					// selected route's one replication call, including rejection.
					require.NoError(t, view.BulkCreateNodes([]*storage.Node{{ID: "a", Labels: []string{"Doc"}}, {ID: "b", Labels: []string{"Doc"}}, {ID: "n", Labels: []string{"Doc"}}}))
					require.NoError(t, view.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
					manager.GetStorageSize("routed")
					before, _, _ := manager.GetStorageSize("routed")
					r.reject, r.calls = reject, 0
					switch route {
					case "create-node":
						_, err = view.CreateNode(&storage.Node{ID: "fresh", Labels: []string{"Doc"}})
					case "update-node":
						err = view.UpdateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"v": "changed"}})
					case "delete-node":
						err = view.DeleteNode("n")
					case "create-edge":
						err = view.CreateEdge(&storage.Edge{ID: "fresh", StartNode: "a", EndNode: "b", Type: "LINK"})
					case "update-edge":
						err = view.UpdateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"v": "changed"}})
					case "delete-edge":
						err = view.DeleteEdge("e")
					case "bulk-node":
						err = view.BulkCreateNodes([]*storage.Node{{ID: "fresh"}, {ID: "fresh2"}})
					case "bulk-edge":
						err = view.BulkCreateEdges([]*storage.Edge{{ID: "fresh", StartNode: "a", EndNode: "b", Type: "LINK"}, {ID: "fresh2", StartNode: "a", EndNode: "b", Type: "LINK"}})
					}
					require.Equal(t, 1, r.calls, "managed writes must reach replication admission exactly once")
					if reject {
						require.ErrorIs(t, err, ErrNotLeader)
						after, _, _ := manager.GetStorageSize("routed")
						require.Equal(t, before, after)
					} else {
						require.NoError(t, err)
					}
					require.NoError(t, base.FlushWriteBehind())
					localView := storage.NewNamespacedEngine(base, "routed")
					peerView := storage.NewNamespacedEngine(peer, "routed")
					for _, id := range []storage.NodeID{"a", "b", "n", "fresh", "fresh2"} {
						a, aerr := storage.GetStoredNode(localView, id)
						b, berr := storage.GetStoredNode(peerView, id)
						require.Equal(t, aerr == nil, berr == nil)
						if aerr == nil {
							require.Equal(t, a.Properties, b.Properties)
						}
					}
					for _, id := range []storage.EdgeID{"e", "fresh", "fresh2"} {
						a, aerr := storage.GetStoredEdge(localView, id)
						b, berr := storage.GetStoredEdge(peerView, id)
						require.Equal(t, aerr == nil, berr == nil)
						if aerr == nil {
							require.Equal(t, a.Properties, b.Properties)
						}
					}
					tracked, _, _ := manager.GetStorageSize("routed")
					recount, err := multidb.NewDatabaseManager(base, nil)
					require.NoError(t, err)
					exact, _, _ := recount.GetStorageSize("routed")
					require.Equal(t, exact, tracked, "local replication apply must publish its admitted accounting batch once")
				})
			}
		}
	}
}
