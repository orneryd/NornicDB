package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type producerModeLeaderStorage struct {
	Storage
	mu    sync.Mutex
	modes []storage.EntityCommitMode
}

func (s *producerModeLeaderStorage) PrepareEntityCommand(ctx context.Context, c *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	return s.Storage.(*StorageAdapter).PrepareEntityCommand(ctx, c)
}
func (s *producerModeLeaderStorage) ApplyCommand(_ context.Context, c *Command) error {
	if c.Type == CmdCommitTransaction {
		e, _, err := storage.DecodeEntityTransaction(c.Data)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.modes = append(s.modes, e.Mode)
		s.mu.Unlock()
	}
	return s.Storage.ApplyCommand(context.Background(), c)
}

type producerModeFollower struct {
	Replicator
	leader *StandaloneReplicator
}

func (r *producerModeFollower) IsLeader() bool { return false }
func (r *producerModeFollower) Apply(c *Command, timeout time.Duration) error {
	return r.leader.Apply(c, timeout)
}
func TestManagedFollowerPreservesSingleItemBulkProducerMode(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "single-item-bulk"}[bulk], func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			fixtureowner.OwnChecked(t, base)
			walDir := t.TempDir()
			wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
			require.NoError(t, err)
			fixtureowner.OwnChecked(t, wal, base)
			base.SetBufferedCommitWAL(wal)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = adapter.Close() })
			capture := &producerModeLeaderStorage{Storage: adapter}
			leader := NewStandaloneReplicator(&Config{}, capture)
			require.NoError(t, leader.Start(context.Background()))
			t.Cleanup(func() { _ = leader.Shutdown() })
			local, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = local.Close() })
			follower := NewReplicatedEngine(local, &producerModeFollower{Replicator: leader, leader: leader}, time.Second)
			manager, err := multidb.NewDatabaseManager(t.Context(), follower, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			managed, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			require.NoError(t, base.FlushWriteBehind(nil))
			before := wal.Sequence()
			capture.mu.Lock()
			capture.modes = nil
			capture.mu.Unlock()
			node := &storage.Node{ID: "mode", Properties: map[string]any{"value": int64(42)}}
			if bulk {
				err = managed.BulkCreateNodes([]*storage.Node{node})
			} else {
				_, err = managed.CreateNode(node)
			}
			require.NoError(t, err)
			capture.mu.Lock()
			modes := append([]storage.EntityCommitMode(nil), capture.modes...)
			capture.mu.Unlock()
			want := storage.CommitModeDirect
			if bulk {
				want = storage.CommitModeImplicit
			}
			require.Equal(t, []storage.EntityCommitMode{want}, modes)
			entries, err := storage.ReadWALEntriesFromDir(walDir)
			require.NoError(t, err)
			acks := 0
			for _, entry := range entries {
				if entry.Sequence > before && entry.Operation == storage.OpBufferedCommit {
					acks++
				}
			}
			wantACK := 0
			if bulk {
				wantACK = 1
			}
			require.Equal(t, wantACK, acks, "successful receipt retains producer ACK policy")
			got, err := base.GetStoredNode("nornic:mode")
			require.NoError(t, err)
			require.Equal(t, int64(42), got.Properties["value"])
			_, err = local.GetStoredNode("nornic:mode")
			require.ErrorIs(t, err, storage.ErrNotFound)
			route, found := storage.EntityCommitSubmitterFor(follower)
			require.True(t, found)
			require.ErrorIs(t, route.CheckAdmission(), ErrNotLeader)
		})
	}
}
