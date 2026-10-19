package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Model consensus order: command 1 outlives its ACK timeout, then applies
// immediately before command 2, whose quota admission has already completed.
type xhighOrderedLateReplicator struct {
	Replicator
	adapter   *StorageAdapter
	pending   *Command
	delayNext bool
}

func (r *xhighOrderedLateReplicator) Apply(cmd *Command, timeout time.Duration) error {
	if r.delayNext {
		r.delayNext = false
		r.pending = cmd
		return context.DeadlineExceeded
	}
	if r.pending != nil {
		pending := r.pending
		r.pending = nil
		if err := r.adapter.ApplyCommand(pending); err != nil {
			return err
		}
	}
	return r.Replicator.Apply(cmd, timeout)
}
func TestXHighLateReplicationQuotaAdmission(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	defer local.Shutdown()
	router := &xhighOrderedLateReplicator{Replicator: local, adapter: adapter}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, router, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("late_quota"))
	view, err := manager.GetStorage("late_quota")
	require.NoError(t, err)
	require.NoError(t, manager.SetDatabaseLimits("late_quota", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
	router.delayNext = true
	_, err = view.CreateNode(&storage.Node{ID: "first", Labels: []string{"Doc"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, secondErr := view.CreateNode(&storage.Node{ID: "second", Labels: []string{"Doc"}})
	count, err := view.NodeCount()
	require.NoError(t, err)
	_, firstErr := storage.GetStoredNode(view, "first")
	_, secondReadErr := storage.GetStoredNode(view, "second")
	t.Logf("consensus order first then second: count=%d secondCommit=%v firstRead=%v secondRead=%v", count, secondErr, firstErr, secondReadErr)
	require.LessOrEqual(t, count, int64(1), "a pending replicated command must retain its quota reservation or re-admit subsequent writes at apply")
}
