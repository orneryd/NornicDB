package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type catalogForwarder struct {
	Replicator
	leader    ContextReplicator
	forwarded chan struct{}
}

func (f *catalogForwarder) IsLeader() bool { return false }
func (f *catalogForwarder) ApplyContext(ctx context.Context, cmd *Command, timeout time.Duration) error {
	// Model the wire boundary: process-local operation pointers and contexts do
	// not travel to the leader. Local publication still arrives through replay.
	encoded, err := encodeGob(cmd)
	if err != nil {
		return err
	}
	var wire Command
	if err := decodeGob(encoded, &wire); err != nil {
		return err
	}
	err = f.leader.ApplyContext(context.Background(), &wire, timeout)
	close(f.forwarded)
	return err
}

func TestNamespaceCreateForwardedWaitsForLocalCatalogPublication(t *testing.T) {
	primary, standby := storage.NewMemoryEngine(), storage.NewMemoryEngine()
	defer primary.Close()
	defer standby.Close()
	leaderStorage, err := NewStorageAdapterWithWAL(primary, t.TempDir())
	require.NoError(t, err)
	defer leaderStorage.Close()
	followerStorage, err := NewStorageAdapterWithWAL(standby, t.TempDir())
	require.NoError(t, err)
	defer followerStorage.Close()
	leader := NewStandaloneReplicator(DefaultConfig(), leaderStorage)
	require.NoError(t, leader.Start(context.Background()))
	_, err = multidb.NewDatabaseManager(NewReplicatedEngine(primary, leader, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, leaderStorage.FlushWAL())
	initial, err := leaderStorage.GetWALEntries(0, 100)
	require.NoError(t, err)
	applier := NewWALApplier(followerStorage)
	_, err = applier.ApplyBatch(initial)
	require.NoError(t, err)
	forwarding := &catalogForwarder{Replicator: leader, leader: leader, forwarded: make(chan struct{})}
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(standby, forwarding, time.Second), nil)
	require.NoError(t, err)
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { result <- manager.CreateDatabaseContext(ctx, "forwarded_create") }()
	select {
	case <-forwarding.forwarded:
	case <-time.After(time.Second):
		t.Fatal("CREATE did not reach leader")
	}
	require.False(t, manager.Exists("forwarded_create"))
	select {
	case err := <-result:
		t.Fatalf("CREATE returned before local catalog publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	_, err = manager.GetStorage("nornic")
	require.NoError(t, err, "a forwarded CREATE must not hold the manager lock while waiting for replay")
	require.NoError(t, leaderStorage.FlushWAL())
	entries, err := leaderStorage.GetWALEntries(initial[len(initial)-1].Position, 100)
	require.NoError(t, err)
	_, err = applier.ApplyBatch(entries)
	require.NoError(t, err)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("local replay did not finish forwarded CREATE")
	}
	require.True(t, manager.Exists("forwarded_create"))
}
