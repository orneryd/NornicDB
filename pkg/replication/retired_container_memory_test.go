package replication

import (
	"runtime"
	"testing"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetiredReplicationWALCollectible(t *testing.T) {
	owner := &StorageAdapter{}
	reference := func() weak.Pointer[WALEntry] {
		entry := &WALEntry{Position: 1}
		owner.memWAL = []*WALEntry{entry}
		ref := weak.Make(entry)
		owner.PruneWALEntries(1)
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}

func TestRetiredReplicationBatchCollectible(t *testing.T) {
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	owner := &StorageAdapter{wal: wal}
	reference := func() weak.Pointer[walWriteRequest] {
		request := &walWriteRequest{barrier: true, posCh: make(chan uint64, 1)}
		owner.walBatch = []*walWriteRequest{request}
		ref := weak.Make(request)
		owner.flushWALBatch()
		require.Equal(t, wal.Sequence(), <-request.posCh)
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}

func TestRetiredRaftConflictTailCollectible(t *testing.T) {
	owner, _ := newCoverageRaftReplicator(t)
	owner.currentTerm = 1
	reference := func() weak.Pointer[RaftLogEntry] {
		retired := &RaftLogEntry{Index: 2, Term: 1}
		owner.log = []*RaftLogEntry{{Index: 0}, {Index: 1, Term: 1}, retired}
		ref := weak.Make(retired)
		response := owner.handleAppendEntriesRequest(&AppendEntriesRequest{
			Term: 2, LeaderID: "leader", PrevLogIndex: 0,
			Entries: []*RaftLogEntry{{Index: 1, Term: 2}},
		})
		require.True(t, response.Success)
		require.Len(t, owner.log, 2)
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}
