package replication

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageAdapter_GetWALEntries_ConcurrentPrune(t *testing.T) {
	const size = 2048
	original := make([]*WALEntry, size)
	for i := range original {
		original[i] = &WALEntry{Position: uint64(i + 1)}
	}
	for _, from := range []uint64{1, size + 1} {
		name := "batch"
		if from > size {
			name = "past_end"
		}
		t.Run(name, func(t *testing.T) {
			walDir := t.TempDir()
			for trial := 0; trial < 128; trial++ {
				adapter := &StorageAdapter{walDir: walDir, memWAL: append([]*WALEntry(nil), original...)}
				start := make(chan struct{})
				var workers sync.WaitGroup
				workers.Add(2)
				go func() {
					defer workers.Done()
					defer func() {
						if recovered := recover(); recovered != nil {
							t.Errorf("WAL reader panicked during prune: %v", recovered)
						}
					}()
					<-start
					for read := 0; read < 16; read++ {
						entries, err := adapter.GetWALEntries(from, size)
						if err != nil {
							t.Errorf("read WAL: %v", err)
							return
						}
						// After pruning, the request falls back to the empty disk WAL.
						// Before pruning it must return the complete, ordered window.
						if len(entries) != 0 {
							if from > size || len(entries) != size-1 {
								t.Errorf("incoherent WAL window: from=%d, length=%d", from, len(entries))
								return
							}
							for i, entry := range entries {
								if entry != original[i+1] {
									t.Errorf("WAL window changed at %d: got %p, want %p", i, entry, original[i+1])
									return
								}
							}
						}
						runtime.Gosched()
					}
				}()
				go func() {
					defer workers.Done()
					<-start
					for upto := uint64(size / 4); upto <= size; upto += size / 4 {
						runtime.Gosched()
						adapter.PruneWALEntries(upto)
					}
				}()
				close(start)
				workers.Wait()
				if t.Failed() {
					return
				}
			}
		})
	}
}

func TestStorageAdapter_GetWALEntries_OwnsWindow(t *testing.T) {
	adapter := &StorageAdapter{memWAL: []*WALEntry{{Position: 1}, {Position: 2}, {Position: 3}}}
	entries, err := adapter.GetWALEntries(1, 2)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	first, second := entries[0], entries[1]
	adapter.PruneWALEntries(3)
	adapter.appendToMemWAL(4, replicationWALRecord{}, &Command{Type: CmdCreateNode})
	require.Same(t, first, entries[0])
	require.Same(t, second, entries[1])
	require.Equal(t, uint64(2), entries[0].Position)
	require.Equal(t, uint64(3), entries[1].Position)
}
