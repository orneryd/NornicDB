package replication

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A follower must not advance past an unreadable command in the requested
// WAL window: doing so would silently omit an acknowledged write.
func TestStorageAdapterRejectsUnreadableReplicationWALRecords(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, tc := range []struct {
		name string
		bad  any
	}{
		{name: "malformed command", bad: map[string]any{"ts": 2, "cmd": "not a command"}},
		{name: "missing command", bad: map[string]any{"ts": 2, "cmd": nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
			require.NoError(t, err)
			defer wal.Close()
			makeRecord := func(id string) replicationWALRecord {
				return replicationWALRecord{Timestamp: 1, Command: &Command{Type: CmdDeleteNode, Data: []byte(id), Timestamp: time.Unix(1, 0)}}
			}
			require.NoError(t, wal.Append(storage.OperationType("replication_command"), makeRecord("before")))
			require.NoError(t, wal.Append(storage.OperationType("replication_command"), tc.bad))
			require.NoError(t, wal.Append(storage.OperationType("replication_command"), makeRecord("after")))
			require.NoError(t, wal.Sync())

			adapter := &StorageAdapter{walDir: dir}
			entries, err := adapter.GetWALEntries(0, 10)
			require.Error(t, err)
			require.Nil(t, entries, "a partial window must not be returned as success")

			// A bounded request that ends before the bad record remains usable.
			entries, err = adapter.GetWALEntries(0, 1)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, []byte("before"), entries[0].Command.Data)

			// A caller that has already acknowledged the bad position may read
			// the later record without decoding an earlier, out-of-window entry.
			entries, err = adapter.GetWALEntries(2, 10)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.Equal(t, []byte("after"), entries[0].Command.Data)
		})
	}
}

func TestStorageAdapterRejectsNegativeAndEmptyWALWindows(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	defer wal.Close()
	for _, name := range []string{"before", "after"} {
		require.NoError(t, wal.Append(storage.OperationType("replication_command"), replicationWALRecord{
			Command: &Command{Type: CmdDeleteNode, Data: []byte(name)},
		}))
	}
	require.NoError(t, wal.Sync())
	for _, tc := range []struct {
		name    string
		adapter *StorageAdapter
	}{
		{name: "disk", adapter: &StorageAdapter{walDir: dir}},
		{name: "memory", adapter: &StorageAdapter{walDir: dir, memWAL: []*WALEntry{{Position: 1}, {Position: 2}}}},
	} {
		t.Run(tc.name+"/zero", func(t *testing.T) {
			entries, err := tc.adapter.GetWALEntries(1, 0)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
		t.Run(tc.name+"/large", func(t *testing.T) {
			require.NotPanics(t, func() {
				entries, err := tc.adapter.GetWALEntries(1, int(^uint(0)>>1))
				require.NoError(t, err)
				require.Len(t, entries, 1)
			})
		})
		t.Run(tc.name+"/negative", func(t *testing.T) {
			require.NotPanics(t, func() {
				entries, err := tc.adapter.GetWALEntries(1, -1)
				require.Error(t, err)
				require.Nil(t, entries)
			})
		})
	}
}
