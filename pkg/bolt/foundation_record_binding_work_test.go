// Author: Lane #1644 (Codex). A public RUN binds identity metadata once;
// PULL record count must not add capability discovery or type checks.
package bolt

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

func TestFoundationPublicRecordBindingWork(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	manager := &mockDBManager{stores: map[string]storage.Engine{"tenant": storage.NewNamespacedEngine(base, "tenant")}, defaultDB: "tenant"}
	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.RequireAuth = false
	cfg.AllowAnonymous = true
	server := NewWithDatabaseManager(cfg, &mockExecutor{}, manager)
	defer server.Close()
	port := startBoltTestServer(t, server)
	var samples [][2]uint64
	for _, count := range []int{1, 17} {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
		require.NoError(t, PerformHandshake(conn))
		require.NoError(t, SendMessage(conn, BuildHelloMessage(map[string]string{"scheme": "none"})))
		kind, _, err := ReadMessage(conn)
		require.NoError(t, err)
		require.Equal(t, byte(MsgSuccess), kind)
		require.NoError(t, SendMessage(conn, BuildRunMessage("UNWIND range(1,$count) AS i RETURN i", map[string]any{"count": int64(count)}, map[string]any{"db": "tenant"})))
		kind, _, err = ReadMessage(conn)
		require.NoError(t, err)
		require.Equal(t, byte(MsgSuccess), kind)
		work := &storage.CapabilityWork{}
		end := storage.ObserveCapabilityWork(work)
		require.NoError(t, SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(-1)})))
		rows := 0
		for {
			kind, _, err = ReadMessage(conn)
			require.NoError(t, err)
			if kind == MsgRecord {
				rows++
				continue
			}
			require.Equal(t, byte(MsgSuccess), kind)
			break
		}
		end()
		require.Equal(t, count, rows)
		samples = append(samples, [2]uint64{work.ProviderVisits.Load(), work.TypeChecks.Load()})
		require.NoError(t, conn.Close())
	}
	require.Equal(t, samples[0], samples[1], "PULL rows consume the bound result view, not a fresh source lookup")
	require.Equal(t, [2]uint64{}, samples[1], "ordinary records perform no structural discovery")
}
