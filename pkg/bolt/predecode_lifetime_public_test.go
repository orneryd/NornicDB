package bolt

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestPredecodeReservationLifetime(t *testing.T) {
	for _, message := range []byte{MsgHello, 0x6a, MsgBegin, MsgRun, MsgRoute, MsgPull, MsgDiscard} {
		for _, outcome := range []string{"decode-or-handler-error", "pressure", "drop-partial"} {
			t.Run(fmt.Sprintf("%02x/%s", message, outcome), func(t *testing.T) {
				var used atomic.Int64
				used.Store(1)
				policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
				require.NoError(t, policy.Refresh())
				server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) { return &QueryResult{}, nil }})
				server.SetMemoryPolicy(policy)
				port := serveBoltIntegrationServer(t, server)
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
				require.NoError(t, PerformHandshake(conn))
				if message != MsgHello {
					require.NoError(t, SendMessage(conn, BuildHelloMessage(nil)))
					_, _, err = ReadMessage(conn)
					require.NoError(t, err)
					require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
				}
				if outcome == "pressure" {
					used.Store(1 << 20)
					require.NoError(t, policy.Refresh())
				}
				if outcome == "drop-partial" {
					_, err = conn.Write([]byte{0xf0, 0x00, 0xb1, message})
					require.NoError(t, err)
					require.Eventually(t, func() bool { return policy.Reserved() > 0 }, time.Second, time.Millisecond)
				} else {
					require.NoError(t, SendMessage(conn, []byte{0xb1, message, 0xff}))
					_, _, err = ReadMessage(conn)
					if message == 0x6a && outcome == "decode-or-handler-error" {
						require.ErrorContains(t, err, "EOF")
					} else {
						require.NoError(t, err)
					}
				}
				_ = conn.Close()
				require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond, "reader, handler and teardown must release exactly once")
			})
		}
	}
}
