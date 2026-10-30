package bolt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestBoltApplicationDecodeAdmissionAllPayloads(t *testing.T) {
	for _, msg := range []byte{MsgHello, MsgBegin, MsgRun, MsgPull, MsgDiscard} {
		t.Run(fmt.Sprintf("%02x", msg), func(t *testing.T) {
			policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
				return memory.Sample{Used: 1 << 20}, nil
			}, time.Now)
			require.NoError(t, policy.Refresh())
			conn := &mockConn{}
			session := newTestSession(conn, &mockExecutor{})
			session.server.SetMemoryPolicy(policy)
			require.NoError(t, session.dispatchMessage(msg, []byte{0xff}))
			require.Contains(t, string(conn.writeData), "Neo.TransientError.General.MemoryPoolOutOfMemoryError",
				"pressure must reject before decoding malformed client payloads")
			require.Zero(t, policy.Reserved())
		})
	}
}

func TestBoltApplicationDecodeAdmissionChunkGrowth(t *testing.T) {
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: 1}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	var input bytes.Buffer
	chunk := make([]byte, 60<<10)
	chunk[0], chunk[1] = 0xB1, MsgBegin
	for i := 0; i < 4; i++ {
		input.Write([]byte{0xf0, 0x00})
		input.Write(chunk)
	}
	input.Write([]byte{0xf0, 0x00}) // deliberately omit the fifth payload
	session := newTestSession(&mockConn{}, &mockExecutor{})
	session.server.SetMemoryPolicy(policy)
	session.reader = bufio.NewReader(&input)
	_, err := session.readMessage()
	require.ErrorIs(t, err, memory.ErrLimitExceeded, "reject before reading or allocating the over-capacity chunk")
	require.Zero(t, policy.Reserved())
}

func TestBoltApplicationDecodeAdmissionDisabledLargeMessage(t *testing.T) {
	data := append([]byte{0xB1, MsgBegin}, bytes.Repeat([]byte{'x'}, 12<<20)...)
	var input bytes.Buffer
	for offset := 0; offset < len(data); {
		size := min(65535, len(data)-offset)
		input.Write([]byte{byte(size >> 8), byte(size)})
		input.Write(data[offset : offset+size])
		offset += size
	}
	input.Write([]byte{0, 0})
	session := newTestSession(&mockConn{}, &mockExecutor{})
	session.reader = bufio.NewReader(&input)
	msg, err := session.readMessage()
	require.NoError(t, err)
	require.Equal(t, 12<<20, len(msg.data), "disabled policy must not add a ten-MiB Bolt cap")
}

func TestBoltApplicationDecodeAdmissionPublicBeginAndChunkGrowth(t *testing.T) {
	var used atomic.Int64
	used.Store(1)
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: used.Load()}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192},
		&mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
			return &QueryResult{}, nil
		}})
	server.SetMemoryPolicy(policy)
	port := serveBoltIntegrationServer(t, server)
	connect := func() net.Conn {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
		require.NoError(t, PerformHandshake(conn))
		require.NoError(t, SendMessage(conn, BuildHelloMessage(nil)))
		kind, _, err := ReadMessage(conn)
		require.NoError(t, err)
		require.Equal(t, byte(MsgSuccess), kind)
		require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
		return conn
	}
	conn := connect()
	used.Store(1 << 20)
	require.NoError(t, policy.Refresh())
	require.NoError(t, SendMessage(conn, []byte{0xB1, MsgBegin, 0xff}))
	kind, data, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, byte(MsgFailure), kind)
	require.Contains(t, string(data), "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
	_ = conn.Close()
	used.Store(1)
	require.NoError(t, policy.Refresh())
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
	conn = connect()
	// Send complete 60 KiB chunks but no terminator. The fifth chunk's wire
	// forecast exceeds 1 MiB. Rejection must arrive without reading its body,
	// completing the message, or invoking the PackStream decoder.
	chunk := []byte(strings.Repeat("x", 60<<10))
	chunk[0], chunk[1] = 0xB1, MsgBegin
	var header [2]byte
	binary.BigEndian.PutUint16(header[:], uint16(len(chunk)))
	for i := 0; i < 4; i++ {
		_, err = conn.Write(header[:])
		require.NoError(t, err)
		_, err = conn.Write(chunk)
		require.NoError(t, err)
	}
	_, err = conn.Write(header[:])
	require.NoError(t, err)
	kind, data, err = ReadMessage(conn)
	require.NoError(t, err, "chunk growth must fail before waiting for the fifth chunk body")
	require.Equal(t, byte(MsgFailure), kind)
	require.Contains(t, string(data), "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
}

func TestBoltApplicationDecodeAdmissionDisconnectReleasesQueuedWork(t *testing.T) {
	policy := memory.NewPolicy(32<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: 1}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	entered := make(chan struct{})
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192},
		&mockExecutor{executeFunc: func(ctx context.Context, _ string, _ map[string]any) (*QueryResult, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}})
	server.SetMemoryPolicy(policy)
	port := serveBoltIntegrationServer(t, server)
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, PerformHandshake(conn))
	require.NoError(t, SendMessage(conn, BuildHelloMessage(nil)))
	kind, _, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, byte(MsgSuccess), kind)
	require.NoError(t, SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil)))
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("RUN did not enter its executor")
	}
	for i := 0; i < 32; i++ {
		require.NoError(t, SendMessage(conn, []byte{0xB1, MsgBegin, 0xA0}))
	}
	require.Eventually(t, func() bool { return policy.Reserved() >= 1<<20 }, time.Second, time.Millisecond,
		"queued payloads must retain their shared admission budget")
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, 3*time.Second, time.Millisecond,
		"disconnect must join the reader and release every queued message")
}
