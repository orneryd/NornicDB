package bolt

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A queued PULL must not hide TCP EOF from the active RUN's connection context.
func TestMessageQueueDisconnectCancelsActiveRun(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		t.Skip("kernel hangup probe supports Darwin, Linux and Windows")
	}
	for _, extra := range []int{0, 10000} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) { testMessageQueueDisconnect(t, extra) })
	}
}

func testMessageQueueDisconnect(t *testing.T, extra int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &Session{conn: conn, reader: bufio.NewReader(conn), messageQueue: make(chan *boltMessage, 16), readErrQueue: make(chan error, 1), connCtx: ctx, connCancel: cancel}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	session.setActiveRun(runCancel)
	session.startMessageReader()
	for i := 0; i < 17; i++ {
		_, err = client.Write([]byte{0, 2, 0xb0, MsgPull, 0, 0})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return len(session.messageQueue) == 16 }, time.Second, time.Millisecond)
	select {
	case <-runCtx.Done():
		t.Fatal("open pipelined connection must not cancel RUN")
	case <-time.After(50 * time.Millisecond):
	}
	if extra > 0 {
		_, err = client.Write(bytes.Repeat([]byte{0, 2, 0xb0, MsgPull, 0, 0}, extra))
		require.NoError(t, err)
	}
	require.NoError(t, client.Close())
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("full message queue hid peer disconnect from active RUN")
	}
}

func TestMessageQueueOpenConnectionPreservesOrder(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Session{conn: left, messageQueue: make(chan *boltMessage, 1), connCtx: ctx}
	first, second := &boltMessage{msgType: MsgPull}, &boltMessage{msgType: MsgRun}
	require.True(t, s.enqueueMessage(first, ctx.Done()))
	done := make(chan bool, 1)
	go func() { done <- s.enqueueMessage(second, ctx.Done()) }()
	time.Sleep(50 * time.Millisecond)
	require.Same(t, first, <-s.messageQueue)
	require.True(t, <-done)
	require.Same(t, second, <-s.messageQueue)
	require.NoError(t, ctx.Err())
	// A blocked non-socket adapter must still leave promptly on cancellation.
	s.messageQueue <- first
	go func() { done <- s.enqueueMessage(second, ctx.Done()) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.False(t, <-done)
	require.False(t, s.enqueueMessage(second, ctx.Done()))
}

func TestPeerProbeTransportUnwrapping(t *testing.T) {
	require.Nil(t, socketConn(nil))
	require.Nil(t, socketConn(&wsConn{}))
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	require.Nil(t, socketConn(a))
	require.Nil(t, socketConn(&peekedConn{Conn: a}))
	require.Nil(t, socketConn(tls.Client(a, &tls.Config{InsecureSkipVerify: true})))
	closed, release := newPeerCloseProbe(nil)
	require.Nil(t, closed)
	require.Nil(t, release)
}

func TestMessageReaderCancellationAndEmptyMessages(t *testing.T) {
	for _, kind := range []byte{MsgReset, MsgGoodbye} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			s := &Session{conn: a, reader: bufio.NewReader(a), messageQueue: make(chan *boltMessage, 1), readErrQueue: make(chan error, 1), connCtx: ctx, connCancel: cancel}
			s.setActiveRun(runCancel)
			s.startMessageReader()
			_, err := b.Write([]byte{0, 0, 0, 2, 0xb0, kind, 0, 0})
			require.NoError(t, err)
			select {
			case <-runCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("control message did not cancel RUN")
			}
			require.Equal(t, kind, (<-s.messageQueue).msgType)
			require.NoError(t, b.Close())
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("EOF did not cancel connection")
			}
		})
	}
	(&Session{}).startMessageReader()
}

func TestPeerCloseProbeResumesWithoutCancelling(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	conn, err := listener.Accept()
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Session{conn: conn, messageQueue: make(chan *boltMessage, 1), connCtx: ctx}
	first, second := &boltMessage{msgType: MsgPull}, &boltMessage{msgType: MsgRun}
	s.messageQueue <- first
	done := make(chan bool, 1)
	go func() { done <- s.enqueueMessage(second, ctx.Done()) }()
	time.Sleep(60 * time.Millisecond)
	require.NoError(t, ctx.Err())
	require.Same(t, first, <-s.messageQueue)
	require.True(t, <-done)
	require.Same(t, second, <-s.messageQueue)
	require.NoError(t, ctx.Err())
	closed, release := newPeerCloseProbe(conn)
	if closed != nil {
		if release != nil {
			defer release()
		}
		require.NoError(t, conn.Close())
		require.True(t, closed())
	}
	// Building a watcher on an already closed connection must release its resources.
	probe, cleanup := newPeerCloseProbe(conn)
	if cleanup != nil {
		cleanup()
	}
	_ = probe
}
