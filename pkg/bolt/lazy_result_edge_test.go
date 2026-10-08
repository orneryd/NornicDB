package bolt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type panickingExecutor struct{}

func (panickingExecutor) Execute(context.Context, string, map[string]any) (*QueryResult, error) {
	panic("statement panicked")
}

// panicAfterExecutor runs the statement and then panics, as a statement
// that panics after its last row would.
type panicAfterExecutor struct{ inner QueryExecutor }

func (p panicAfterExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	if _, err := p.inner.Execute(ctx, query, params); err != nil {
		return nil, err
	}
	panic("statement panicked")
}

// A statement that panics after it started streaming fails the PULL that
// reaches its end, and the panic goes on as the statement's; one that
// panics before panics at RUN, as any statement does.
func TestLazyResultStatementPanics(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := cypher.NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "UNWIND range(1, 1500) AS i CREATE (:Item {k: i})", nil)
	require.NoError(t, err)
	conn, client := net.Pipe()
	t.Cleanup(func() { _ = conn.Close(); _ = client.Close() })
	session := newTestSession(conn, panicAfterExecutor{inner: &cypherQueryExecutor{executor: exec}})
	session.messageQueue = make(chan *boltMessage, 4)
	session.readErrQueue = make(chan error, 1)
	session.messageQueue <- &boltMessage{msgType: MsgPull, data: BuildPullMessage(map[string]any{"n": int64(-1)})[2:]}

	replies := make(chan []byte, 1)
	go func() {
		var types []byte
		for {
			msgType, _, err := ReadMessage(client)
			if err != nil {
				replies <- types
				return
			}
			if msgType != MsgRecord {
				types = append(types, msgType)
			}
		}
	}()
	func() {
		defer func() { require.Equal(t, "statement panicked", recover()) }()
		_ = session.handleRun(BuildRunMessage("MATCH (n:Item) RETURN n.k AS k", nil, nil)[2:])
	}()
	_ = conn.Close()
	require.Equal(t, []byte{MsgSuccess, MsgFailure}, <-replies)

	func() {
		defer func() { require.Equal(t, "statement panicked", recover()) }()
		_, _, _ = newTestSession(&mockConn{}, panickingExecutor{}).runStreamed(context.Background(), func() {}, panickingExecutor{}, "RETURN 1", nil, "", time.Now())
	}()
}

// A connection that fails or closes while a result streams ends the
// statement, and the message loop then sees the failure.
func TestLazyResultEndsWhenTheConnectionGoes(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	exec := cypher.NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "UNWIND range(1, 1500) AS i CREATE (:Item {k: i})", nil)
	require.NoError(t, err)
	readFailure := errors.New("connection reset")
	for _, tc := range []struct {
		name string
		gone func(*Session)
		want error
	}{
		{"read error", func(s *Session) { s.readErrQueue <- readFailure }, readFailure},
		{"reader closed", func(s *Session) { s.messageQueue <- nil }, io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &mockConn{}
			session := newTestSession(conn, &cypherQueryExecutor{executor: exec})
			session.messageQueue = make(chan *boltMessage, 4)
			session.readErrQueue = make(chan error, 1)
			tc.gone(session)
			require.NoError(t, session.handleRun(BuildRunMessage("MATCH (n:Item) RETURN n.k AS k", nil, nil)[2:]))
			require.Nil(t, session.lastLazy)
			_, err := session.nextQueuedMessage()
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// A PULL that reaches a statement RESET cancelled is IGNORED.
func TestLazyResultFailureAfterResetIsIgnored(t *testing.T) {
	conn := &mockConn{}
	session := newTestSession(conn, nil)
	failure := &lazyStatementError{err: context.Canceled, reason: MsgReset}
	require.Equal(t, context.Canceled.Error(), failure.Error())
	require.NoError(t, session.sendLazyFailure(failure))
	require.True(t, bytes.Contains(conn.writeData, ignoredMessage))
}

// A DISCARD that reaches the statement's error fails with it.
func TestBoltLazyResultDiscardReachesTheError(t *testing.T) {
	conn := lazyResultTestConn(t, 0)
	requireNoError(t, SendRun(t, conn, "UNWIND range(1, 5000) AS i RETURN 10 / (i - 2000) AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	sendDiscard(t, conn, 3000)
	if _, _, failure := readBatch(t, conn); failure != "Neo.ClientError.Statement.ArithmeticError" {
		t.Fatalf("DISCARD failure = %q", failure)
	}
}
