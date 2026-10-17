package bolt

import (
	"context"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A statement stopped at its deadline fails with Neo4j's transaction timeout,
// not SyntaxError (#1339). Neo4j 5.26 reports a client tx_timeout as
// TransactionTimedOutClientConfiguration; the server's own statement timeout
// is TransactionTimedOut.
func TestBoltDeadlineStatus(t *testing.T) {
	for _, test := range []struct {
		name          string
		serverTimeout time.Duration
		metadata      map[string]any
		code, message string
	}{
		{"client tx_timeout", 0, map[string]any{"tx_timeout": int64(50)},
			nornicerrors.TransactionTimedOutClientConfiguration, nornicerrors.TransactionTimedOutClientConfigurationMessage},
		{"server statement timeout", 50 * time.Millisecond, nil,
			nornicerrors.TransactionTimedOut, nornicerrors.TransactionTimedOutMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.BoltStatementTimeout = test.serverTimeout
			executor := &mockExecutor{executeFunc: func(ctx context.Context, _ string, _ map[string]any) (*QueryResult, error) {
				// The statement runs until its deadline; the executor, which owns
				// the statement's context, marks that as its timeout.
				<-ctx.Done()
				return nil, nornicerrors.MarkStatementTimeout(ctx.Err(), context.Cause(ctx))
			}}
			port := startBoltTestServer(t, New(config, executor))
			conn := openBoltTestConn(t, port)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, SendRun(t, conn, "RETURN 1", nil, test.metadata))
			code, message, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, test.code, code)
			require.Equal(t, test.message, message)
			require.NotEqual(t, nornicerrors.StatementSyntaxError, code)
		})
	}
}

// A streamed auto-commit read (larger than lazyResultThreshold) whose client
// tx_timeout passes between PULLs fails the later PULL with the timeout the
// client configured, as a read answered whole does (review 4300775970).
func TestBoltLazyResultClientTimeoutStatus(t *testing.T) {
	conn := lazyResultTestConn(t, 3000)
	require.NoError(t, SendRun(t, conn, "MATCH (n:Item) RETURN n.k AS k", nil, map[string]any{"tx_timeout": int64(100)}))
	_, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	records, success, failure := pullBatch(t, conn, 10)
	require.Empty(t, failure)
	require.Len(t, records, 10)
	require.Equal(t, true, success["has_more"], "the result is streamed")
	time.Sleep(300 * time.Millisecond)
	_, _, failure = pullBatch(t, conn, -1)
	require.Equal(t, nornicerrors.TransactionTimedOutClientConfiguration, failure)
}
