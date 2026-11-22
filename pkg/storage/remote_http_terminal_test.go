package storage

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// HTTP transaction closure is a response fact, not an inference from every
// statement error. Unknown outcomes still owe their normal rollback request.
func TestRemoteHTTPStatementTerminalObservation(t *testing.T) {
	const primary = `{"code":"Neo.ClientError.Statement.ArithmeticError","message":"/ by zero"}`
	const cleanup = `{"code":"Neo.DatabaseError.Transaction.RollbackFailed","message":"remote cleanup unconfirmed"}`
	for _, tc := range []struct {
		name, body     string
		status         int
		transportError bool
		closed         bool
	}{
		{name: "closed statement", status: 200, body: `{"results":[],"errors":[` + primary + `]}`, closed: true},
		{name: "closed with cleanup failure", status: 200, body: `{"results":[],"errors":[` + primary + `,` + cleanup + `]}`, closed: true},
		{name: "still open", status: 200, body: `{"results":[],"errors":[` + primary + `],"transaction":{"expires":"Thu, 3 Aug 2023 11:45:20 GMT"}}`},
		{name: "non-success response", status: 401, body: `{"results":[],"errors":[` + primary + `]}`},
		{name: "invalid response", status: 200, body: `{"results":`},
		{name: "transport failure", transportError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ledger remoteTxWire
			base := newRemoteExplicitTxWire(t, "terminal", &ledger)
			executions := 0
			engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPost && req.URL.Path == "/db/remote_db/tx/terminal" {
					executions++
					if tc.transportError {
						return nil, errors.New("statement transport unavailable")
					}
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				}
				return base(req)
			})
			tx, err := engine.BeginCypherTx(t.Context(), RemoteAccessWrite)
			require.NoError(t, err)
			_, _, statementErr := tx.QueryCypher(t.Context(), "RETURN 1 / 0", nil)
			require.Error(t, statementErr)
			if tc.name == "closed with cleanup failure" {
				require.Contains(t, statementErr.Error(), "/ by zero")
				require.Contains(t, statementErr.Error(), "remote cleanup unconfirmed")
			}
			if tc.closed {
				require.NoError(t, tx.(*remoteCypherTx).RenewLease(t.Context()))
				require.Equal(t, 1, executions, "confirmed closed transaction cannot be renewed")
				_, _, err = tx.QueryCypher(t.Context(), "RETURN 2", nil)
				require.Error(t, err)
				require.Equal(t, 1, executions, "closed query does not reach the wire")
				err = tx.Commit(t.Context())
				require.Error(t, err, "an aborted transaction cannot report successful commit")
				require.Contains(t, err.Error(), "/ by zero")
				require.ErrorContains(t, tx.Commit(t.Context()), "/ by zero", "repeated commit must not turn an abort into success")
				require.False(t, ledger.committed)
			}
			require.NoError(t, tx.Rollback(t.Context()))
			require.Equal(t, !tc.closed, ledger.rolledBack, "only observed closure discharges the rollback request")
			if tc.closed {
				require.ErrorContains(t, tx.Commit(t.Context()), "/ by zero", "cleanup cannot turn an abort into successful commit")
				require.False(t, ledger.committed)
			}
		})
	}
}

func TestRemoteHTTPUnknownStatementOutcomeRetainsRollbackFailure(t *testing.T) {
	var ledger remoteTxWire
	base := newRemoteExplicitTxWire(t, "unknown", &ledger)
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/db/remote_db/tx/unknown" {
			if req.Method == http.MethodDelete {
				return nil, errors.New("rollback transport unavailable")
			}
			return nil, errors.New("statement transport unavailable")
		}
		return base(req)
	})
	tx, err := engine.BeginCypherTx(t.Context(), RemoteAccessWrite)
	require.NoError(t, err)
	_, _, err = tx.QueryCypher(t.Context(), "RETURN 1", nil)
	require.ErrorContains(t, err, "statement transport unavailable")
	require.ErrorContains(t, tx.Rollback(t.Context()), "rollback transport unavailable")
}
