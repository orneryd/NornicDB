// request_access_mode_test.go — a client's requested access mode is a
// message-level restriction (Neo4j 5.26: RUN and BEGIN option "mode", "r" for
// read). These tests drive the Bolt message owner directly, so they can send
// the options the official driver sends only on one connection: a READ RUN on
// its own, then a RUN without a mode on the same connection, an explicit
// transaction whose mode outlives its RUNs, and the RESET/COMMIT boundaries
// that must clear it.
package bolt

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// accessModeStatementExecutor records the statements a Bolt message executes,
// so a refused message is proven to have reached no executor.
type accessModeStatementExecutor struct{ queries []string }

func (e *accessModeStatementExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	e.queries = append(e.queries, query)
	return &QueryResult{Columns: []string{"v"}, Rows: [][]any{{int64(1)}}}, nil
}

// requestAccessModeProtocol is one Bolt session on an in-memory connection with
// the recording executor, so a test can send messages and read each response.
// The in-memory connection is unbuffered: the message owner runs on its own
// goroutine while the test reads the response.
type requestAccessModeProtocol struct {
	session *Session
	client  net.Conn
	exec    *accessModeStatementExecutor
}

func newRequestAccessModeProtocol(t *testing.T) *requestAccessModeProtocol {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	exec := &accessModeStatementExecutor{}
	return &requestAccessModeProtocol{session: newTestSession(server, exec), client: client, exec: exec}
}

// runPayload is a RUN payload: statement, parameters, options.
func runPayload(statement string, options map[string]any) []byte {
	if options == nil {
		options = map[string]any{}
	}
	payload := encodePackStreamString(statement)
	payload = append(payload, encodePackStreamMap(map[string]any{})...)
	return append(payload, encodePackStreamMap(options)...)
}

// exchange dispatches one message through the session's message owner and
// returns the single response it answers with.
func (p *requestAccessModeProtocol) exchange(t *testing.T, msgType byte, payload []byte) (byte, map[string]any) {
	t.Helper()
	answered := make(chan error, 1)
	go func() {
		answered <- p.session.dispatchInner(msgType, payload, boltOpName(msgType), nil)
	}()
	require.NoError(t, p.client.SetReadDeadline(time.Now().Add(10*time.Second)))
	marker, data, err := ReadMessage(p.client)
	require.NoError(t, err)
	select {
	case err := <-answered:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the message owner did not return")
	}
	if marker != MsgSuccess && marker != MsgFailure {
		return marker, nil
	}
	metadata, _, err := decodePackStreamMap(data, 0)
	require.NoError(t, err)
	return marker, metadata
}

// expectFailure sends a message and requires that it was refused with code.
func (p *requestAccessModeProtocol) expectFailure(t *testing.T, msgType byte, payload []byte, code string) {
	t.Helper()
	marker, metadata := p.exchange(t, msgType, payload)
	require.Equal(t, byte(MsgFailure), marker)
	require.Equal(t, code, metadata["code"])
	require.Empty(t, p.exec.queries, "a refused message must reach no executor")
}

// reset clears the failed state a FAILURE leaves on the connection.
func (p *requestAccessModeProtocol) reset(t *testing.T) {
	t.Helper()
	marker, _ := p.exchange(t, MsgReset, nil)
	require.Equal(t, byte(MsgSuccess), marker)
}

// expectSuccess requires that the message succeeded.
func (p *requestAccessModeProtocol) expectSuccess(t *testing.T, msgType byte, payload []byte) {
	t.Helper()
	marker, metadata := p.exchange(t, msgType, payload)
	require.Equal(t, byte(MsgSuccess), marker, "%v", metadata)
}

// expectExecuted sends a message and requires that the executor ran statement.
func (p *requestAccessModeProtocol) expectExecuted(t *testing.T, msgType byte, payload []byte, statement string) {
	t.Helper()
	p.expectSuccess(t, msgType, payload)
	require.Equal(t, []string{statement}, p.exec.queries)
	p.exec.queries = nil
}

func TestRequestAccessModeRunIsPerMessage(t *testing.T) {
	p := newRequestAccessModeProtocol(t)

	// A READ RUN refuses an update even though the session, the principal and
	// the database allow writes.
	p.expectFailure(t, MsgRun, runPayload("CREATE (:RunModeRefused)", map[string]any{"mode": "r"}),
		"Neo.ClientError.Statement.AccessMode")
	p.reset(t)

	// The next auto-commit RUN asked for no mode: the refused message's mode
	// did not persist, so the write runs.
	p.expectExecuted(t, MsgRun, runPayload("CREATE (:RunModeAllowed)", nil), "CREATE (:RunModeAllowed)")
}

func TestRequestAccessModeBeginGovernsItsTransaction(t *testing.T) {
	p := newRequestAccessModeProtocol(t)

	p.expectSuccess(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": "r"}))

	// A read statement of the transaction completes, and its result is pulled.
	p.expectExecuted(t, MsgRun, runPayload("MATCH (n) RETURN n", nil), "MATCH (n) RETURN n")

	// The transaction's mode still governs the next RUN although that RUN
	// carries no mode of its own.
	p.expectFailure(t, MsgRun, runPayload("CREATE (:TransactionModeRefused)", nil),
		"Neo.ClientError.Statement.AccessMode")
	p.reset(t)

	// RESET ended the transaction and its mode: an auto-commit RUN without a
	// mode writes again.
	p.expectExecuted(t, MsgRun, runPayload("CREATE (:AfterReset)", nil), "CREATE (:AfterReset)")
}

func TestRequestAccessModeCommitClearsTransactionMode(t *testing.T) {
	p := newRequestAccessModeProtocol(t)

	p.expectSuccess(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": "r"}))
	p.expectSuccess(t, MsgCommit, nil)

	p.expectExecuted(t, MsgRun, runPayload("CREATE (:AfterCommit)", nil), "CREATE (:AfterCommit)")
}

func TestRequestAccessModeBeginReplacesTransactionMode(t *testing.T) {
	p := newRequestAccessModeProtocol(t)

	p.expectSuccess(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": "r"}))
	p.expectSuccess(t, MsgRollback, nil)
	p.expectSuccess(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": "w"}))

	p.expectExecuted(t, MsgRun, runPayload("CREATE (:WriteTransaction)", nil), "CREATE (:WriteTransaction)")
}

func TestRequestAccessModeRejectsUnknownMode(t *testing.T) {
	p := newRequestAccessModeProtocol(t)

	p.expectFailure(t, MsgRun, runPayload("CREATE (:UnknownRunMode)", map[string]any{"mode": "read"}),
		"Neo.ClientError.Request.Invalid")
	p.reset(t)

	p.expectFailure(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": int64(1)}),
		"Neo.ClientError.Request.Invalid")
}

func TestRequestAccessModeRunCanNarrowWriteTransaction(t *testing.T) {
	p := newRequestAccessModeProtocol(t)
	p.expectSuccess(t, MsgBegin, encodePackStreamMap(map[string]any{"mode": "w"}))
	p.expectFailure(t, MsgRun, runPayload("CREATE (:PerMessageRead)", map[string]any{"mode": "r"}), "Neo.ClientError.Statement.AccessMode")
	// A refused RUN leaves the wire FAILED: neither COMMIT nor another RUN can
	// bypass it. RESET owns rollback and restores an unconstrained session.
	marker, _ := p.exchange(t, MsgCommit, nil)
	require.Equal(t, byte(MsgIgnored), marker)
	p.reset(t)
	p.expectExecuted(t, MsgRun, runPayload("CREATE (:AfterReadReset)", nil), "CREATE (:AfterReadReset)")
}
