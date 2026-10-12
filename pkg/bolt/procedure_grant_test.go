package bolt

import (
	"context"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
)

// A Bolt session whose role holds EXECUTE BOOSTED PROCEDURE db.cdc.* runs
// db.cdc.query without administrator rights, and the executor's own check
// accepts it too; another procedure that needs administrator rights is still
// refused.
func TestBoltBoostedProcedureGrant(t *testing.T) {
	run := func(t *testing.T, query string) (executed bool, authorizeErr error, code string) {
		t.Helper()
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		session := newTestSession(serverConn, &mockExecutor{
			executeFunc: func(ctx context.Context, query string, _ map[string]any) (*QueryResult, error) {
				executed = true
				authorizeErr = cypher.AuthorizeQuery(ctx, query)
				return &QueryResult{}, nil
			},
		})
		session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
		session.server.SetProcedureGrantResolver(func(roles []string) func(string) bool {
			return func(procedure string) bool {
				return len(roles) == 1 && roles[0] == "cdc_reader" && auth.ProcedureGlobMatches("db.cdc.*", procedure)
			}
		})
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Username: "worker", Roles: []string{"cdc_reader"}, Permissions: []string{string(auth.PermRead)}}

		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData(query, nil, nil)) }()
		if failureCode, _, err := AssertFailure(t, clientConn); err == nil {
			code = failureCode
		}
		if err := <-done; err != nil {
			t.Fatalf("handle RUN: %v", err)
		}
		return executed, authorizeErr, code
	}

	executed, authorizeErr, code := run(t, "CALL db.cdc.query()")
	if !executed || authorizeErr != nil || code != "" {
		t.Fatalf("db.cdc.query with a boosted grant: executed=%v authorize=%v code=%q", executed, authorizeErr, code)
	}
	executed, _, code = run(t, "CALL db.txlog.entries()")
	if executed || code != "Neo.ClientError.Security.Forbidden" {
		t.Fatalf("db.txlog.entries without a grant: executed=%v code=%q", executed, code)
	}
}
