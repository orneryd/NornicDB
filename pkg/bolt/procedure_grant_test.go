package bolt

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
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
				authorizeErr = cypher.AuthorizeQuery(ctx, query)
				if authorizeErr != nil {
					return nil, authorizeErr
				}
				executed = true
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

// A nil server, or one without a resolver, has no grant check.
func TestBoltProcedureGrantsWithoutResolver(t *testing.T) {
	var nilServer *Server
	if nilServer.procedureGrants([]string{"r"}) != nil {
		t.Fatal("a nil server has no grant check")
	}
	if (&Server{}).procedureGrants([]string{"r"}) != nil {
		t.Fatal("a server without a resolver has no grant check")
	}
}

// runOnBoltSession runs query through a Bolt session of server as user
// (roles, permissions), with execute standing in for the executor; it
// returns whether execute ran and the FAILURE code the session sent, if
// any.
func runOnBoltSession(t *testing.T, server *Server, result *BoltAuthResult, query string, execute func(ctx context.Context, query string, params map[string]any) error) (executed bool, code string) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	session := newTestSession(serverConn, &mockExecutor{
		executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			if err := cypher.AuthorizeQuery(ctx, query); err != nil {
				return nil, err
			}
			executed = true
			return &QueryResult{}, execute(ctx, query, params)
		},
	})
	session.server = server
	session.authenticated = true
	session.authResult = result
	done := make(chan error, 1)
	go func() { done <- session.handleRun(buildRunMessageData(query, nil, nil)) }()
	if failureCode, _, err := AssertFailure(t, clientConn); err == nil {
		code = failureCode
	}
	if err := <-done; err != nil {
		t.Fatalf("handle RUN: %v", err)
	}
	return executed, code
}

// A Bolt session's grants come from the grant store on every statement: a
// granted procedure is refused when the grants can't be read, and once
// the grant is revoked.
func TestBoltBoostedProcedureGrantFromStore(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	unreadable := false
	store := auth.NewProcedurePrivilegeStore(&grantLookupEngine{Engine: system, fail: &unreadable})
	require.NoError(t, auth.NewProcedurePrivilegeStore(system).Grant(ctx, "cdc_reader", "db.cdc.*", true, false))
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	server.SetProcedureGrantResolver(func(roles []string) func(string) bool {
		return func(procedure string) bool {
			allowed, _ := store.AllowsBoosted(roles, procedure)
			return allowed
		}
	})
	worker := &BoltAuthResult{Authenticated: true, Username: "worker", Roles: []string{"cdc_reader"}, Permissions: []string{string(auth.PermRead)}}
	authorize := func(ctx context.Context, query string, _ map[string]any) error {
		return cypher.AuthorizeQuery(ctx, query)
	}

	executed, code := runOnBoltSession(t, server, worker, "CALL db.cdc.query()", authorize)
	require.True(t, executed)
	require.Empty(t, code)
	unreadable = true
	executed, code = runOnBoltSession(t, server, worker, "CALL db.cdc.query()", authorize)
	require.False(t, executed)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	unreadable = false
	_, err := auth.NewProcedurePrivilegeStore(system).Revoke(ctx, "cdc_reader", "db.cdc.*", true, false)
	require.NoError(t, err)
	executed, code = runOnBoltSession(t, server, worker, "CALL db.cdc.query()", authorize)
	require.False(t, executed)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
}

// grantLookupEngine fails the label lookup procedure grants are read with
// while *fail is set.
type grantLookupEngine struct {
	storage.Engine
	fail *bool
}

func (e *grantLookupEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if *e.fail {
		return nil, errors.New("grants unreadable")
	}
	return e.Engine.GetNodesByLabel(label)
}

// GRANT IMMUTABLE runs on a Bolt server without authentication, and is
// refused (Neo.ClientError.Security.Forbidden, as Neo4j 5.26.30
// Enterprise) on one with authentication.
func TestBoltImmutableGrantNeedsAuthenticationDisabled(t *testing.T) {
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	exec := cypher.NewStorageExecutor(system)
	admin := &BoltAuthResult{Authenticated: true, Username: "admin", Roles: []string{"admin"},
		Permissions: []string{string(auth.PermRead), string(auth.PermWrite), string(auth.PermSchema), string(auth.PermAdmin)}}
	var execErr error
	execute := func(ctx context.Context, query string, params map[string]any) error {
		_, execErr = exec.Execute(ctx, query, params)
		return nil
	}
	const grant = "GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer"

	open := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	executed, _ := runOnBoltSession(t, open, admin, grant, execute)
	require.True(t, executed)
	require.NoError(t, execErr)

	authStorage := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = authStorage.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")}, authStorage)
	require.NoError(t, err)
	// A stored administrator: a session's roles are its account's current
	// ones.
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	adapter := NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(func(roles []string) []string { return auth.PermissionsForRoles(roles, nil) })
	secured := &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	secured.config.Authenticator = adapter
	storedAdmin, err := adapter.Authenticate("basic", "admin", "password123")
	require.NoError(t, err)
	executed, _ = runOnBoltSession(t, secured, storedAdmin, grant, execute)
	require.True(t, executed)
	require.ErrorContains(t, execErr, "Permission cannot be granted for ASSIGN IMMUTABLE PRIVILEGE.")
}
