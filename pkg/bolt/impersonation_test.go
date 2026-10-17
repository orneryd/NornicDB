package bolt

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// impersonationFixture is a Bolt server wired as cmd/nornicdb wires it (role
// entitlements, database read / write, database allowlist, IMPERSONATE
// privileges, all over one system database), a data executor over database
// "nornic" with change capture on, and a session signed in as the service
// account "svc936".
type impersonationFixture struct {
	t              *testing.T
	ctx            context.Context
	conn           *mockConn
	session        *Session
	server         *Server
	executor       *cypher.StorageExecutor
	privileges     *auth.ImpersonatePrivilegeStore
	dataPrivileges *auth.PrivilegesStore
	auth           *auth.Authenticator
	adapter        *AuthenticatorAdapter
	logs           *bytes.Buffer
}

// boltResponse is one message a session wrote: its signature, the metadata
// of a SUCCESS or FAILURE, and the fields of a RECORD.
type boltResponse struct {
	signature byte
	metadata  map[string]any
	fields    []any
}

func newImpersonationFixture(t *testing.T) *impersonationFixture {
	t.Helper()
	ctx := context.Background()
	systemBase := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = systemBase.Close() })
	system := storage.NewNamespacedEngine(systemBase, "system")
	config := auth.DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := auth.NewAuthenticator(config, system)
	require.NoError(t, err)
	roles := auth.NewRoleStore(system)
	entitlements := auth.NewRoleEntitlementsStore(system)
	databasePrivileges := auth.NewPrivilegesStore(system)
	for role, permissions := range map[string][]auth.Permission{
		"svc":    {auth.PermRead, auth.PermWrite},
		"reader": {auth.PermRead},
		"writer": {auth.PermRead, auth.PermWrite},
		"walled": {auth.PermRead, auth.PermWrite},
		"blind":  {},
		"root":   {auth.PermRead, auth.PermWrite, auth.PermAdmin},
	} {
		require.NoError(t, roles.CreateRole(ctx, role))
		ids := make([]string, len(permissions))
		for i, permission := range permissions {
			ids[i] = string(permission)
		}
		require.NoError(t, entitlements.Set(ctx, role, ids))
	}
	for user, role := range map[string]auth.Role{
		"svc936": "svc", "alice936": "reader", "writer936": "writer", "walled936": "walled",
		"blind936": "blind", "root936": "root", "bob936": "reader",
	} {
		_, err := authenticator.CreateUser(user, "password123", []auth.Role{role})
		require.NoError(t, err)
	}
	adapter := NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(func(roles []string) []string { return auth.PermissionsForRoles(roles, entitlements) })

	serverConfig := DefaultConfig()
	serverConfig.Authenticator = adapter
	serverConfig.RequireAuth = true
	serverConfig.LogQueries = true
	logs := &bytes.Buffer{}
	server := &Server{config: serverConfig, sessions: map[string]*Session{}}
	server.log = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	server.SetResolvedAccessResolver(func(roles []string, database string) auth.ResolvedAccess {
		return auth.ResolveDatabaseAccess(roles, database, databasePrivileges, entitlements)
	})
	// "walled" may use only database "other".
	allowlist := map[string][]string{"walled": {"other"}}
	server.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		return auth.NewAllowlistDatabaseAccessMode(allowlist, roles)
	})
	privileges := auth.NewImpersonatePrivilegeStore(system)
	server.SetImpersonationResolver(func(roles []string, admin bool, user string) bool {
		allowed, err := privileges.AllowsImpersonation(roles, admin, user)
		require.NoError(t, err)
		return allowed
	})

	dataBase, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataBase.Close() })
	data := storage.NewNamespacedEngine(dataBase, "nornic")
	capture, ok := storage.ChangeCaptureOf(data)
	require.True(t, ok)
	capture.SetChangeCapture("nornic", storage.ChangeCaptureFull)
	executor := cypher.NewStorageExecutor(data)

	conn := &mockConn{}
	session := newTestSession(conn, &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: executor},
	})
	session.server = server
	session.version = BoltV4_4
	session.authenticated = true
	session.authResult, err = adapter.Authenticate("basic", "svc936", "password123")
	require.NoError(t, err)
	return &impersonationFixture{
		t: t, ctx: ctx, conn: conn, session: session, server: server, executor: executor,
		privileges: privileges, auth: authenticator, adapter: adapter, logs: logs,
		dataPrivileges: databasePrivileges,
	}
}

// responses decodes and clears what the session wrote.
func (f *impersonationFixture) responses() []boltResponse {
	f.t.Helper()
	require.NoError(f.t, f.session.writer.Flush())
	data := f.conn.writeData
	f.conn.writeData = nil
	var out []boltResponse
	var message []byte
	for i := 0; i+1 < len(data); {
		size := int(data[i])<<8 | int(data[i+1])
		i += 2
		if size > 0 {
			message = append(message, data[i:i+size]...)
			i += size
			continue
		}
		if len(message) < 2 {
			message = nil
			continue
		}
		response := boltResponse{signature: message[1]}
		switch response.signature {
		case MsgSuccess, MsgFailure:
			if len(message) > 2 {
				response.metadata, _, _ = decodePackStreamMap(message, 2)
			}
		case MsgRecord:
			response.fields, _, _ = decodePackStreamList(message, 2)
		}
		out = append(out, response)
		message = nil
	}
	return out
}

// firstFailure returns the code and message of the first FAILURE among
// responses, or "" when there is none.
func firstFailure(responses []boltResponse) (code, message string) {
	for _, response := range responses {
		if response.signature == MsgFailure {
			code, _ = response.metadata["code"].(string)
			message, _ = response.metadata["message"].(string)
			return code, message
		}
	}
	return "", ""
}

// run sends an auto-commit (or, inside BEGIN, a transaction's) RUN with
// extra, pulls its rows, and returns them, or the FAILURE's code after which
// the session is RESET.
func (f *impersonationFixture) run(query string, extra map[string]any) (rows [][]any, code string) {
	f.t.Helper()
	require.NoError(f.t, f.session.handleRun(buildRunMessageData(query, nil, extra)))
	if code, _ = firstFailure(f.responses()); code != "" {
		f.reset()
		return nil, code
	}
	require.NoError(f.t, f.session.handlePull(nil))
	responses := f.responses()
	if code, _ = firstFailure(responses); code != "" {
		f.reset()
		return nil, code
	}
	for _, response := range responses {
		if response.signature == MsgRecord {
			rows = append(rows, response.fields)
		}
	}
	return rows, ""
}

func (f *impersonationFixture) reset() {
	f.t.Helper()
	require.NoError(f.t, f.session.handleReset(nil))
	f.responses()
}

// begin sends BEGIN with extra and returns the FAILURE's code, or "".
func (f *impersonationFixture) begin(extra map[string]any) string {
	f.t.Helper()
	require.NoError(f.t, f.session.handleBegin(encodePackStreamMap(extra)))
	code, _ := firstFailure(f.responses())
	if code != "" {
		f.reset()
	}
	return code
}

func (f *impersonationFixture) grant(role string, target auth.ImpersonateTarget, denied bool) {
	f.t.Helper()
	require.NoError(f.t, f.privileges.Set(f.ctx, role, target, denied, false))
}

func (f *impersonationFixture) nodeCount() int64 {
	f.t.Helper()
	result, err := f.executor.Execute(f.ctx, "MATCH (n:Imp936) RETURN count(n)", nil)
	require.NoError(f.t, err)
	return result.Rows[0][0].(int64)
}

// labelCount is the number of nodes labelled label in database "nornic".
func (f *impersonationFixture) labelCount(label string) int64 {
	f.t.Helper()
	result, err := f.executor.Execute(f.ctx, "MATCH (n:`"+label+"`) RETURN count(n)", nil)
	require.NoError(f.t, err)
	return result.Rows[0][0].(int64)
}

func imp(user any) map[string]any { return map[string]any{"imp_user": user} }

// An authorized impersonation runs with the impersonated user's privileges
// only: a write the service account may make is refused for a read-only
// target, a database the target may not use is refused although the
// service account may, and a target without read reads nothing.
func TestBoltImpersonationRunsWithTargetPrivilegesOnly(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)

	_, code := f.run("CREATE (:Imp936)", nil)
	require.Empty(t, code, "the service account itself may write")
	require.EqualValues(t, 1, f.nodeCount())

	_, code = f.run("CREATE (:Imp936)", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "a read-only target's write must be refused")
	require.EqualValues(t, 1, f.nodeCount())
	rows, code := f.run("MATCH (n:Imp936) RETURN count(n)", imp("alice936"))
	require.Empty(t, code)
	require.Equal(t, [][]any{{int64(1)}}, rows)

	_, code = f.run("MATCH (n:Imp936) RETURN count(n)", imp("walled936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "a database the target may not use must be refused")
	_, code = f.run("MATCH (n:Imp936) RETURN count(n)", imp("blind936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "a target without read must read nothing")

	_, code = f.run("CREATE (:Imp936)", imp("writer936"))
	require.Empty(t, code)
	require.EqualValues(t, 2, f.nodeCount())

	// The impersonation ends with its RUN: the next RUN is the service
	// account's again.
	_, code = f.run("CREATE (:Imp936)", nil)
	require.Empty(t, code)
	require.EqualValues(t, 3, f.nodeCount())
}

// Without the privilege, or with it denied, impersonation fails with
// Neo.ClientError.Security.Forbidden before the statement runs; a user who
// doesn't exist is refused the same way, so a request can't tell.
func TestBoltImpersonationRefusedBeforeExecution(t *testing.T) {
	f := newImpersonationFixture(t)
	_, code := f.run("CREATE (:Imp936)", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "no IMPERSONATE privilege")
	require.Zero(t, f.nodeCount())

	f.grant("svc", auth.ImpersonateUser("writer936"), false)
	_, code = f.run("CREATE (:Imp936)", imp("bob936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "granted another user only")
	_, code = f.run("CREATE (:Imp936)", imp("writer936"))
	require.Empty(t, code)
	require.EqualValues(t, 1, f.nodeCount())

	// DENY wins over GRANT, a specific one over GRANT (*) too.
	f.grant("svc", auth.ImpersonateUser("writer936"), true)
	_, code = f.run("CREATE (:Imp936)", imp("writer936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	_, code = f.run("CREATE (:Imp936)", imp("writer936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.EqualValues(t, 1, f.nodeCount())

	// IMPERSONATE (*) doesn't reveal which users exist.
	require.NoError(t, f.session.handleRun(buildRunMessageData("RETURN 1", nil, imp("nobody936"))))
	missingCode, missingMessage := firstFailure(f.responses())
	f.reset()
	require.NoError(t, f.session.handleRun(buildRunMessageData("RETURN 1", nil, imp("writer936"))))
	deniedCode, deniedMessage := firstFailure(f.responses())
	f.reset()
	require.Equal(t, "Neo.ClientError.Security.Forbidden", missingCode)
	require.Equal(t, deniedCode, missingCode)
	require.Equal(t, strings.ReplaceAll(deniedMessage, "writer936", "nobody936"), missingMessage)

	// A disabled account can't be impersonated.
	require.NoError(t, f.auth.DisableUser("alice936"))
	_, code = f.run("RETURN 1", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
}

// An administrator (the admin entitlement, Neo4j's ALL DBMS PRIVILEGES) may
// impersonate anyone not denied to one of its roles.
func TestBoltImpersonationByAdministrator(t *testing.T) {
	f := newImpersonationFixture(t)
	var err error
	f.session.authResult, err = f.adapter.Authenticate("basic", "root936", "password123")
	require.NoError(t, err)
	_, code := f.run("CREATE (:Imp936)", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "the administrator's own write privilege must not apply")
	rows, code := f.run("MATCH (n:Imp936) RETURN count(n)", imp("alice936"))
	require.Empty(t, code)
	require.Equal(t, [][]any{{int64(0)}}, rows)
	f.grant("root", auth.ImpersonateUser("alice936"), true)
	_, code = f.run("RETURN 1", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
}

// A null imp_user, a missing one, and the signed-in user's own name are no
// impersonation; Bolt before 4.4, which doesn't define imp_user, ignores it.
// A non-string imp_user is an invalid request.
func TestBoltImpersonationNullAndProtocolVersions(t *testing.T) {
	f := newImpersonationFixture(t)
	_, code := f.run("CREATE (:Imp936)", imp(nil))
	require.Empty(t, code)
	_, code = f.run("CREATE (:Imp936)", imp("svc936"))
	require.Empty(t, code)
	require.EqualValues(t, 2, f.nodeCount())
	_, code = f.run("RETURN 1", imp(int64(5)))
	require.Equal(t, "Neo.ClientError.Request.Invalid", code)

	f.session.version = BoltV4_3
	_, code = f.run("CREATE (:Imp936)", imp("alice936"))
	require.Empty(t, code, "Bolt 4.3 has no imp_user")
	require.EqualValues(t, 3, f.nodeCount())
}

// With authentication disabled, impersonation is refused as Neo4j refuses
// it: Neo.ClientError.Statement.ArgumentError.
func TestBoltImpersonationWithAuthenticationDisabled(t *testing.T) {
	f := newImpersonationFixture(t)
	f.server.config.Authenticator = nil
	f.server.config.RequireAuth = false
	_, code := f.run("RETURN 1", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", code)
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", f.begin(imp("alice936")))
}

// BEGIN's imp_user holds for the whole explicit transaction, whose RUNs
// ignore their own; the transaction is listed under the impersonated user
// in SHOW TRANSACTIONS; and the impersonation ends with the transaction.
func TestBoltImpersonationExplicitTransaction(t *testing.T) {
	f := newImpersonationFixture(t)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", f.begin(imp("alice936")), "unauthorized BEGIN")
	require.False(t, f.session.inTransaction)
	require.Nil(t, f.session.impersonated)

	f.grant("svc", auth.ImpersonateAllUsers, false)
	require.Empty(t, f.begin(imp("alice936")))
	require.True(t, f.session.inTransaction)
	listed, err := f.executor.Execute(f.ctx, "SHOW TRANSACTIONS YIELD username RETURN username", nil)
	require.NoError(t, err)
	require.Contains(t, listed.Rows, []interface{}{"alice936"}, "SHOW TRANSACTIONS lists the executing user")
	require.NotContains(t, listed.Rows, []interface{}{"svc936"})
	listed, err = f.executor.Execute(f.ctx, "SHOW TRANSACTIONS YIELD username, authenticatedUser, executingUser RETURN username, authenticatedUser, executingUser", nil)
	require.NoError(t, err)
	require.Contains(t, listed.Rows, []interface{}{"alice936", "svc936", "alice936"}, "transaction attribution retains both users")

	_, code := f.run("CREATE (:Imp936)", nil)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.False(t, f.session.inTransaction, "RESET ended the transaction")
	require.Nil(t, f.session.impersonated)

	require.Empty(t, f.begin(imp("alice936")))
	_, code = f.run("CREATE (:Imp936)", imp("writer936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "a RUN in a transaction can't change its user")

	require.Empty(t, f.begin(imp("writer936")))
	_, code = f.run("CREATE (:Imp936)", nil)
	require.Empty(t, code)
	require.NoError(t, f.session.handleCommit(nil))
	commitCode, _ := firstFailure(f.responses())
	require.Empty(t, commitCode)
	require.Nil(t, f.session.impersonated)
	require.EqualValues(t, 1, f.nodeCount())

	require.Empty(t, f.begin(nil))
	_, code = f.run("CREATE (:Imp936)", nil)
	require.Empty(t, code, "the next transaction is the service account's")
	require.NoError(t, f.session.handleRollback(nil))
	f.responses()
}

func TestBoltWriteOnlyPrincipalTerminatesOwnTransaction(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	require.NoError(t, f.dataPrivileges.SavePrivilege(f.ctx, "writer", "nornic", false, true))
	for _, impersonated := range []bool{false, true} {
		// Both public sessions use the same current role/data resolvers. Only
		// their data executors differ, as for separate Bolt connections.
		terminator := *f
		terminator.conn = &mockConn{}
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		terminator.session = newTestSession(terminator.conn, &boltQueryExecutorAdapter{
			executor: cypher.NewStatementExecutor(storage.NewNamespacedEngine(engine, "nornic")),
		})
		terminator.session.server = f.server
		terminator.session.version = BoltV4_4
		terminator.session.authenticated = true
		var err error
		var extra map[string]any
		if impersonated {
			f.session.authResult, err = f.adapter.Authenticate("basic", "svc936", "password123")
			extra = imp("writer936")
		} else {
			f.session.authResult, err = f.adapter.Authenticate("basic", "writer936", "password123")
		}
		require.NoError(t, err)
		terminator.session.authResult = f.session.authResult
		require.Empty(t, f.begin(extra))
		_, code := f.run("CREATE (:OwnTermination)", nil)
		require.Empty(t, code, "the principal can write the graph")
		listing, err := f.executor.Execute(f.ctx, "SHOW TRANSACTIONS YIELD transactionId, username WHERE username = 'writer936' RETURN transactionId", nil)
		require.NoError(t, err)
		require.Len(t, listing.Rows, 1)
		id := listing.Rows[0][0].(string)
		rows, code := terminator.run("TERMINATE TRANSACTION '"+id+"'", extra)
		require.Empty(t, code, "own termination requires no graph read")
		require.Equal(t, [][]any{{id, "writer936", "Transaction terminated."}}, rows)
		f.reset()
	}
}

// A ROUTE naming a user the session may not impersonate is refused like a
// BEGIN; an allowed or null imp_user gets the routing table, which names
// the database routed for the impersonated user.
func TestBoltImpersonationRoute(t *testing.T) {
	f := newImpersonationFixture(t)
	route := func(extra map[string]any) (string, map[string]any) {
		data := append(encodePackStreamMap(map[string]any{}), encodePackStreamList(nil)...)
		if extra != nil {
			data = append(data, encodePackStreamMap(extra)...)
		}
		require.NoError(t, f.session.handleRoute(data))
		responses := f.responses()
		code, _ := firstFailure(responses)
		if code != "" {
			f.reset()
			return code, nil
		}
		require.Len(t, responses, 1)
		require.Contains(t, responses[0].metadata, "rt")
		rt, _ := responses[0].metadata["rt"].(map[string]any)
		return "", rt
	}
	code, _ := route(imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	code, rt := route(imp(nil))
	require.Empty(t, code)
	require.NotContains(t, rt, "db")
	code, _ = route(nil)
	require.Empty(t, code)
	f.grant("svc", auth.ImpersonateUser("alice936"), false)
	code, rt = route(imp("alice936"))
	require.Empty(t, code)
	require.Equal(t, "nornic", rt["db"])
	require.Nil(t, f.session.impersonated, "a ROUTE leaves the session's principal as it was")
}

// A ROUTE decides its impersonation on the session's current roles, as a
// RUN does: once the login loses the role granted IMPERSONATE, ROUTE is
// refused even though that role still holds the privilege.
func TestBoltImpersonationRouteUsesCurrentLoginRoles(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateUser("alice936"), false)
	routeCode := func() string {
		data := append(encodePackStreamMap(map[string]any{}), encodePackStreamList(nil)...)
		data = append(data, encodePackStreamMap(imp("alice936"))...)
		require.NoError(t, f.session.handleRoute(data))
		code, _ := firstFailure(f.responses())
		if code != "" {
			f.reset()
		}
		return code
	}
	require.Empty(t, routeCode())
	require.NoError(t, f.auth.UpdateRoles("svc936", []auth.Role{"reader"}))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", routeCode())
	_, code := f.run("RETURN 1", imp("alice936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "RUN agrees with ROUTE")
}

// A ROUTE impersonating a user is for a database that user may use: the
// requested one, or the default one, refused as a RUN as that user would be,
// although the login may use it.
func TestBoltImpersonationRouteChecksTargetDatabase(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateUser("walled936"), false)
	routeCode := func(extra map[string]any) string {
		data := append(encodePackStreamMap(map[string]any{}), encodePackStreamList(nil)...)
		data = append(data, encodePackStreamMap(extra)...)
		require.NoError(t, f.session.handleRoute(data))
		code, _ := firstFailure(f.responses())
		if code != "" {
			f.reset()
		}
		return code
	}
	require.Equal(t, "Neo.ClientError.Security.Forbidden", routeCode(map[string]any{"db": "nornic", "imp_user": "walled936"}))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", routeCode(imp("walled936")), "the default database")
	_, code := f.run("RETURN 1", imp("walled936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "RUN agrees with ROUTE")
	require.Empty(t, routeCode(map[string]any{"db": "other", "imp_user": "walled936"}))
	require.Empty(t, routeCode(map[string]any{"db": "nornic"}), "the login itself may use nornic")
}

// While impersonating, updating administration commands are refused (Neo4j:
// "Not allowed to run updating system commands when impersonating a user"),
// even for an impersonated administrator; SHOW commands are allowed.
func TestBoltImpersonationRefusesSystemUpdates(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateUser("root936"), false)
	_, code := f.run("GRANT IMPERSONATE (bob936) ON DBMS TO svc", imp("root936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	allowed, err := f.privileges.AllowsImpersonation([]string{"svc"}, false, "bob936")
	require.NoError(t, err)
	require.False(t, allowed)
	// A procedure that changes the DBMS (installing a server-wide custom
	// procedure) is refused the same way, directly or from dynamic Cypher.
	t.Cleanup(cypher.ClearUserProcedures)
	install := "CALL apoc.custom.installProcedure('imp936_bolt() :: (v :: INTEGER)', 'RETURN 1 AS v', 'neo4j', 'read')"
	_, code = f.run(install, imp("root936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	_, code = f.run("CALL apoc.cypher.doIt(\""+install+"\", {}) YIELD value RETURN value", imp("root936"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	_, code = f.run("CALL custom.imp936_bolt() YIELD v RETURN v", nil)
	require.NotEmpty(t, code, "nothing was installed")
	_, code = f.run("SHOW PRIVILEGES", imp("root936"))
	require.Empty(t, code)
}

// Change events and the query log keep both users: the executing
// (impersonated) one and the authenticated one.
func TestBoltImpersonationAttribution(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateUser("writer936"), false)
	_, code := f.run("CREATE (:Imp936 {v: 1})", imp("writer936"))
	require.Empty(t, code)
	_, code = f.run("CREATE (:Imp936 {v: 2})", nil)
	require.Empty(t, code)
	require.Empty(t, f.begin(imp("writer936")))
	_, code = f.run("CREATE (:Imp936 {v: 3})", nil)
	require.Empty(t, code)
	require.NoError(t, f.session.handleCommit(nil))
	f.responses()

	result, err := f.executor.Execute(f.ctx, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD metadata, event "+
		"RETURN event.state.after.properties.v AS v, metadata.executingUser AS executing, metadata.authenticatedUser AS authenticated ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{int64(1), "writer936", "svc936"},
		{int64(2), "svc936", "svc936"},
		{int64(3), "writer936", "svc936"},
	}, result.Rows)
	selected, err := f.executor.Execute(f.ctx, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [{select: 'n', executingUser: 'writer936'}]) YIELD event RETURN event.state.after.properties.v AS v ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, selected.Rows)

	logged := f.logs.String()
	require.Contains(t, logged, `"authenticatedUser":"svc936"`)
	require.Contains(t, logged, `"executingUser":"writer936"`)
	require.Contains(t, logged, `"user":"svc936"`)
}

// An impersonating RUN, BEGIN or ROUTE on a remote constituent, or on a
// composite with one, is refused before an executor is opened or anything
// is sent to the remote server, which would run it as the login's forwarded
// credential (or the constituent's stored one), never as the impersonated
// user; and the login's credential is not forwarded while impersonating.
func TestBoltImpersonationRefusesRemoteGraphs(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"columns":["n"],"data":[]}],"errors":[]}`))
	}))
	t.Cleanup(remote.Close)
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("tenant_local"))
	require.NoError(t, manager.CreateCompositeDatabase("mixed", []multidb.ConstituentRef{
		{Alias: "loc", DatabaseName: "tenant_local", Type: "local", AccessMode: "read_write"},
		{Alias: "rem", DatabaseName: "tenant_remote", Type: "remote", AccessMode: "read_write", URI: remote.URL, AuthMode: "oidc_forwarding"},
	}))
	f.server.dbManager = manager
	f.session.forwardedAuthHeader = "Bearer login-token"

	for _, db := range []string{"mixed.rem", "mixed"} {
		extra := map[string]any{"db": db, "imp_user": "alice936"}
		_, code := f.run("MATCH (n) RETURN n", extra)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "RUN on "+db)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", f.begin(extra), "BEGIN on "+db)
		require.False(t, f.session.inTransaction)
		data := append(encodePackStreamMap(map[string]any{}), encodePackStreamList(nil)...)
		require.NoError(t, f.session.handleRoute(append(data, encodePackStreamMap(extra)...)))
		code, message := firstFailure(f.responses())
		f.reset()
		require.Equal(t, "Neo.ClientError.Security.Forbidden", code, "ROUTE on "+db)
		require.Contains(t, message, db)
	}
	require.Zero(t, requests.Load(), "nothing reached the remote server")
	_, code := f.run("MATCH (n) RETURN count(n)", map[string]any{"db": "mixed.loc", "imp_user": "alice936"})
	require.Empty(t, code, "a local constituent runs")

	require.Equal(t, "Bearer login-token", f.session.forwardedAuth())
	f.session.impersonated = &BoltAuthResult{Username: "alice936"}
	require.Empty(t, f.session.forwardedAuth(), "no login credential while impersonating")
	f.session.impersonated = nil
}

// An impersonated statement stopped at its deadline gets Neo4j's timeout for
// the deadline's owner (#1392), as the signed-in user's would; a refusal
// stays a refusal, decided before the statement runs, however short the
// client's tx_timeout: impersonation without the privilege, a database the
// target may not use, a write the target may not make.
func TestBoltImpersonationDeadlineStatus(t *testing.T) {
	f := newImpersonationFixture(t)
	f.grant("svc", auth.ImpersonateAllUsers, false)
	f.grant("svc", auth.ImpersonateUser("bob936"), true)
	slow := "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(x) AS c"
	with := func(user string, txTimeout int64) map[string]any {
		return map[string]any{"imp_user": user, "tx_timeout": txTimeout}
	}

	_, code := f.run(slow, with("alice936", 100))
	require.Equal(t, nornicerrors.TransactionTimedOutClientConfiguration, code, "the impersonated statement times out by the client's tx_timeout")

	for name, refused := range map[string]struct {
		query string
		user  string
	}{
		"impersonation denied":    {slow, "bob936"},
		"database not allowed":    {slow, "walled936"},
		"write without the right": {"CREATE (:Imp936Deadline) RETURN 1 AS one", "alice936"},
	} {
		_, code := f.run(refused.query, with(refused.user, 1))
		require.Equal(t, "Neo.ClientError.Security.Forbidden", code, name)
	}
	require.Zero(t, f.labelCount("Imp936Deadline"))

	// Signed in as the service account, the same deadline gives the same code.
	_, code = f.run(slow, map[string]any{"tx_timeout": int64(100)})
	require.Equal(t, nornicerrors.TransactionTimedOutClientConfiguration, code)
}
