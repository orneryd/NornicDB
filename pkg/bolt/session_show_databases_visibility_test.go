package bolt

import (
	"net"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltShowDatabasesHidesDatabasesInEveryForm pins, through RUN and the real
// executor, that no comment, spacing, final ";", tail or name shows a database
// the principal may not access.
func TestBoltShowDatabasesHidesDatabasesInEveryForm(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}, dbManager: manager}
	server.databaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		return staticDBAccessMode{
			visible: map[string]bool{"nornic": true},
			access:  map[string]bool{"nornic": true},
		}
	}

	names := func(t *testing.T, statement string) []any {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		session := newTestSession(serverConn, &mockExecutor{})
		session.server = server
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Roles: []string{"reader"}, Permissions: []string{"read"}}
		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData(statement, nil, nil)) }()
		_, err := AssertSuccess(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
		require.NotNil(t, session.lastResult)
		var rows []any
		for _, row := range session.lastResult.Rows {
			rows = append(rows, row[0])
		}
		return rows
	}

	for _, statement := range []string{
		"SHOW DATABASES",
		"show databases",
		"SHOW DATABASES;",
		"SHOW DATABASES /* review */",
		"SHOW DATABASES // review",
		"SHOW DATABASES // review\n",
		"SHOW DATABASES /* a */ ; /* b */",
		"/* review */ SHOW DATABASES",
		"// review\nSHOW DATABASES",
		"SHOW /* review */ DATABASES",
		"  SHOW\t\nDATABASES  ",
		"SHOW\u00a0DATABASES\u2003",
		"\ufeffSHOW DATABASES",
		"SHOW DATABASES YIELD name",
		"SHOW DATABASES YIELD name RETURN name",
		"SHOW DATABASES YIELD *",
		"SHOW DATABASES /* review */ YIELD name",
		"SHOW DATABASE",
		"SHOW DATABASE /* review */",
		"SHOW DATABASE YIELD name",
	} {
		t.Run(statement, func(t *testing.T) {
			// Neo4j: the system database "is always visible".
			require.Equal(t, []any{"nornic", "system"}, names(t, statement))
		})
	}
	for _, statement := range []string{
		"SHOW DATABASE nornic",
		"SHOW DATABASE nornic YIELD name",
		"SHOW DATABASES nornic",
	} {
		t.Run(statement, func(t *testing.T) {
			require.Equal(t, []any{"nornic"}, names(t, statement))
		})
	}
	t.Run("SHOW DATABASE system", func(t *testing.T) {
		require.Equal(t, []any{"system"}, names(t, "SHOW DATABASE system"))
	})
	for _, statement := range []string{
		"SHOW DATABASES WHERE name = 'private'",
		"SHOW DATABASES YIELD name WHERE name = 'private'",
		"SHOW DATABASE private",
		"SHOW DATABASE private YIELD name",
		"SHOW DATABASE private WHERE name = 'private'",
		"SHOW DATABASE /* review */ private",
		"SHOW DATABASE absent",
		"SHOW DATABASES private",
		"SHOW DATABASES absent",
	} {
		t.Run(statement, func(t *testing.T) {
			require.Empty(t, names(t, statement))
		})
	}

	failure := func(t *testing.T, statement string, metadata map[string]any) (string, string) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		session := newTestSession(serverConn, &mockExecutor{})
		session.server = server
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Roles: []string{"reader"}, Permissions: []string{"read"}}
		done := make(chan error, 1)
		go func() { done <- session.handleRun(buildRunMessageData(statement, nil, metadata)) }()
		code, message, err := AssertFailure(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
		return code, message
	}
	normalizeName := func(message string) string {
		return strings.ReplaceAll(strings.ReplaceAll(message, "private", "NAME"), "absent", "NAME")
	}
	limitsAbsentCode, limitsAbsentMessage := failure(t, "SHOW LIMITS FOR DATABASE absent", nil)
	limitsHiddenCode, limitsHiddenMessage := failure(t, "SHOW LIMITS FOR DATABASE private", nil)
	require.Equal(t, limitsAbsentCode, limitsHiddenCode)
	require.Equal(t, normalizeName(limitsAbsentMessage), normalizeName(limitsHiddenMessage))
	absentCode, absentMessage := failure(t, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE absent", nil)
	hiddenCode, hiddenMessage := failure(t, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE private", nil)
	require.Equal(t, absentCode, hiddenCode)
	require.Equal(t, absentMessage, hiddenMessage)

	// A hidden database answers as an absent one when RUN selects it.
	normalize := func(message string) string {
		return strings.ReplaceAll(strings.ReplaceAll(message, "private", "NAME"), "absent", "NAME")
	}
	for _, statement := range []string{
		"RETURN 1", "CREATE (n)", "CREATE INDEX i FOR (n:L) ON (n.p)", "SHOW DATABASES", "CREATE DATABASE d", "", "// only a comment",
		// Syntactically invalid, before any permission is read.
		"RETRUN 1", "MATCH (n RETURN n", "RETURN 1 /* unclosed", "EXPLAIN PROFILE RETURN 1", "CYPHER 99 RETURN 1", "RETURN 1; RETURN 2",
		// Fail while running, or for a value only execution reads.
		"RETURN 1/0", "UNWIND [1] AS x RETURN x/0", "RETURN $missing", "CALL no.such.proc()", "RETURN no.such(1)", "CREATE (:A {p: 1/0})", "MATCH (n) RETURN n.p/0",
	} {
		t.Run("RUN db "+statement, func(t *testing.T) {
			absentCode, absentMessage := failure(t, statement, map[string]any{"db": "absent"})
			hiddenCode, hiddenMessage := failure(t, statement, map[string]any{"db": "private"})
			require.Equal(t, absentCode, hiddenCode)
			require.Equal(t, normalize(absentMessage), normalize(hiddenMessage))
		})
		t.Run("USE "+statement, func(t *testing.T) {
			absentCode, absentMessage := failure(t, "USE absent "+statement, nil)
			hiddenCode, hiddenMessage := failure(t, "USE private "+statement, nil)
			require.Equal(t, absentCode, hiddenCode)
			require.Equal(t, normalize(absentMessage), normalize(hiddenMessage))
		})
	}

	// BEGIN and HELLO name a database before any statement.
	selection := func(t *testing.T, run func(*Session) error) (string, string) {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		session := newTestSession(serverConn, &mockExecutor{})
		session.server = server
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Roles: []string{"reader"}, Permissions: []string{"read"}}
		done := make(chan error, 1)
		go func() { done <- run(session) }()
		code, message, err := AssertFailure(t, clientConn)
		require.NoError(t, err)
		require.NoError(t, <-done)
		return code, normalize(message)
	}
	begin := func(name string) func(*Session) error {
		return func(session *Session) error {
			return session.dispatchMessage(MsgBegin, encodePackStreamMap(map[string]any{"db": name}))
		}
	}
	absentCode, absentMessage = selection(t, begin("absent"))
	hiddenCode, hiddenMessage = selection(t, begin("private"))
	require.Equal(t, absentCode, hiddenCode)
	require.Equal(t, absentMessage, hiddenMessage)
	hello := func(name string) func(*Session) error {
		return func(session *Session) error {
			return session.handleHello(encodePackStreamMap(map[string]any{"scheme": "none", "database": name}))
		}
	}
	absentCode, absentMessage = selection(t, hello("absent"))
	hiddenCode, hiddenMessage = selection(t, hello("private"))
	require.Equal(t, absentCode, hiddenCode)
	require.Equal(t, absentMessage, hiddenMessage)
}
