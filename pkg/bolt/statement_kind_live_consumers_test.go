package bolt

import (
	"net"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Preserve the existing bare-listing filter and prove that its live RUN
// consumer uses the same identity when a driver adds ordinary comments.
func TestBoltBareShowDatabasesComments(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}, dbManager: manager}
	server.databaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		return staticDBAccessMode{visible: map[string]bool{"nornic": true}, access: map[string]bool{"nornic": true}}
	}
	for _, query := range []string{"SHOW DATABASES", "/* client */ SHOW DATABASES", "SHOW /* client */ DATABASES", "CYPHER 5 SHOW DATABASES"} {
		t.Run(query, func(t *testing.T) {
			serverConn, clientConn := net.Pipe()
			t.Cleanup(func() { _ = serverConn.Close(); _ = clientConn.Close() })
			session := newTestSession(serverConn, &mockExecutor{})
			session.server = server
			session.authenticated = true
			session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Roles: []string{"reader"}, Permissions: []string{"read"}}
			done := make(chan error, 1)
			go func() { done <- session.handleRun(buildRunMessageData(query, nil, nil)) }()
			_, err := AssertSuccess(t, clientConn)
			require.NoError(t, err)
			require.NoError(t, <-done)
			require.NotNil(t, session.lastResult)
			require.NotEmpty(t, session.lastResult.Rows)
			for _, row := range session.lastResult.Rows {
				require.Equal(t, "nornic", row[0])
			}
		})
	}
}
