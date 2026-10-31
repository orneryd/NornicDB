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

// TestBoltHiddenTargetAnswersAsAbsentWhereverItIsSelected compares RUN's answer
// for a graph the principal may not access and one that does not exist, selected
// where execution may not reach it or may fail first.
func TestBoltHiddenTargetAnswersAsAbsentWhereverItIsSelected(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "vis", []multidb.ConstituentRef{
		{Alias: "pub", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
		{Alias: "sec", DatabaseName: "private", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "hiddencomp", []multidb.ConstituentRef{
		{Alias: "ro", DatabaseName: "private", Type: "local", AccessMode: "read"},
	}))
	server := &Server{config: DefaultConfig(), sessions: map[string]*Session{}, dbManager: manager}
	server.databaseAccessModeResolver = func(roles []string) auth.DatabaseAccessMode {
		allowed := map[string]bool{"nornic": true, "vis": true}
		return staticDBAccessMode{visible: allowed, access: allowed}
	}
	// RUN's answer: its failure, or that it did not fail.
	answer := func(t *testing.T, statement, database string) string {
		serverConn, clientConn := net.Pipe()
		defer serverConn.Close()
		defer clientConn.Close()
		session := newTestSession(serverConn, &mockExecutor{})
		session.server = server
		session.authenticated = true
		session.authResult = &BoltAuthResult{Authenticated: true, Username: "reader", Roles: []string{"reader"}, Permissions: []string{"read"}}
		done := make(chan error, 1)
		go func() {
			done <- session.handleRun(buildRunMessageData(statement, nil, map[string]any{"db": database}))
		}()
		code, message, failureErr := AssertFailure(t, clientConn)
		require.NoError(t, <-done)
		if failureErr != nil {
			return "no failure"
		}
		return code + ": " + message
	}
	for _, scenario := range []struct{ session, hidden, missing string }{
		{"vis", "vis.sec", "vis.abs"},
		{"nornic", "hiddencomp", "absentcomp"},
		{"nornic", "hiddencomp.ro", "absentcomp.ro"},
		{"nornic", "private", "missing"},
	} {
		for _, form := range []string{
			"USE {name} UNWIND [] AS u RETURN u",
			"UNWIND [] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
			"UNWIND [1/0] AS u CALL { USE {name} RETURN 1 AS x } RETURN x",
			"MATCH (n:NoSuchLabel) CALL { USE {name} RETURN 1 AS x } RETURN x",
		} {
			t.Run(scenario.hidden+" "+form, func(t *testing.T) {
				normalize := func(text string) string {
					return strings.ReplaceAll(strings.ReplaceAll(text, scenario.hidden, "NAME"), scenario.missing, "NAME")
				}
				require.Equal(t,
					normalize(answer(t, strings.ReplaceAll(form, "{name}", scenario.missing), scenario.session)),
					normalize(answer(t, strings.ReplaceAll(form, "{name}", scenario.hidden), scenario.session)))
			})
		}
	}
}
