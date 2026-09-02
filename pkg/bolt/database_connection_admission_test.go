package bolt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionDatabaseConnectionAdmissionLifecycle(t *testing.T) {
	admitted := 0
	released := 0
	server := &Server{}
	server.SetDatabaseConnectionAdmission(func(databaseName string) error {
		require.Equal(t, "alpha", databaseName)
		admitted++
		return nil
	}, func(databaseName string) {
		require.Equal(t, "alpha", databaseName)
		released++
	})
	session := &Session{server: server}

	require.NoError(t, session.bindDatabaseConnection("alpha"))
	require.NoError(t, session.bindDatabaseConnection("alpha"))
	require.Equal(t, 1, admitted)
	session.releaseDatabaseConnection()
	session.releaseDatabaseConnection()
	require.Equal(t, 1, released)
}

func TestSessionDatabaseConnectionAdmissionFailureDoesNotRelease(t *testing.T) {
	wantErr := errors.New("connection limit")
	released := 0
	server := &Server{}
	server.SetDatabaseConnectionAdmission(func(string) error { return wantErr }, func(string) { released++ })
	session := &Session{server: server}

	require.ErrorIs(t, session.bindDatabaseConnection("alpha"), wantErr)
	session.releaseDatabaseConnection()
	require.Zero(t, released)
}
