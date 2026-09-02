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
	server.SetDatabaseConnectionAdmission(func(databaseName string) (string, error) {
		return databaseName, nil
	}, func(databaseName string) error {
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
	server.SetDatabaseConnectionAdmission(func(databaseName string) (string, error) { return databaseName, nil }, func(string) error { return wantErr }, func(string) { released++ })
	session := &Session{server: server}

	require.ErrorIs(t, session.bindDatabaseConnection("alpha"), wantErr)
	session.releaseDatabaseConnection()
	require.Zero(t, released)
}

func TestSessionDatabaseConnectionAdmissionRebindsEffectiveDatabase(t *testing.T) {
	var admitted []string
	var released []string
	server := &Server{}
	server.SetDatabaseConnectionAdmission(func(databaseName string) (string, error) {
		if databaseName == "alpha-alias" {
			return "alpha", nil
		}
		return databaseName, nil
	}, func(databaseName string) error {
		admitted = append(admitted, databaseName)
		return nil
	}, func(databaseName string) {
		released = append(released, databaseName)
	})
	session := &Session{server: server}

	require.NoError(t, session.bindDatabaseConnection("alpha-alias"))
	require.NoError(t, session.bindDatabaseConnection("alpha"))
	require.NoError(t, session.bindDatabaseConnection("beta"))
	require.Equal(t, []string{"alpha", "beta"}, admitted)
	require.Equal(t, []string{"alpha"}, released)
	require.Equal(t, "beta", session.admittedDatabase)
}

func TestSessionDatabaseConnectionAdmissionKeepsPreviousBindingWhenRebindFails(t *testing.T) {
	wantErr := errors.New("beta is full")
	server := &Server{}
	server.SetDatabaseConnectionAdmission(func(databaseName string) (string, error) { return databaseName, nil }, func(databaseName string) error {
		if databaseName == "beta" {
			return wantErr
		}
		return nil
	}, func(string) {})
	session := &Session{server: server}

	require.NoError(t, session.bindDatabaseConnection("alpha"))
	require.ErrorIs(t, session.bindDatabaseConnection("beta"), wantErr)
	require.Equal(t, "alpha", session.admittedDatabase)
}
