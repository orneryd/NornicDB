package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func requireCypherCompositeLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

type cypherCompositeErrorManager struct {
	*mockDatabaseManager
	createCompositeErr error
}

func (m *cypherCompositeErrorManager) CreateCompositeDatabase(context.Context, string, []interface{}) error {
	return m.createCompositeErr
}

func TestCypherCompositeLocalizedErrorsHaveTypedIdentity(t *testing.T) {
	t.Run("constituent syntax", func(t *testing.T) {
		idx := 0
		_, err := parseConstituentFromTokens([]string{"BROKEN"}, &idx)
		requireCypherCompositeLocalizedError(t, err, localization.MessageCypherCompositeConstituentAliasExpected, "invalid constituent syntax: ALIAS expected")
	})

	t.Run("machine token", func(t *testing.T) {
		idx := 0
		_, err := parseConstituentFromTokens([]string{"ALIAS", "a", "FOR", "DATABASE", "db", "BROKEN"}, &idx)
		localizedErr := requireCypherCompositeLocalizedError(t, err, localization.MessageCypherCompositeConstituentUnexpectedToken, "invalid constituent syntax: unexpected token 'BROKEN'")
		require.Equal(t, "BROKEN", localizedErr.Message.Data["Token"])
	})

	t.Run("manager unavailable", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.executeShowCompositeDatabases(context.Background(), "SHOW COMPOSITE DATABASES")
		localizedErr := requireCypherCompositeLocalizedError(t, err, localization.MessageCypherCompositeDatabaseManagerUnavailable, "database manager not available - SHOW COMPOSITE DATABASES requires multi-database support")
		require.Equal(t, "SHOW COMPOSITE DATABASES", localizedErr.Message.Data["Command"])
	})
}

func TestCypherCompositeLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced composite failure")
	exec := &StorageExecutor{}
	exec.SetDatabaseManager(&cypherCompositeErrorManager{
		mockDatabaseManager: newMockDatabaseManager(t),
		createCompositeErr:  cause,
	})

	_, err := exec.executeCompositeCommand(context.Background(), "CREATE COMPOSITE DATABASE analytics ALIAS tenant FOR DATABASE tenant_db", compositeCreateCommand)
	localizedErr := requireCypherCompositeLocalizedError(t, err, localization.MessageCypherCompositeCreateDatabaseFailed, "failed to create composite database 'analytics': forced composite failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "analytics", localizedErr.Message.Data["Database"])
	require.Equal(t, "forced composite failure", localizedErr.Message.Data["Cause"])
}

func TestCompositeCommandFamilyPreservesDirectErrorPriority(t *testing.T) {
	for _, entry := range []struct {
		family compositeCommandFamily
		id     localization.MessageID
	}{
		{compositeCreateCommand, localization.MessageCypherCompositeCreateInvalidSyntax},
		{compositeAlterCommand, localization.MessageCypherCompositeAlterInvalidSyntax},
	} {
		t.Run(string(entry.family), func(t *testing.T) {
			exec := &StorageExecutor{}
			// The caller selected the family; malformed text cannot change its
			// manager-unavailable diagnostic or cause parsing to run first.
			result, err := exec.executeCompositeCommand(t.Context(), "BROKEN", entry.family)
			require.Nil(t, result)
			localized := requireCypherCompositeLocalizedError(t, err,
				localization.MessageCypherCompositeDatabaseManagerUnavailable,
				"database manager not available - "+string(entry.family)+" requires multi-database support")
			require.Equal(t, string(entry.family), localized.Message.Data["Command"])
			exec.SetDatabaseManager(newMockDatabaseManager(t))
			result, err = exec.executeCompositeCommand(t.Context(), "BROKEN", entry.family)
			require.Nil(t, result)
			requireCypherCompositeLocalizedError(t, err, entry.id, "invalid "+string(entry.family)+" syntax")
		})
	}
}
