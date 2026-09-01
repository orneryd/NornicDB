package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
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

func (m *cypherCompositeErrorManager) CreateCompositeDatabase(string, []interface{}) error {
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
		mockDatabaseManager: newMockDatabaseManager(),
		createCompositeErr:  cause,
	})

	_, err := exec.executeCreateCompositeDatabase(context.Background(), "CREATE COMPOSITE DATABASE analytics ALIAS tenant FOR DATABASE tenant_db")
	localizedErr := requireCypherCompositeLocalizedError(t, err, localization.MessageCypherCompositeCreateDatabaseFailed, "failed to create composite database 'analytics': forced composite failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "analytics", localizedErr.Message.Data["Database"])
	require.Equal(t, "forced composite failure", localizedErr.Message.Data["Cause"])
}

func TestCypherCompositeCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherCompositeConstituentUnexpectedToken("BROKEN")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "sintaxis de componente no válida: token inesperado 'BROKEN'", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! invalid constituent syntax: unexpected token 'BROKEN' !!]", pseudo)
}
