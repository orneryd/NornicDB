package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireCypherCommandRoutingLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherCommandRoutingErrorsHaveTypedIdentity(t *testing.T) {
	exec := &StorageExecutor{}

	t.Run("shell command", func(t *testing.T) {
		_, _, err := exec.executeShellCommand(context.Background(), ":broken", nil)
		localizedErr := requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingUnknownCommand, "unknown command: :broken")
		require.Equal(t, ":broken", localizedErr.Message.Data["Command"])
	})

	t.Run("CALL", func(t *testing.T) {
		_, err := exec.executeCall(context.Background(), "CALL missing.procedure()")
		localizedErr := requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingUnknownProcedure, "unknown procedure: missing.procedure (try SHOW PROCEDURES for available procedures)")
		require.Equal(t, "missing.procedure", localizedErr.Message.Data["Procedure"])
	})

	t.Run("USE", func(t *testing.T) {
		_, _, _, err := parseLeadingUseClause("USE")
		requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingUseDatabaseRequired, "USE clause requires a database name")
	})

	t.Run("Fabric", func(t *testing.T) {
		_, err := exec.executeViaPreparedFabricWithTx(context.Background(), "RETURN 1", nil, nil, true, nil)
		requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingFabricNotPrepared, "fabric execution was not prepared")
	})

	t.Run("YIELD", func(t *testing.T) {
		err := validateYieldColumnsExist([]string{"name"}, &yieldClause{items: []yieldItem{{name: "missing"}}})
		localizedErr := requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingUnknownYieldColumn, "unknown YIELD column: missing")
		require.Equal(t, "missing", localizedErr.Message.Data["Column"])
	})

	t.Run("procedure minimum arguments", func(t *testing.T) {
		err := validateProcedureArgCount(ProcedureSpec{Name: "db.test", MinArgs: 2, MaxArgs: 3}, []interface{}{1})
		localizedErr := requireCypherCommandRoutingLocalizedError(t, err, localization.MessageCypherCommandRoutingProcedureMinArguments, "procedure db.test requires at least 2 arguments, got 1")
		require.Equal(t, "db.test", localizedErr.Message.Data["Procedure"])
	})
}

func TestCypherCommandRoutingWrappedErrorsPreserveCause(t *testing.T) {
	cause := errors.New("forced routing failure")

	useErr := localizedError(localization.CypherCommandRoutingUseFailed("tenant.db", cause), cause)
	requireCypherCommandRoutingLocalizedError(t, useErr, localization.MessageCypherCommandRoutingUseFailed, "USE tenant.db failed: forced routing failure")
	require.ErrorIs(t, useErr, cause)

	fabricErr := localizedError(localization.CypherCommandRoutingFabricConstituentsFailed("analytics", cause), cause)
	localizedErr := requireCypherCommandRoutingLocalizedError(t, fabricErr, localization.MessageCypherCommandRoutingFabricConstituentsFailed, "failed to get constituents for 'analytics': forced routing failure")
	require.ErrorIs(t, fabricErr, cause)
	require.Equal(t, "analytics", localizedErr.Message.Data["Database"])
}

func TestCypherCommandRoutingCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherCommandRoutingUnknownProcedure("missing.procedure")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "procedimiento desconocido: missing.procedure (pruebe SHOW PROCEDURES para ver los procedimientos disponibles)", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! unknown procedure: missing.procedure (try SHOW PROCEDURES for available procedures) !!]", pseudo)
}
