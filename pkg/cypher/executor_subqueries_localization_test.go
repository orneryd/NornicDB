package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireCypherSubqueriesLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherSubqueriesLocalizedErrorsHaveTypedIdentity(t *testing.T) {
	t.Run("subquery parser", func(t *testing.T) {
		_, _, _, err := parseLeadingWithImports("WITH seed")
		requireCypherSubqueriesLocalizedError(t, err, localization.MessageCypherSubqueriesWithQueryClauseRequired, "invalid CALL {} subquery: WITH must be followed by a query clause")
	})

	t.Run("RAG request parser", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.parseRagProcedureRequest(context.Background(), "CALL db.retrieve($request)", "DB.RETRIEVE")
		localizedErr := requireCypherSubqueriesLocalizedError(t, err, localization.MessageCypherSubqueriesRAGParameterMustBeMap, "db.retrieve parameter $request must be a map")
		require.Equal(t, "db.retrieve", localizedErr.Message.Data["Procedure"])
		require.Equal(t, "$request", localizedErr.Message.Data["Parameter"])
	})
}

func TestCypherSubqueriesLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced subquery failure")
	err := localizedError(localization.CypherSubqueriesCallError(cause), cause)

	localizedErr := requireCypherSubqueriesLocalizedError(t, err, localization.MessageCypherSubqueriesCallError, "CALL subquery error: forced subquery failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "forced subquery failure", localizedErr.Message.Data["Cause"])
}

func TestCypherSubqueriesCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherSubqueriesRAGParameterMustBeMap("db.retrieve", "$request")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "el parámetro $request de db.retrieve debe ser un mapa", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! db.retrieve parameter $request must be a map !!]", pseudo)
}
