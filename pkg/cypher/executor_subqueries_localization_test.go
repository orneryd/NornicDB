package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
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
		_, _, _, err := parseLeadingWithImports(originalBraceView("WITH seed"))
		requireCypherSubqueriesLocalizedError(t, err, localization.MessageCypherSubqueriesWithQueryClauseRequired, "invalid CALL {} subquery: WITH must be followed by a query clause")
	})

}

func TestCypherSubqueriesLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced subquery failure")
	err := localizedError(localization.CypherSubqueriesCallError(cause), cause)

	localizedErr := requireCypherSubqueriesLocalizedError(t, err, localization.MessageCypherSubqueriesCallError, "CALL subquery error: forced subquery failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "forced subquery failure", localizedErr.Message.Data["Cause"])
}
