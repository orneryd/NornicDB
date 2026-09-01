package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireCypherMergeLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherMergeLocalizedErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	exec := &StorageExecutor{}

	_, err := exec.parseSetMergeMapLiteralStrict(context.Background(), "broken")
	requireCypherMergeLocalizedError(t, err, localization.MessageCypherMergeMapLiteralEnclosureRequired, "map literal must be enclosed in { ... }")

	_, err = exec.executeMerge(context.Background(), "RETURN 1")
	localizedErr := requireCypherMergeLocalizedError(t, err, localization.MessageCypherMergeClauseNotFound, `MERGE clause not found in query: "RETURN 1"`)
	require.Equal(t, "RETURN 1", localizedErr.Message.Data["Query"])
}

func TestCypherMergeLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced merge failure")
	err := localizedError(localization.CypherMergeCreateNodeFailed(cause), cause)

	localizedErr := requireCypherMergeLocalizedError(t, err, localization.MessageCypherMergeCreateNodeFailed, "failed to create node in MERGE: forced merge failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "forced merge failure", localizedErr.Message.Data["Cause"])
}

func TestCypherMergeCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherMergeUnwindParameterNotFound("rows")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "no se encontró el parámetro UNWIND $rows o es nulo", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! UNWIND parameter $rows not found or is null !!]", pseudo)
}
