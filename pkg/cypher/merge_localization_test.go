package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
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
}

func TestCypherMergeLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced merge failure")
	err := localizedError(localization.CypherMergeCreateNodeFailed(cause), cause)

	localizedErr := requireCypherMergeLocalizedError(t, err, localization.MessageCypherMergeCreateNodeFailed, "failed to create node in MERGE: forced merge failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "forced merge failure", localizedErr.Message.Data["Cause"])
}
