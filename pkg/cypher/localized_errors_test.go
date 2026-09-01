package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestCypherMutationLocalizedErrors(t *testing.T) {
	requireMessageID := func(t *testing.T, err error, expected localization.MessageID) *localization.LocalizedError {
		t.Helper()
		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, err, &localizedErr)
		require.Equal(t, expected, localizedErr.Message.ID)
		require.Equal(t, string(expected), localizedErr.Code)
		return localizedErr
	}

	t.Run("validation error has typed identity and named argument", func(t *testing.T) {
		err := localizedError(localization.CypherMutationsInvalidLabelName("9Person"), nil)

		require.EqualError(t, err, `invalid label name: "9Person" (must be alphanumeric starting with letter or underscore)`)
		localizedErr := requireMessageID(t, err, localization.MessageCypherMutationsInvalidLabelName)
		require.Equal(t, "9Person", localizedErr.Message.Data["Label"])
	})

	t.Run("wrapper preserves cause", func(t *testing.T) {
		cause := errors.New("storage failure")
		err := localizedError(localization.CypherMutationsCreateNodeFailed(cause), cause)

		require.EqualError(t, err, "failed to create node: storage failure")
		require.ErrorIs(t, err, cause)
		localizedErr := requireMessageID(t, err, localization.MessageCypherMutationsCreateNodeFailed)
		require.Equal(t, "storage failure", localizedErr.Message.Data["Cause"])
	})

	t.Run("clause validation paths expose stable IDs", func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		ctx := context.Background()

		_, err := exec.executeDelete(ctx, "DELETE n")
		require.EqualError(t, err, "DELETE requires a MATCH clause first (e.g., MATCH (n) DELETE n)")
		requireMessageID(t, err, localization.MessageCypherMutationsDeleteMatchRequired)

		_, err = exec.executeSet(ctx, "SET n.value = 1")
		require.EqualError(t, err, "SET requires a MATCH clause first (e.g., MATCH (n) SET n.property = value)")
		requireMessageID(t, err, localization.MessageCypherMutationsSetMatchRequired)

		_, err = exec.executeRemove(ctx, "REMOVE n.value")
		require.EqualError(t, err, "REMOVE requires a MATCH clause first (e.g., MATCH (n) REMOVE n.property)")
		requireMessageID(t, err, localization.MessageCypherMutationsRemoveMatchRequired)

		_, err = exec.executeUnwind(ctx, "UNWIND [1] RETURN 1")
		require.EqualError(t, err, "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
		requireMessageID(t, err, localization.MessageCypherMutationsUnwindASRequired)

		_, err = exec.executeJoinedRowsWithOptionalMatch(ctx, nil, "source", "target", "rel", "WITH source RETURN source")
		require.EqualError(t, err, "WITH, OPTIONAL MATCH, and RETURN clauses required")
		requireMessageID(t, err, localization.MessageCypherMutationsWithOptionalReturnRequired)

		_, err = exec.executeForeach(ctx, "FOREACH value")
		require.EqualError(t, err, "FOREACH requires parentheses (e.g., FOREACH (x IN list | SET ...))")
		requireMessageID(t, err, localization.MessageCypherMutationsForeachParenthesesRequired)
	})

	t.Run("nested wrappers retain outer and inner IDs plus sentinel cause", func(t *testing.T) {
		cause := errors.New("storage failure")
		inner := localizedError(localization.CypherMutationsCreateNodeFailed(cause), cause)
		outer := localizedError(localization.CypherMutationsUnwindMutationFailed(inner), inner)

		require.EqualError(t, outer, "UNWIND mutation failed: failed to create node: storage failure")
		require.ErrorIs(t, outer, cause)
		requireMessageID(t, outer, localization.MessageCypherMutationsUnwindMutationFailed)
		requireMessageID(t, errors.Unwrap(outer), localization.MessageCypherMutationsCreateNodeFailed)
	})
}
