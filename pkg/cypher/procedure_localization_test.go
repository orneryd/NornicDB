package cypher

import (
	"context"
	"errors"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireCypherProceduresLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCompatibilityProcedureErrorsHaveTypedIdentity(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	t.Run("null argument", func(t *testing.T) {
		_, err := exec.callDbIndexVectorCreateIndex(context.Background(), []interface{}{"idx", nil, "embedding", int64(3)}, storage.ConstraintEntityNode)
		localizedErr := requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresArgumentNull, "db.index.vector.createNodeIndex: argument label is null")
		require.Equal(t, "label", localizedErr.Message.Data["Argument"])
	})

	t.Run("named argument", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{})
		_, err := exec.callDbIndexVectorQueryRelationships(ctx, "CALL db.index.vector.queryRelationships('idx', 10, $embedding)")
		localizedErr := requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresParameterNotProvided, "parameter $embedding not provided")
		require.Equal(t, "embedding", localizedErr.Message.Data["Parameter"])
	})

	t.Run("argument type", func(t *testing.T) {
		_, err := exec.callDbIndexVectorCreateIndex(context.Background(), []interface{}{"idx", "REL", "embedding", "nope"}, storage.ConstraintEntityRelationship)
		localizedErr := requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresArgumentType, "db.index.vector.createRelationshipIndex: argument dimension must be INTEGER, not STRING")
		require.Equal(t, "dimension", localizedErr.Message.Data["Argument"])
		code, _ := nerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
	})
}

func TestProcedureDDLErrorsHaveTypedIdentity(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	t.Run("invalid argument", func(t *testing.T) {
		_, err := parseProcedureArgNames("9invalid")
		localizedErr := requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresInvalidArgumentName, "invalid procedure argument name: 9invalid")
		require.Equal(t, "9invalid", localizedErr.Message.Data["Argument"])
	})

	t.Run("invalid mode", func(t *testing.T) {
		_, _, _, err := exec.compilePersistedProcedure(persistedProcedureRecord{Mode: "UNKNOWN"})
		requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresInvalidMode, "invalid procedure mode: UNKNOWN")
	})

	t.Run("handler argument count", func(t *testing.T) {
		_, handler, _, err := exec.compilePersistedProcedure(persistedProcedureRecord{Name: "custom.echo", Mode: "READ", Body: "RETURN 1", ArgNames: []string{"value"}})
		require.NoError(t, err)
		_, err = handler(context.Background(), exec, "", nil)
		localizedErr := requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresArgumentCount, "procedure custom.echo requires 1 arguments, got 0")
		require.Equal(t, "custom.echo", localizedErr.Message.Data["Procedure"])
		require.Equal(t, 1, localizedErr.Message.Data["Expected"])
		require.Equal(t, 0, localizedErr.Message.Data["Actual"])
	})

	t.Run("wrapped persistence cause", func(t *testing.T) {
		cause := errors.New("forced persistence failure")
		err := localizedError(localization.CypherProceduresPersistCatalogFailed(cause), cause)
		requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresPersistCatalogFailed, "failed to persist procedure catalog: forced persistence failure")
		require.ErrorIs(t, err, cause)
	})

	t.Run("registry reload sentinel", func(t *testing.T) {
		cause := errors.New("forced reload failure")
		err := localizedError(localization.CypherProceduresRegistryReloadFailed(cause), nerrors.ErrProcedureRegistryReloadFailed)
		requireCypherProceduresLocalizedError(t, err, localization.MessageCypherProceduresRegistryReloadFailed, "cypher: procedure registry reload failed: forced reload failure")
		require.ErrorIs(t, err, nerrors.ErrProcedureRegistryReloadFailed)
		require.NotErrorIs(t, err, cause)
	})
}
