package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func requireCypherGraphProceduresLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherGraphProcedureErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	t.Run("graph does not exist", func(t *testing.T) {
		_, err := exec.callGdsGraphDrop(context.Background(), []interface{}{"missing"})
		localizedErr := requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphNotFound, "Graph with name `missing` does not exist on database `"+exec.databaseName()+"`. It might exist on another database.")
		require.Equal(t, "missing", localizedErr.Message.Data["Graph"])
	})

	t.Run("graph must be projected first", func(t *testing.T) {
		_, err := exec.callGdsFastRPStream([]interface{}{"missing", map[string]interface{}{"embeddingDimension": int64(4)}})
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphNotFound, "Graph with name `missing` does not exist on database `"+exec.databaseName()+"`. It might exist on another database.")
	})

	t.Run("source node required", func(t *testing.T) {
		_, err := linkPredictionConfigFromArguments([]interface{}{map[string]interface{}{"topK": int64(5)}})
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresSourceNodeRequired, "sourceNode parameter required")
	})
}
