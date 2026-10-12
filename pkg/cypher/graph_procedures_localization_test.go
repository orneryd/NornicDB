package cypher

import (
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
		_, err := exec.callGdsGraphDrop([]interface{}{"missing"})
		localizedErr := requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphDoesNotExist, "graph 'missing' does not exist")
		require.Equal(t, "missing", localizedErr.Message.Data["Graph"])
	})

	t.Run("graph must be projected first", func(t *testing.T) {
		_, err := exec.callGdsFastRPStream([]interface{}{"missing"})
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphDoesNotExistProjectFirst, "graph 'missing' does not exist. Create it with gds.graph.project first")
	})

	t.Run("source node required", func(t *testing.T) {
		_, err := linkPredictionConfigFromArguments([]interface{}{map[string]interface{}{"topK": int64(5)}})
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresSourceNodeRequired, "sourceNode parameter required")
	})
}
