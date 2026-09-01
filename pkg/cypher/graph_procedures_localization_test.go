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

	t.Run("graph name required", func(t *testing.T) {
		_, err := exec.callGdsGraphProject("CALL gds.graph.project()")
		localizedErr := requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphNameRequired, "graph name required for gds.graph.project")
		require.Equal(t, "gds.graph.project", localizedErr.Message.Data["Procedure"])
	})

	t.Run("graph does not exist", func(t *testing.T) {
		_, err := exec.callGdsGraphDrop("CALL gds.graph.drop('missing')")
		localizedErr := requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphDoesNotExist, "graph 'missing' does not exist")
		require.Equal(t, "missing", localizedErr.Message.Data["Graph"])
	})

	t.Run("graph must be projected first", func(t *testing.T) {
		_, err := exec.callGdsFastRPStream("CALL gds.fastRP.stream('missing')")
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresGraphDoesNotExistProjectFirst, "graph 'missing' does not exist. Create it with gds.graph.project first")
	})

	t.Run("invalid link prediction syntax", func(t *testing.T) {
		_, err := exec.parseLinkPredictionConfig(context.Background(), "CALL gds.linkPrediction.predict.stream", nil)
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresInvalidProcedureCallSyntax, "invalid procedure call syntax")
	})

	t.Run("source node required", func(t *testing.T) {
		_, err := exec.parseLinkPredictionConfig(context.Background(), "CALL gds.linkPrediction.predict.stream({topK: 5})", nil)
		requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresSourceNodeRequired, "sourceNode parameter required")
	})

	t.Run("query variable not found", func(t *testing.T) {
		_, err := exec.parseLinkPredictionConfig(context.Background(), "CALL gds.linkPrediction.predict.stream({sourceNode: id(person)})", map[string]*storage.Node{})
		localizedErr := requireCypherGraphProceduresLocalizedError(t, err, localization.MessageCypherGraphProceduresVariableNotFound, `variable "person" not found in query context (id(person) cannot be resolved)`)
		require.Equal(t, "person", localizedErr.Message.Data["Variable"])
	})
}
