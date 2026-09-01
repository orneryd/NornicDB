package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func requireCypherTransactionsLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherTransactionsLocalizedErrorsHaveTypedIdentityAndExactEnglish(t *testing.T) {
	t.Run("transaction lifecycle", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.handleBegin()
		requireCypherTransactionsLocalizedError(t, err, localization.MessageCypherTransactionsEngineUnsupported, "engine does not support transactions")
	})

	t.Run("syntax validation", func(t *testing.T) {
		exec := &StorageExecutor{}
		err := exec.validateSyntaxNornic("INVALID")
		requireCypherTransactionsLocalizedError(t, err, localization.MessageCypherTransactionsSyntaxStartInvalid, "syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc.)")
	})

	t.Run("multi match validation", func(t *testing.T) {
		exec := &StorageExecutor{}
		_, err := exec.executeMatchWithUnwind(context.Background(), "MATCH (n) RETURN n")
		requireCypherTransactionsLocalizedError(t, err, localization.MessageCypherTransactionsMatchWithUnwindClausesRequired, "MATCH, WITH, UNWIND, and RETURN clauses required (e.g., MATCH (n) WITH n UNWIND n.items AS item RETURN item)")
	})

	t.Run("delete guard", func(t *testing.T) {
		err := residualRelationshipDeleteError(storage.NodeID("node-1"))
		localizedErr := requireCypherTransactionsLocalizedError(t, err, localization.MessageCypherTransactionsDeleteResidualRelationships, "Cannot delete node node-1, because it still has relationships. To delete this node, you must first delete its relationships (or use DETACH DELETE)")
		require.Equal(t, "node-1", localizedErr.Message.Data["NodeID"])
	})
}

func TestCypherTransactionsDescriptorCarriesMachineData(t *testing.T) {
	message := localization.CypherTransactionsQueryTypeUnsupported("VACUUM")
	require.Equal(t, localization.MessageCypherTransactionsQueryTypeUnsupported, message.ID)
	require.Equal(t, "unsupported query type: VACUUM (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER)", message.Fallback)
	require.Equal(t, "VACUUM", message.Data["QueryType"])
}

func TestCypherTransactionsDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced commit failure")
	testCases := []struct {
		message localization.Message
		text    string
	}{
		{localization.CypherTransactionsAlreadyActive(), "transaction already active"},
		{localization.CypherTransactionsEngineUnsupported(), "engine does not support transactions"},
		{localization.CypherTransactionsPrimeNamespaceFailed(cause), "failed to prime transaction namespace: forced commit failure"},
		{localization.CypherTransactionsStartFailed(cause), "failed to start transaction: forced commit failure"},
		{localization.CypherTransactionsPinNamespaceFailed(cause), "failed to pin transaction namespace: forced commit failure"},
		{localization.CypherTransactionsConfigureFailed(cause), "failed to configure transaction: forced commit failure"},
		{localization.CypherTransactionsWALBeginFailed(cause), "failed to write WAL tx begin: forced commit failure"},
		{localization.CypherTransactionsNoActive(), "no active transaction"},
		{localization.CypherTransactionsUnknownType(), "unknown transaction type"},
		{localization.CypherTransactionsCommitFailed(cause), "commit failed: forced commit failure"},
		{localization.CypherTransactionsRollbackFailed(cause), "rollback failed: forced commit failure"},
		{localization.CypherTransactionsShowInTransactionUnsupported("SHOW THINGS"), "unsupported SHOW command in transaction: SHOW THINGS"},
		{localization.CypherTransactionsQueryInTransactionUnsupported("VACUUM"), "unsupported query type in transaction: VACUUM"},
		{localization.CypherTransactionsInvalidScriptAction("ABORT"), "invalid transaction script action: ABORT"},
		{localization.CypherTransactionsCaseBlockMissing(), "invalid transaction CASE script: missing CASE block"},
		{localization.CypherTransactionsCaseSyntaxInvalid(), "invalid transaction CASE syntax: expected CASE WHEN ... THEN ROLLBACK ELSE RETURN ... COMMIT"},
		{localization.CypherTransactionsConditionNotBoolean(42), "condition expression did not evaluate to boolean: 42"},
		{localization.CypherTransactionsQueryTypeUnsupported("VACUUM"), "unsupported query type: VACUUM (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER)"},
		{localization.CypherTransactionsReturnClauseNotFound("RETURN-less"), `RETURN clause not found in query: "RETURN-less"`},
		{localization.CypherTransactionsSyntaxStartInvalid(), "syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc.)"},
		{localization.CypherTransactionsSyntaxUnbalancedAt(7), "syntax error: unbalanced brackets at position 7"},
		{localization.CypherTransactionsSyntaxUnbalancedParentheses(), "syntax error: unbalanced parentheses"},
		{localization.CypherTransactionsSyntaxUnbalancedSquareBrackets(), "syntax error: unbalanced square brackets"},
		{localization.CypherTransactionsSyntaxUnbalancedCurlyBraces(), "syntax error: unbalanced curly braces"},
		{localization.CypherTransactionsSyntaxUnclosedQuote(), "syntax error: unclosed quote"},
		{localization.CypherTransactionsDeleteResidualRelationships("node-1"), "Cannot delete node node-1, because it still has relationships. To delete this node, you must first delete its relationships (or use DETACH DELETE)"},
		{localization.CypherTransactionsMatchWithUnwindClausesRequired(), "MATCH, WITH, UNWIND, and RETURN clauses required (e.g., MATCH (n) WITH n UNWIND n.items AS item RETURN item)"},
		{localization.CypherTransactionsStorageFailed(cause), "storage error: forced commit failure"},
		{localization.CypherTransactionsUnwindASRequired(), "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)"},
		{localization.CypherTransactionsOrderByParseFailed(), "failed to parse ORDER BY clause"},
		{localization.CypherTransactionsMultiMatchReturnRequired(), "multi-MATCH query requires RETURN clause"},
		{localization.CypherTransactionsMultipleMatchExpected(), "expected multiple MATCH clauses"},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
	}
}

func TestCypherTransactionsLocalizedErrorsPreserveCauses(t *testing.T) {
	cause := errors.New("forced transaction failure")
	testCases := []struct {
		message localization.Message
		text    string
	}{
		{localization.CypherTransactionsPrimeNamespaceFailed(cause), "failed to prime transaction namespace: forced transaction failure"},
		{localization.CypherTransactionsStartFailed(cause), "failed to start transaction: forced transaction failure"},
		{localization.CypherTransactionsPinNamespaceFailed(cause), "failed to pin transaction namespace: forced transaction failure"},
		{localization.CypherTransactionsConfigureFailed(cause), "failed to configure transaction: forced transaction failure"},
		{localization.CypherTransactionsWALBeginFailed(cause), "failed to write WAL tx begin: forced transaction failure"},
		{localization.CypherTransactionsCommitFailed(cause), "commit failed: forced transaction failure"},
		{localization.CypherTransactionsRollbackFailed(cause), "rollback failed: forced transaction failure"},
		{localization.CypherTransactionsStorageFailed(cause), "storage error: forced transaction failure"},
	}

	for _, testCase := range testCases {
		t.Run(string(testCase.message.ID), func(t *testing.T) {
			err := localizedError(testCase.message, cause)
			localizedErr := requireCypherTransactionsLocalizedError(t, err, testCase.message.ID, testCase.text)
			require.ErrorIs(t, err, cause)
			var target *localization.LocalizedError
			require.ErrorAs(t, err, &target)
			require.Same(t, localizedErr, target)
			require.Equal(t, "forced transaction failure", localizedErr.Message.Data["Cause"])
		})
	}
}

func TestCypherTransactionsCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherTransactionsInvalidScriptAction("ABORT")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "acción de script de transacción no válida: ABORT", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! invalid transaction script action: ABORT !!]", pseudo)
}
