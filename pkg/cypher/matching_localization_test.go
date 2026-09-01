package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func requireCypherMatchingLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestCypherMatchingLocalizedErrorsHaveTypedIdentity(t *testing.T) {
	exec := &StorageExecutor{}

	t.Run("MATCH validation", func(t *testing.T) {
		_, err := exec.executeMatch(context.Background(), "MATCH RETURN n")
		requireCypherMatchingLocalizedError(t, err, localization.MessageCypherMatchingMatchPatternRequired, "MATCH clause requires a pattern")
	})

	t.Run("shortest path validation", func(t *testing.T) {
		_, err := exec.parseShortestPathQuery(context.Background(), "MATCH (n) RETURN n")
		requireCypherMatchingLocalizedError(t, err, localization.MessageCypherMatchingShortestPathQueryExpected, "not a shortest path query")
	})

	t.Run("optional endpoint validation", func(t *testing.T) {
		_, err := exec.parseOptionalClauseEndpoints(context.Background(), "-[r]->")
		requireCypherMatchingLocalizedError(t, err, localization.MessageCypherMatchingOptionalMatchNodeEndpointMissing, `optional match pattern "-[r]->" has no node endpoint`)
	})

	t.Run("aggregate validation", func(t *testing.T) {
		_, err := parseTraversalAggregateCall("count()")
		requireCypherMatchingLocalizedError(t, err, localization.MessageCypherMatchingFunctionParametersInsufficient, "insufficient parameters for function 'count'")
	})
}

func TestCypherMatchingLocalizedErrorPreservesCause(t *testing.T) {
	cause := errors.New("forced matching failure")
	err := localizedError(localization.CypherMatchingStorageFailed(cause), cause)
	localizedErr := requireCypherMatchingLocalizedError(t, err, localization.MessageCypherMatchingStorageFailed, "storage error: forced matching failure")
	require.ErrorIs(t, err, cause)
	require.Equal(t, "forced matching failure", localizedErr.Message.Data["Cause"])
}

func TestCypherMatchingDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced matching failure")
	testCases := []struct {
		message localization.Message
		text    string
	}{
		{localization.CypherMatchingMatchPatternRequired(), "MATCH clause requires a pattern"},
		{localization.CypherMatchingMatchNodePatternRequired(), "MATCH clause requires a node pattern, not just a relationship pattern"},
		{localization.CypherMatchingReturnExpressionRequired(), "RETURN clause requires at least one expression"},
		{localization.CypherMatchingReturnExpressionEmpty(), "RETURN clause contains empty expression"},
		{localization.CypherMatchingStorageFailed(cause), "storage error: forced matching failure"},
		{localization.CypherMatchingCollectSubqueryFailed(cause), "COLLECT subquery failed: forced matching failure"},
		{localization.CypherMatchingMatchUnwindClausesRequired(), "MATCH and UNWIND clauses required (e.g., MATCH (n) UNWIND n.items AS item RETURN item)"},
		{localization.CypherMatchingUnwindASRequired(), "UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)"},
		{localization.CypherMatchingWithReturnClausesRequired(), "WITH and RETURN clauses required"},
		{localization.CypherMatchingWithOptionalMatchReturnClausesRequired(), "WITH, OPTIONAL MATCH, and RETURN clauses required"},
		{localization.CypherMatchingOrderByParseFailed(), "failed to parse ORDER BY clause"},
		{localization.CypherMatchingMatchPatternVariableMissing("(Person)"), `invalid MATCH pattern: missing variable in "(Person)"`},
		{localization.CypherMatchingTraversalPatternInvalid("(a)-[r"), "invalid traversal pattern: (a)-[r"},
		{localization.CypherMatchingReturnAfterWithRequired(), "RETURN clause required after WITH"},
		{localization.CypherMatchingSkipParseFailed(), "failed to parse SKIP clause"},
		{localization.CypherMatchingLimitParseFailed(), "failed to parse LIMIT clause"},
		{localization.CypherMatchingShortestPathQueryExpected(), "not a shortest path query"},
		{localization.CypherMatchingShortestPathSyntaxInvalid(), "invalid shortestPath syntax"},
		{localization.CypherMatchingPathPatternInvalid("(a)-[r"), "invalid path pattern: (a)-[r"},
		{localization.CypherMatchingShortestPathStartVariableUnresolved("a"), `shortestPath: could not resolve start variable "a" from preceding MATCH clause`},
		{localization.CypherMatchingShortestPathEndVariableUnresolved("b"), `shortestPath: could not resolve end variable "b" from preceding MATCH clause`},
		{localization.CypherMatchingOptionalMatchNodeEndpointMissing("-[r]->(b)"), `optional match pattern "-[r]->(b)" has no node endpoint`},
		{localization.CypherMatchingOptionalMatchNodeEndpointUnterminated("(a-[r]->(b)"), `optional match pattern "(a-[r]->(b)" has an unterminated node endpoint`},
		{localization.CypherMatchingOptionalMatchTargetEndpointMissing("(a)-[r]->"), `optional match pattern "(a)-[r]->" has no target endpoint`},
		{localization.CypherMatchingOptionalMatchTargetEndpointUnterminated("(a)-[r]->(b"), `optional match pattern "(a)-[r]->(b" has an unterminated target endpoint`},
		{localization.CypherMatchingInitialTraversalMatchFailed(cause), "failed to execute initial traversal MATCH: forced matching failure"},
		{localization.CypherMatchingAggregateCallExpected("count"), `not a whole aggregate call: "count"`},
		{localization.CypherMatchingFunctionParametersInsufficient("count"), "insufficient parameters for function 'count'"},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
	}

	message := localization.CypherMatchingMatchPatternVariableMissing("(Person)")
	require.Equal(t, `"(Person)"`, message.Data["Clause"])
}
