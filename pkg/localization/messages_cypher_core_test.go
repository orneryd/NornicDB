package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestCypherCoreDescriptorsPreserveExactEnglish(t *testing.T) {
	cause := errors.New("forced core failure")
	testCases := []struct {
		message Message
		text    string
	}{
		{CypherCoreEmptyQuery(), "empty query"},
		{CypherCoreCompositeTargetRequired(), "Neo.ClientError.Statement.NotAllowed: Queries on composite databases require explicit graph targeting. Use USE <composite>.<alias> to target a specific constituent"},
		{CypherCoreInvalidLabelName("9Person"), `invalid label name: "9Person" (must be alphanumeric starting with letter or underscore)`},
		{CypherCoreInvalidLabelReserved("MATCH"), `invalid label name: "MATCH" (contains reserved keyword)`},
		{CypherCoreInvalidPropertyKey("bad key"), `invalid property key: "bad key" (must be alphanumeric starting with letter or underscore)`},
		{CypherCoreInvalidPropertyValue("name"), `invalid property value for key "name": malformed syntax`},
		{CypherCoreEmbeddingTransactionStorageRequired(), "WITH EMBEDDING requires transaction-capable storage"},
		{CypherCoreImplicitTransactionPrimeFailed(cause), "failed to prime implicit transaction namespace: forced core failure"},
		{CypherCoreImplicitTransactionStartFailed(cause), "failed to start implicit transaction: forced core failure"},
		{CypherCoreImplicitTransactionPinFailed(cause), "failed to pin implicit transaction namespace: forced core failure"},
		{CypherCoreImplicitTransactionConfigureFailed(cause), "failed to configure implicit transaction: forced core failure"},
		{CypherCoreImplicitTransactionWALBeginFailed(cause), "failed to write WAL tx begin: forced core failure"},
		{CypherCoreImplicitTransactionCommitFailed(cause), "commit failed: forced core failure"},
		{CypherCoreEmbeddingConfiguredRequired(), "WITH EMBEDDING requires configured embedder"},
		{CypherCoreEmbeddingChunkFailed("node-1", cause), "WITH EMBEDDING chunking failed for node node-1: forced core failure"},
		{CypherCoreEmbeddingNodeFailed("node-1", cause), "WITH EMBEDDING embed failed for node node-1: forced core failure"},
		{CypherCoreEmbeddingEmptyVector("node-1"), "WITH EMBEDDING embed returned empty vector for node node-1"},
		{CypherCoreOptionalMatchRequired(), "OPTIONAL must be followed by MATCH"},
		{CypherCoreUnterminatedStringLiteral(), "unterminated string literal"},
		{CypherCoreParseFailed(cause), "parse error: forced core failure"},
		{CypherCoreCaseEnvelopeInvalid(), "invalid CASE expression: must start with CASE and end with END"},
		{CypherCoreCaseWhenRequired(), "CASE expression must have at least one WHEN clause"},
		{CypherCoreCaseThenRequired("x"), "WHEN clause must have THEN: x"},
		{CypherCoreFulltextUnexpectedToken("AND"), `query cannot be parsed: unexpected token "AND"`},
		{CypherCoreFulltextNumberAfterBoostExpected(), "query cannot be parsed: expected number after ^"},
		{CypherCoreFulltextBadBoost("nope", cause), `query cannot be parsed: bad boost "nope"`},
		{CypherCoreFulltextClosingParenthesisRequired(), "query cannot be parsed: missing ')'"},
		{CypherCoreFulltextRangeTORequired(), "query cannot be parsed: expected TO in range"},
		{CypherCoreFulltextRangeCloseRequired(), "query cannot be parsed: expected ] or } to close range"},
		{CypherCoreFulltextRangeEndpointRequired(), "query cannot be parsed: expected range endpoint"},
		{CypherCoreFulltextBadRegex("[", cause), "query cannot be parsed: bad regex /[/: forced core failure"},
		{CypherCoreFulltextBadWildcard(cause), "query cannot be parsed: bad wildcard: forced core failure"},
		{CypherCoreIndexHintNotFound("USING INDEX n:Person(name)", "Person", "name"), "no index found for hint: USING INDEX n:Person(name) (index on :Person(name) does not exist)"},
		{CypherCoreExecutionPlanBuildFailed(cause), "failed to build execution plan: forced core failure"},
		{CypherCoreTypedDecodeRowFailed(cause), "failed to decode row: forced core failure"},
		{CypherCoreTypedDestinationPointerRequired(), "dest must be a non-nil pointer"},
		{CypherCoreTypedDestinationUnsupported("int"), "unsupported destination type: int"},
		{CypherCoreTypedFieldFailed("name", cause), "field name: forced core failure"},
		{CypherCoreTypedTimeParseFailed("not-a-time", cause), "cannot parse time: not-a-time"},
		{CypherCoreTypedAssignmentFailed(42, "string"), "cannot assign int to string"},
		{CypherCoreEmbedderNotConfigured(), "no embedder configured"},
		{CypherCoreEmbeddingNoOutput(), "failed to embed query (no embeddings produced)"},
	}

	require.Len(t, testCases, 42)
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, testCase := range testCases {
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.message.ID)
		rendered, tag, err := manager.Render(WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, testCase.message.ID)
		require.Equal(t, language.AmericanEnglish, tag, testCase.message.ID)
		require.Equal(t, testCase.text, rendered, testCase.message.ID)
	}
}

func TestCypherCoreLocalizedErrorPreservesCauseIdentity(t *testing.T) {
	cause := errors.New("forced core failure")
	err := NewLocalizedError(string(MessageCypherCoreParseFailed), CypherCoreParseFailed(cause), cause)

	require.ErrorIs(t, err, cause)
	var localizedErr *LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, MessageCypherCoreParseFailed, localizedErr.Message.ID)
	require.Equal(t, "forced core failure", localizedErr.Message.Data["Cause"])
}

func TestCypherCoreCatalogRendering(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := CypherCoreCaseThenRequired("n.active")

	spanish, tag, err := manager.Render(WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "la cláusula WHEN debe tener THEN: n.active", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! WHEN clause must have THEN: n.active !!]", pseudo)
}
