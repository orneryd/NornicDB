package cypher

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestResidualCypherFilesContainNoRawReturnedErrors(t *testing.T) {
	files := []string{"clauses.go", "create.go", "executor_mutations.go", "executor_show.go"}

	for _, path := range files {
		t.Run(path, func(t *testing.T) {
			fileSet := token.NewFileSet()
			file, err := parser.ParseFile(fileSet, path, nil, 0)
			require.NoError(t, err)

			var positions []token.Position
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				if (identifier.Name == "fmt" && selector.Sel.Name == "Errorf") ||
					(identifier.Name == "errors" && selector.Sel.Name == "New") {
					positions = append(positions, fileSet.Position(call.Pos()))
				}
				return true
			})

			require.Empty(t, positions, "query-visible errors must use localized descriptors")
		})
	}
}

func TestResidualCypherDescriptorsPreserveContracts(t *testing.T) {
	cause := errors.New("forced failure")
	testCases := []struct {
		message localization.Message
		id      localization.MessageID
		text    string
	}{
		{localization.CypherResidualWithClauseNotFound("RETURN 1"), localization.MessageCypherResidualWithClauseNotFound, `WITH clause not found in query: "RETURN 1"`},
		{localization.CypherResidualUnwindClauseNotFound("RETURN 1"), localization.MessageCypherResidualUnwindClauseNotFound, `UNWIND clause not found in query: "RETURN 1"`},
		{localization.CypherResidualUnionClauseNotFound("RETURN 1"), localization.MessageCypherResidualUnionClauseNotFound, `UNION clause not found in query: "RETURN 1"`},
		{localization.CypherResidualUnionAllClauseNotFound("RETURN 1"), localization.MessageCypherResidualUnionAllClauseNotFound, `UNION ALL clause not found in query: "RETURN 1"`},
		{localization.CypherResidualUnionBranchFailed(2, "RETURN broken", cause), localization.MessageCypherResidualUnionBranchFailed, `error in UNION query 2 ("RETURN broken"): forced failure`},
		{localization.CypherResidualUnionColumnCountMismatch(1, 2), localization.MessageCypherResidualUnionColumnCountMismatch, "UNION queries must return the same number of columns (got 1 and 2)"},
		{localization.CypherResidualOptionalMatchNotFound("MATCH (n)"), localization.MessageCypherResidualOptionalMatchNotFound, `OPTIONAL MATCH not found in query: "MATCH (n)"`},
		{localization.CypherResidualCompoundOptionalMatchNotFound("MATCH (n)"), localization.MessageCypherResidualCompoundOptionalMatchNotFound, `OPTIONAL MATCH not found in compound query: "MATCH (n)"`},
		{localization.CypherResidualMatchNodePatternParseFailed("broken"), localization.MessageCypherResidualMatchNodePatternParseFailed, `could not parse node pattern from MATCH clause: "broken"`},
		{localization.CypherResidualInitialNodesLookupFailed(cause), localization.MessageCypherResidualInitialNodesLookupFailed, "failed to get initial nodes: forced failure"},
		{localization.CypherResidualSumArithmeticTermUnsupported("n.value"), localization.MessageCypherResidualSumArithmeticTermUnsupported, "unsupported SUM arithmetic term: n.value"},
		{localization.CypherResidualSumNumericRequired("bad", "n.value"), localization.MessageCypherResidualSumNumericRequired, `SUM() requires numeric values, got string in expression "n.value"`},
		{localization.CypherResidualReturnClauseRequired(), localization.MessageCypherResidualReturnClauseRequired, "RETURN clause required"},
		{localization.CypherResidualForeachClauseNotFound("RETURN 1"), localization.MessageCypherResidualForeachClauseNotFound, `FOREACH clause not found in query: "RETURN 1"`},
		{localization.CypherResidualLoadCSVUnsupported(), localization.MessageCypherResidualLoadCSVUnsupported, "LOAD CSV is not supported in NornicDB embedded mode"},
		{localization.CypherResidualEmptyLabelAfterColon("(n:)"), localization.MessageCypherResidualEmptyLabelAfterColon, "empty label name after colon in pattern: (n:)"},
		{localization.CypherResidualPropertyMapSyntaxInvalid("(n {x: 1)"), localization.MessageCypherResidualPropertyMapSyntaxInvalid, "invalid property map syntax in pattern: (n {x: 1)"},
		{localization.CypherResidualRelationshipConnectorExpected("broken"), localization.MessageCypherResidualRelationshipConnectorExpected, "invalid relationship pattern: expected -[ or <-[, got: broken"},
		{localization.CypherResidualCreateRelationshipInvalid("(a)-[r]"), localization.MessageCypherResidualCreateRelationshipInvalid, "invalid relationship pattern in CREATE: (a)-[r]"},
		{localization.CypherResidualWithItemInvalid("n AS"), localization.MessageCypherResidualWithItemInvalid, `invalid WITH item: "n AS"`},
		{localization.CypherResidualCreateSetScopeEntityRequired("value"), localization.MessageCypherResidualCreateSetScopeEntityRequired, `WITH item "value" does not resolve to a node or relationship in CREATE...SET scope`},
		{localization.CypherResidualCreateWithExpressionInvalid("missing"), localization.MessageCypherResidualCreateWithExpressionInvalid, `invalid CREATE...WITH query: invalid WITH expression "missing"`},
		{localization.CypherResidualMergePatternInvalid("broken"), localization.MessageCypherResidualMergePatternInvalid, "invalid pattern: broken"},
		{localization.CypherResidualSetMergeMapOrParameterRequired("broken"), localization.MessageCypherResidualSetMergeMapOrParameterRequired, `SET += requires a map or parameter (got: "broken")`},
		{localization.CypherResidualSetAssignmentInvalid("broken"), localization.MessageCypherResidualSetAssignmentInvalid, `invalid SET assignment: "broken" (expected n.property = value or n:Label)`},
		{localization.CypherResidualSetEntityAssignmentInvalid("n = broken", cause), localization.MessageCypherResidualSetEntityAssignmentInvalid, `invalid SET assignment: "n = broken" (expected variable.property = value or variable = {property: value}): forced failure`},
		{localization.CypherResidualCollectSubquerySyntaxInvalid(), localization.MessageCypherResidualCollectSubquerySyntaxInvalid, "invalid COLLECT subquery syntax"},
		{localization.CypherResidualCollectSubqueryReturnRequired(), localization.MessageCypherResidualCollectSubqueryReturnRequired, "COLLECT subquery must have a RETURN clause"},
		{localization.CypherResidualCollectSubqueryExecutionFailed(cause), localization.MessageCypherResidualCollectSubqueryExecutionFailed, "COLLECT subquery execution failed: forced failure"},
		{localization.CypherResidualPolicyDisallowed("deny", "Source", "LINKS", "Target"), localization.MessageCypherResidualPolicyDisallowed, `policy constraint "deny" violated: (Source)-[:LINKS]->(Target) is DISALLOWED`},
		{localization.CypherResidualPolicyAllowedRequired("LINKS"), localization.MessageCypherResidualPolicyAllowedRequired, "policy constraint violated: no ALLOWED policy permits edge of type LINKS with these endpoint labels"},
		{localization.CypherResidualCompositeShowTargetRequired("SHOW INDEXES"), localization.MessageCypherResidualCompositeShowTargetRequired, "Neo.ClientError.Statement.NotAllowed: SHOW INDEXES on composite databases requires a constituent target. Use USE <composite>.<alias> SHOW INDEXES"},
		{localization.CypherResidualShowAliasesManagerUnavailable(), localization.MessageCypherResidualShowAliasesManagerUnavailable, "database manager not available - SHOW ALIASES requires multi-database support"},
		{localization.CypherResidualShowAliasesSyntaxInvalid(), localization.MessageCypherResidualShowAliasesSyntaxInvalid, "invalid SHOW ALIASES syntax"},
	}

	require.Len(t, testCases, 34)
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, testCase := range testCases {
		require.Equal(t, testCase.id, testCase.message.ID)
		require.Equal(t, testCase.text, testCase.message.Fallback, testCase.id)
		localized := localizedError(testCase.message, nil)
		require.EqualError(t, localized, testCase.text)

		var localizedErr *localization.LocalizedError
		require.ErrorAs(t, localized, &localizedErr)
		require.Equal(t, testCase.id, localizedErr.Message.ID)
		require.Equal(t, string(testCase.id), localizedErr.Code)

		rendered, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.AmericanEnglish), testCase.message)
		require.NoError(t, err, testCase.id)
		require.Equal(t, language.AmericanEnglish, tag, testCase.id)
		require.Equal(t, testCase.text, rendered, testCase.id)
	}

	t.Run("cause identity", func(t *testing.T) {
		cause := errors.New("forced branch failure")
		err := localizedError(localization.CypherResidualUnionBranchFailed(2, "RETURN broken", cause), cause)
		require.EqualError(t, err, `error in UNION query 2 ("RETURN broken"): forced branch failure`)
		require.ErrorIs(t, err, cause)
	})

	t.Run("Neo4j code remains machine data", func(t *testing.T) {
		message := localization.CypherResidualCompositeShowTargetRequired("SHOW INDEXES")
		require.Equal(t, "Neo.ClientError.Statement.NotAllowed", message.Data["Code"])
		require.Equal(t, "Neo.ClientError.Statement.NotAllowed: SHOW INDEXES on composite databases requires a constituent target. Use USE <composite>.<alias> SHOW INDEXES", message.Fallback)
	})
}

func TestResidualCypherCatalogLocales(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.CypherResidualSetMergeMapOrParameterRequired("broken")

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, `SET += requiere un mapa o parámetro (recibido: "broken")`, spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, `[!! SET += requires a map or parameter (got: "broken") !!]`, pseudo)
}
