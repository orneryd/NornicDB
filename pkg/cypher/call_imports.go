// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// callSubqueryImportedVariables is the variables a CALL subquery imports from its
// enclosing query ("*" for every variable), read with the CALL subquery
// grammar single-database execution uses (fabric.CallImportReader). A scoped
// subquery imports what its scope clause lists (CALL (x, y), CALL (*),
// CALL ()); any other imports the items of its body's importing WITH that are
// * or a plain variable reference (WITH x, `y z`). Comments are whitespace
// here, so query normalization doesn't change what a subquery imports. A
// scope clause item that is not a variable is an error (callScopeClauseVariables).
func callSubqueryImportedVariables(ctx context.Context, scope string, scoped bool, body string) ([]string, error) {
	if scoped {
		return callScopeClauseVariables(ctx, scope)
	}
	names := importingWithVariables(ctx, body)
	return names, ctx.Err()
}

// callScopeClauseVariables is the list of a CALL scope clause, the text
// between its parentheses: "*" alone, or each variable's name, unquoted. Any
// other item (CALL (1), CALL (x + 1), CALL (x, *)), or an empty one (CALL
// (x,), CALL (,x), CALL (x,,y)), is Neo4j's syntax error for a scope clause,
// the one statement validation reports too. A word such as null or true is
// a variable's name here, as in Neo4j, and is an error only where no such
// variable is defined.
//
// The list is read by callSubquery's own scopeList production (ScopeListEntry)
// and scopeListNames, the reader of every CALL tree's scope; the list is a
// window of the statement, and no CALL statement is composed around it.
// Author of the grammar-owned form: Claude R17 implementation agent, 30 October 2026.
func callScopeClauseVariables(ctx context.Context, list string) ([]string, error) {
	names, err := cypherantlr.ReadScopeNames(ctx, cypherantlr.ReadStatement(list), cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherResultMetadataImportVariablesRequired())
	}
	if names == nil {
		return []string{"*"}, nil
	}
	return names, nil
}

func importingWithVariables(ctx context.Context, source string) []string {
	body := readQueryBodyText(ctx, source, 0, len(source))
	if len(body.branches) == 0 {
		return nil
	}
	return importingWithSyntax(body.branches[0])
}

func importingWithSyntax(branch queryBodyBranch) []string {
	if len(branch.clauses) == 0 || branch.clauses[0].kind != pipelineClauseWith {
		return nil
	}
	return importingWithNode(branch.clauses[0].syntax)
}

func importingWithNode(node cypherantlr.SyntaxNode) []string {
	plan := projectionPlanOfNode(node)
	var names []string
	if plan.star {
		names = append(names, "*")
		plan = plan.withStarExpanded(nil)
	}
	for _, item := range plan.projections {
		if item.direct && item.variable == item.alias {
			names = append(names, item.variable)
		}
	}
	return names
}
