package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// callScopeClauseVariables is the list of a CALL scope clause, the text
// between its parentheses: "*" alone, or each variable's name, unquoted. Any
// other item (CALL (1), CALL (x + 1), CALL (x, *)), or an empty one (CALL
// (x,), CALL (,x), CALL (x,,y)), is Neo4j's syntax error for a scope clause,
// the one statement validation reports too. A word such as null or true is
// a variable's name here, as in Neo4j, and is an error only where no such
// variable is defined.
func callScopeClauseVariables(ctx context.Context, list string) ([]string, error) {
	const prefix = "CALL ("
	syntax, err := cypherantlr.ParseSyntax(ctx, prefix+list+") { RETURN 0 }", cypherantlr.CallEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherResultMetadataImportVariablesRequired())
	}
	scope := scopeClauseFromSyntax(syntax.Root())
	if scope.names == nil {
		return []string{"*"}, nil
	}
	return scope.names, nil
}

func importingWithVariables(ctx context.Context, source string) []string {
	body := readQueryBody(ctx, source, 0, len(source))
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
	plan := projectionPlanFromSyntax(node)
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

// Legacy Fabric syntax adapter, retained until #1583 replaces its planner.
func callSubqueryImportedVariables(ctx context.Context, scope string, scoped bool, body string) ([]string, error) {
	if scoped {
		return callScopeClauseVariables(ctx, scope)
	}
	return importingWithVariables(ctx, body), ctx.Err()
}
