package cypher

import (
	"strings"

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
func callSubqueryImportedVariables(scope string, scoped bool, body string) ([]string, error) {
	if scoped {
		return callScopeClauseVariables(scope)
	}
	return importingWithVariables(body), nil
}

// callScopeClauseVariables is the list of a CALL scope clause, the text
// between its parentheses: "*" alone, or each variable's name, unquoted. Any
// other item (CALL (1), CALL (x + 1), CALL (x, *)), or an empty one (CALL
// (x,), CALL (,x), CALL (x,,y)), is Neo4j's syntax error for a scope clause,
// the one statement validation reports too. A word such as null or true is
// a variable's name here, as in Neo4j, and is an error only where no such
// variable is defined.
func callScopeClauseVariables(list string) ([]string, error) {
	list = strings.TrimSpace(StripComments(list))
	names := []string{}
	if list == "" {
		return names, nil
	}
	if list == "*" {
		return []string{"*"}, nil
	}
	for _, item := range splitTopLevelCommaKeepEmpty(list) {
		name, ok := isOneSymbolicName(item)
		if !ok {
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
				localization.CypherResultMetadataImportVariablesRequired())
		}
		names = append(names, name)
	}
	return names, nil
}

// importingWithVariables is the variables a CALL body's leading WITH
// imports: its items that are * or a plain variable reference, kept under
// their own name. Any other item (WITH $k AS k, WITH x AS y) is a
// projection inside the body.
func importingWithVariables(body string) []string {
	body = strings.TrimSpace(StripComments(body))
	if !startsWithKeywordFold(body, "WITH") {
		return nil
	}
	clause, _ := leadingWithClause(body)
	projection, _ := projectionSemanticBodyAndTail(clause, "WITH")
	var names []string
	for _, item := range appendTopLevelComma(nil, projection) {
		expression, alias := parseProjectionExprAlias(item)
		if strings.TrimSpace(expression) == "*" {
			names = append(names, "*")
			continue
		}
		if name, ok := isOneSymbolicName(expression); ok && name == alias {
			names = append(names, name)
		}
	}
	return names
}
