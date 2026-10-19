package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateKnownFunctions is the unknown-function check: every function a
// statement calls, in any clause or subquery, is a built-in or a loaded
// plugin's function, as in Neo4j, whether or not a database was opened
// (#1166). A procedure a CALL names and the graph a USE names are not
// function calls; schema and administration commands declare rules, not
// calls. A SHOW command's YIELD / WHERE / RETURN tail is checked, as in
// Neo4j.
func validateKnownFunctions(expression string) error {
	if !startsWithKeywordFold(strings.TrimSpace(expression), "SHOW") &&
		(isSchemaCommandStatement(expression) || isCreateSchemaOrAdministrationCommand(expression)) {
		return nil
	}
	var distinctError error
	if err := forEachSemanticCall(expression, func(name, normalized string, arguments int, procedure bool) error {
		if procedure {
			return nil
		}
		if distinctError == nil {
			distinctError = gdsProjectDistinctCallError(expression, normalized, arguments)
		}
		if _, known := builtInCypherFunctions[normalized]; known {
			if nativeAPOCCaseMismatch(name) {
				if _, found := pluginFunctionHandler(name); !found {
					return unknownFunctionError(name)
				}
			}
			return nil
		}
		// graph.byName / graph.byElementId are catalog names; outside USE
		// they fail where they are evaluated. graph.names and
		// graph.propertiesByName are known on a composite database;
		// elsewhere the call reports Neo4j's "Unknown function"
		// (fn.UnknownFunctionError).
		if normalized == "graph.names" || normalized == "graph.propertiesbyname" {
			return nil
		}
		if _, found := pluginFunctionHandler(name); found {
			return nil
		}
		return unknownFunctionError(name)
	}); err != nil {
		return err
	}
	return distinctError
}

// gdsProjectDistinctError refuses DISTINCT in gds.graph.project(...), as
// Neo4j refuses it for that aggregating function.
func gdsProjectDistinctError(text string) error {
	return forEachSemanticCall(text, func(_, normalized string, arguments int, procedure bool) error {
		if !procedure {
			return gdsProjectDistinctCallError(text, normalized, arguments)
		}
		return nil
	})
}

func gdsProjectDistinctCallError(text, normalized string, arguments int) error {
	if normalized == "gds.graph.project" && startsWithKeywordFold(text[queryGapEnd(text, arguments):], "DISTINCT") {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidUseOfDistinct", localization.CypherGraphProceduresProjectDistinct())
	}
	return nil
}

// internalFunctionCallError rejects a statement, as its client sent it, that
// calls one of NornicDB's internal functions (__nornic_interpolate, …, the
// catalog's __nornic_* names), in any letter case, with Neo4j's "Unknown
// function" (#1105). NornicDB's own rewrites produce these calls from
// standard syntax (string interpolation, map comprehension, vector(…),
// dynamic labels, path selectors), so the check reads the text before any
// rewrite: Execute's statement and the statement text a procedure runs
// (executeUserStatement).
func internalFunctionCallError(text string) error {
	if !containsInternalFunctionPrefix(text) {
		return nil
	}
	return forEachFunctionCall(text, func(name, normalized string) error {
		if _, internal := builtInCypherFunctions[normalized]; internal && strings.HasPrefix(normalized, "__nornic_") {
			return unknownFunctionError(name)
		}
		return nil
	})
}

// containsInternalFunctionPrefix reports whether text holds __nornic_ in any
// letter case: most statements don't, and skip the scan.
func containsInternalFunctionPrefix(text string) bool {
	const prefix = "__nornic_"
	for i := 0; i+len(prefix) <= len(text); i++ {
		if text[i] == '_' && text[i+1] == '_' && strings.EqualFold(text[i:i+len(prefix)], prefix) {
			return true
		}
	}
	return false
}

func unknownFunctionError(name string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnknownFunction",
		fmt.Sprintf("unknown function: %s", name),
	)
}

// forEachFunctionCall calls visit with each function call in text, in order:
// its name as written (dotted parts joined) and lower-cased, without
// backticks. Strings and comments, a procedure a CALL names, the graph a USE
// names and the keywords that can stand before a parenthesis are not calls.
func forEachFunctionCall(text string, visit func(name, normalized string) error) error {
	syntax:=readFunctionSyntax(text)
	for index := 0; index < len(text); {
		call, ok := syntax.nextCall(index,true,false,false)
		if !ok {
			break
		}
		if err := visit(call.name, lowerASCII(call.name)); err != nil {
			return err
		}
		index = call.open + 1
	}
	return nil
}
func isNonFunctionSemanticKeyword(name string) bool {
	switch name {
	case "and", "by", "call", "case", "contains", "create", "delete", "detach", "distinct", "do", "each", "elements", "else", "end", "filter", "for", "foreach", "group", "groups", "in", "insert", "is", "limit", "match", "merge", "not", "of", "offset", "on", "optional", "or", "path", "paths", "remove", "require", "return", "set", "shortest", "skip", "starts", "then", "union", "unwind", "when", "where", "with", "xor", "yield":
		return true
	default:
		return false
	}
}

// forEachSemanticCall retains the procedure dependency inventory, using the
// same lexical spans as runtime and static function consumers.
func forEachSemanticCall(text string, visit func(name, normalized string, arguments int, procedure bool) error) error {
	for index := 0; index < len(text); {
		call, ok := nextCallSpan(text, index, true, true)
		if !ok {
			break
		}
		if err := visit(call.name, lowerASCII(call.name), call.next, call.procedure); err != nil {
			return err
		}
		index = call.next
	}
	return nil
}

func skipQuotedSemanticText(expression string, start int) int {
	return skipCypherQuotedText(expression, start, expression[start])
}
