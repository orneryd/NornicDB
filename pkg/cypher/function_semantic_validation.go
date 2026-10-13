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
	for index := 0; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		if expression[index] != '`' && (strings.EqualFold(name, "CALL") || strings.EqualFold(name, "USE")) {
			index = skipCalledName(expression, next)
			continue
		}
		parts, cursor := dottedNameParts(expression, next)
		name += parts
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' {
			index = next
			continue
		}
		normalized := lowerASCII(normalizeProjectionColumnName(name))
		// Clause and predicate keywords can legally be followed by a parenthesized
		// pattern/expression (MATCH (n), STARTS WITH (...)). They are syntax, not
		// function invocations, and must stay in the converged expression scanner.
		if isNonFunctionSemanticKeyword(normalized) {
			index = cursor + 1
			continue
		}
		if _, known := builtInCypherFunctions[normalized]; known {
			index = cursor + 1
			continue
		}
		switch normalized {
		case "graph.byname", "graph.byelementid":
			// A graph reference, not a value (Neo4j 5.26).
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCommandRoutingGraphFunctionOnlyInUse(name))
		case "graph.names", "graph.propertiesbyname":
			// Known on a composite database; elsewhere the call reports
			// Neo4j's "Unknown function" (fn.UnknownFunctionError).
			index = cursor + 1
			continue
		}
		if strings.Contains(normalized, ".") && PluginFunctionLookup != nil {
			if _, found := PluginFunctionLookup(normalized); found {
				index = cursor + 1
				continue
			}
		}
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"UnknownFunction",
			fmt.Sprintf("unknown function: %s", name),
		)
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

// skipCalledName returns where the name after CALL or USE ends: a procedure
// or graph name, dotted, which is not a function call. Anything else after
// the keyword (a subquery, its imports) is scanned on.
func skipCalledName(expression string, start int) int {
	_, end, ok := scanIdentifierToken(expression, skipSpaces(expression, start))
	if !ok {
		return start
	}
	_, end = dottedNameParts(expression, end)
	return end
}

// dottedNameParts returns the dotted parts (.b.c) following a name that ends
// at end, and where they end. Whitespace may stand around a dot, as in
// Neo4j (date . truncate(…), USE graph . byName(…)).
func dottedNameParts(expression string, end int) (string, int) {
	parts := ""
	for {
		dot := skipSpaces(expression, end)
		if dot >= len(expression) || expression[dot] != '.' {
			return parts, end
		}
		part, partEnd, ok := scanIdentifierToken(expression, skipSpaces(expression, dot+1))
		if !ok {
			return parts, end
		}
		parts += "." + part
		end = partEnd
	}
}

func skipQuotedSemanticText(expression string, start int) int {
	quote := expression[start]
	for index := start + 1; index < len(expression); index++ {
		if expression[index] == quote && !isBackslashEscaped(expression, index) {
			return index + 1
		}
	}
	return len(expression)
}
