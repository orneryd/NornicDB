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
// calls one of NornicDB's internal functions (__nornic_interpolate, …: every
// __nornic_* function the function classification knows, from the catalog
// or the function registry), in any letter case, with Neo4j's "Unknown
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
		if _, internal := functionPrivilegeClass(normalized); internal && strings.HasPrefix(normalized, "__nornic_") {
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
	return forEachSemanticCall(text, func(name, normalized string, _ int, procedure bool) error {
		if procedure {
			return nil
		}
		return visit(name, normalized)
	})
}

// forEachSemanticCall is the token-aware owner for function and procedure
// detection. arguments is the position after the opening parenthesis (or the
// end of a procedure name when its optional parentheses are absent).
func forEachSemanticCall(text string, visit func(name, normalized string, arguments int, procedure bool) error) error {
	expression := text
	for index := 0; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' {
			index = skipQuotedSemanticText(expression, index)
			continue
		}
		if end := queryCommentEnd(expression, index); end >= 0 {
			index = end
			continue
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		procedure := expression[index] != '`' && strings.EqualFold(name, "CALL")
		if procedure || expression[index] != '`' && strings.EqualFold(name, "USE") {
			start := queryGapEnd(expression, next)
			called, end, named := scanIdentifierToken(expression, start)
			if !named {
				index = next
				continue
			}
			name, next = called, end
			if !procedure {
				_, index = dottedNameParts(expression, next, true)
				continue
			}
		}
		parts, cursor := dottedNameParts(expression, next, true)
		name += parts
		cursor = queryGapEnd(expression, cursor)
		parenthesis := cursor < len(expression) && expression[cursor] == '('
		if !parenthesis && !procedure {
			index = next
			continue
		}
		index = cursor
		if parenthesis {
			index++
		}
		normalized := lowerASCII(normalizeProjectionColumnName(name))
		// Clause and predicate keywords can legally be followed by a parenthesized
		// pattern/expression (MATCH (n), STARTS WITH (...)). They are syntax, not
		// function invocations.
		if !procedure && isNonFunctionSemanticKeyword(normalized) {
			continue
		}
		if err := visit(name, normalized, index, procedure); err != nil {
			return err
		}
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

// dottedNameParts returns the dotted parts (.b.c) following a name that ends
// at end, and where they end. Each name is read as the code that runs it
// reads it: whitespace may stand around a dot only when spaced is set, for a
// procedure, function or graph name. Contiguous unquoted components reuse
// the source slice; only names containing trivia or quotes are normalized.
func dottedNameParts(expression string, end int, spaced bool) (string, int) {
	skip := func(index int) int {
		if spaced && index < len(expression) && (isASCIIWhitespace(expression[index]) || expression[index] == '/' || expression[index] >= 0x80) {
			return queryGapEnd(expression, index)
		}
		return index
	}
	origin := end
	var normalized strings.Builder
	normalizing := false
	for {
		dot := skip(end)
		if dot >= len(expression) || expression[dot] != '.' {
			if normalizing {
				return normalized.String(), end
			}
			return expression[origin:end], end
		}
		partStart := skip(dot + 1)
		part, partEnd, ok := scanIdentifierToken(expression, partStart)
		if !ok {
			if normalizing {
				return normalized.String(), end
			}
			return expression[origin:end], end
		}
		if !normalizing && (dot != end || partStart != dot+1 || expression[partStart] == '`') {
			normalized.WriteString(expression[origin:end])
			normalizing = true
		}
		if normalizing {
			normalized.WriteByte('.')
			normalized.WriteString(part)
		}
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
