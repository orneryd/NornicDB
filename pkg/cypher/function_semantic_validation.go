package cypher

import (
	"fmt"
	"strings"
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
	if err := forEachFunctionCall(expression, func(name, normalized string) error {
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
	return gdsProjectDistinctError(expression)
}

// gdsProjectDistinctError refuses DISTINCT in gds.graph.project(...), as
// Neo4j refuses it for that aggregating function.
func gdsProjectDistinctError(text string) error {
	const name = "gds.graph.project"
	for from := 0; ; {
		at := indexASCIIFold(text[from:], name)
		if at < 0 {
			return nil
		}
		rest := strings.TrimLeft(text[from+at+len(name):], " \t\r\n")
		if strings.HasPrefix(rest, "(") && startsWithKeywordFold(strings.TrimLeft(rest[1:], " \t\r\n"), "DISTINCT") {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidUseOfDistinct", "Invalid use of DISTINCT with function 'project'")
		}
		from += at + len(name)
	}
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
		if expression[index] != '`' && (strings.EqualFold(name, "CALL") || strings.EqualFold(name, "USE")) {
			index = skipCalledName(expression, next)
			continue
		}
		parts, cursor := dottedNameParts(expression, next, true)
		name += parts
		for cursor < len(expression) && isASCIIWhitespace(expression[cursor]) {
			cursor++
		}
		if cursor >= len(expression) || expression[cursor] != '(' {
			index = next
			continue
		}
		index = cursor + 1
		normalized := lowerASCII(normalizeProjectionColumnName(name))
		// Clause and predicate keywords can legally be followed by a parenthesized
		// pattern/expression (MATCH (n), STARTS WITH (...)). They are syntax, not
		// function invocations.
		if isNonFunctionSemanticKeyword(normalized) {
			continue
		}
		if err := visit(name, normalized); err != nil {
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

// skipCalledName returns where the name after CALL or USE ends: a procedure
// or graph name, dotted, which is not a function call. Anything else after
// the keyword (a subquery, its imports) is scanned on.
func skipCalledName(expression string, start int) int {
	_, end, ok := scanIdentifierToken(expression, skipSpaces(expression, start))
	if !ok {
		return start
	}
	_, end = dottedNameParts(expression, end, true)
	return end
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
