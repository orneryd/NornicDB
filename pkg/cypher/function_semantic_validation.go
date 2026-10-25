package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
		(isSchemaCommandStatement(expression) || isMetadataCommand(expression, metadataCreateAdmission)) {
		return nil
	}
	return forEachFunctionCall(expression, knownFunctionCallError)
}

// knownFunctionCallError is the signature owner for both retained grammar
// invocations and legacy whole-statement scanning.
func knownFunctionCallError(name, normalized string) error {
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
	return forEachFunctionInvocation(text, func(name, normalized string, _ int) error { return visit(name, normalized) })
}

// forEachFunctionInvocation visits the function calls of text, read by the
// grammar (syntaxFunctionCalls), with the offset of each call's "(".
func forEachFunctionInvocation(text string, visit func(name, normalized string, open int) error) error {
	return textFunctionCalls(context.Background(), text, func(call syntaxFunctionCall) error {
		return visit(call.name, call.normalized, call.open)
	})
}

// dottedNameParts returns the dotted parts (.b.c) following a name that ends
// at end, and where they end. Each name is read as the code that runs it
// reads it: whitespace may stand around a dot only when spaced is set, for a
// procedure, function or graph name. Contiguous unquoted components reuse
// the source slice; only names containing trivia or quotes are normalized.
func dottedNameParts(expression string, end int, spaced bool) (string, int) {
	skip := func(index int) int {
		if spaced && index < len(expression) && (cyphertext.IsWhitespaceAt(expression, index) || expression[index] == '/' || expression[index] >= 0x80) {
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
		part, ok := cyphertext.ScanIdentifier(expression, partStart)
		if !ok {
			if normalizing {
				return normalized.String(), end
			}
			return expression[origin:end], end
		}
		if !normalizing && (dot != end || partStart != dot+1 || isCypherBacktickByte(expression[partStart])) {
			normalized.WriteString(expression[origin:end])
			normalizing = true
		}
		if normalizing {
			normalized.WriteByte('.')
			normalized.WriteString(part.Value)
		}
		end = part.End
	}
}

func skipQuotedSemanticText(expression string, start int) int {
	return skipCypherQuotedText(expression, start, expression[start])
}
