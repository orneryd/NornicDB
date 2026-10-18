package cypher

import (
	"fmt"
	"strings"
)

// scanSymbolicName reads the Cypher symbolic name (variable, alias, property
// key) that starts at s[start]: a plain identifier, or a backtick-quoted one in
// which a doubled backtick stands for a backtick (`the g`, `a“b`). It returns
// the name as written (quotes included; normalizeProjectionColumnName removes
// them), the offset after it, and whether a name starts there.
func scanSymbolicName(s string, start int) (string, int, bool) {
	_, end, ok := scanIdentifierToken(s, start)
	if !ok {
		return "", end, false
	}
	return s[start:end], end, true
}

// isPatternVariableName reports whether name is exactly one symbolic name,
// plain or backtick-quoted (`p p`): a pattern or path variable as written.
func isPatternVariableName(name string) bool {
	_, end, ok := scanSymbolicName(name, 0)
	return ok && end == len(name)
}

// unquoteBacktickIdentifier removes surrounding backticks from a Cypher identifier.
//
// Neo4j/Cypher uses backticks for escaping identifiers. For system commands like
// CREATE/DROP DATABASE, users (and UIs) commonly send backtick-quoted names:
//   - DROP DATABASE `bench_col`
//
// We accept this form and normalize it to the raw name ("bench_col").
//
// This helper intentionally does not attempt to fully implement Cypher's escaping rules
// for arbitrary identifiers. NornicDB database names should remain simple and are
// validated elsewhere.
func unquoteBacktickIdentifier(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if len(s) < 2 || s[0] != '`' || s[len(s)-1] != '`' {
		return s, nil
	}
	written, end, ok := scanSymbolicName(s, 0)
	inner := symbolicNameValue(written)
	if !ok || end != len(s) || strings.Contains(inner, "`") {
		// Internal parser detail: every caller wraps this cause in an admin or
		// composite localized error before returning it from query execution.
		return "", fmt.Errorf("invalid identifier %q: nested backticks are not supported", raw)
	}
	return inner, nil
}
