package cypher

import (
	"fmt"
	"strings"
)

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
	inner := s[1 : len(s)-1]
	if strings.Contains(inner, "`") {
		// Internal parser detail: every caller wraps this cause in an admin or
		// composite localized error before returning it from query execution.
		return "", fmt.Errorf("invalid identifier %q: nested backticks are not supported", raw)
	}
	return inner, nil
}
