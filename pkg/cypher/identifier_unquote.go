package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// scanSymbolicName reads the Cypher symbolic name (variable, alias, property
// key) that starts at s[start]: a plain identifier, or a backtick-quoted one in
// which a doubled backtick stands for a backtick (`the g`, `a“b`). It returns
// the name as written (quotes included; normalizeProjectionColumnName removes
// them), the offset after it, and whether a name starts there.
func scanSymbolicName(s string, start int) (string, int, bool) {
	end, ok := cyphertext.ScanIdentifier(s, start)
	if !ok {
		return "", end.End, false
	}
	return s[start:end.End], end.End, true
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
// The canonical identifier scanner owns quoted closure and doubled-backtick
// decoding. Database-name admission remains with each command consumer.
func unquoteBacktickIdentifier(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if len(s) < 2 || !isCypherBacktickByte(s[0]) || !isCypherBacktickByte(s[len(s)-1]) {
		return s, nil
	}
	name, ok := cyphertext.ScanIdentifier(s, 0)
	if !ok || !name.Quoted || name.End != len(s) {
		// Internal parser detail: every caller wraps this cause in an admin or
		// composite localized error before returning it from query execution.
		return "", fmt.Errorf("invalid quoted identifier %q", raw)
	}
	return name.Value, nil
}
