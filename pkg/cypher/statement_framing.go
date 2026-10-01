package cypher

import (
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func (e *StorageExecutor) validateStatementFraming(cypher string) error {
	if containsOutsideStrings(cypher, ";") {
		statements := e.splitBySemicolon(cypher)
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCommandRoutingMultipleStatements(len(statements)))
	}
	if explainProfileConflict(cypher) {
		return nornicerrors.MarkCompileTime(newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
			"Can't specify multiple conflicting values for execution mode"))
	}
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		hasFinish, hasColumns := false, false
		for _, branch := range branches {
			_, finishes := stripTrailingFinish(branch)
			if finishes {
				hasFinish = true
			} else if len(e.StatementColumns(branch)) > 0 {
				hasColumns = true
			}
		}
		if hasFinish && hasColumns {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidProjection",
				"All sub queries in an UNION must have the same return column names")
		}
	}
	return nil
}

// Statement framing helpers: Neo4j 5 statement preamble (CYPHER … groups),
// the FINISH clause terminator, and the EXPLAIN/PROFILE exclusivity rule.

// cypherClauseStarts are the keywords that end a CYPHER preamble group and
// start the actual statement. Matched case-insensitively at a token boundary.
var cypherClauseStarts = [...]string{
	"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH",
	"UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN",
	"PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK", "TERMINATE",
	"UNION", "CYPHER", "FINISH",
}

func startsWithClauseKeyword(s string) bool {
	for _, keyword := range cypherClauseStarts {
		if matchKeywordAt(s, 0, keyword) {
			return true
		}
	}
	return false
}

// stripCypherPreamble removes leading CYPHER groups: the keyword CYPHER, an
// optional version number, and option tokens (ident, ident=value, or a quoted
// string) until a clause keyword starts the statement. Neo4j accepts any
// number of such groups (`CYPHER 5 runtime=slotted CYPHER RETURN 1`).
func stripCypherPreamble(query string) (string, bool) {
	rest := query
	changed := false
	for {
		trimmed := strings.TrimSpace(rest)
		if !matchKeywordAt(trimmed, 0, "CYPHER") {
			break
		}
		changed = true
		rest = strings.TrimSpace(trimmed[len("CYPHER"):])
		// Optional version number (e.g. CYPHER 5, CYPHER 25).
		if len(rest) > 0 && rest[0] >= '0' && rest[0] <= '9' {
			i := 0
			for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
				i++
			}
			rest = strings.TrimSpace(rest[i:])
		}
		// Option tokens until the next clause keyword starts the statement.
		for rest != "" && !startsWithClauseKeyword(rest) {
			if rest[0] == '\'' || rest[0] == '"' {
				end := skipCypherQuotedText(rest, 0, rest[0])
				rest = strings.TrimSpace(rest[end:])
				continue
			}
			i := 0
			for i < len(rest) && !isASCIISpace(rest[i]) {
				i++
			}
			rest = strings.TrimSpace(rest[i:])
		}
		if rest == "" {
			break
		}
	}
	if changed {
		return strings.TrimSpace(rest), true
	}
	return query, false
}

// finishProhibitedClauses are the clause keywords a statement may end with
// before FINISH. A trailing FINISH after a projection clause (RETURN, WITH,
// YIELD) is invalid in Neo4j and stays in the text so validation rejects it.
var finishProhibitedClauses = map[string]bool{
	"RETURN": true,
	"WITH":   true,
	"YIELD":  true,
}

// trailingBareFinish reports whether the statement ends with a standalone
// FINISH keyword outside quotes and comments.
func trailingBareFinish(cypher string) (string, bool) {
	last := lastLiveByte(cypher, len(cypher))
	if last < 0 || !isIdentByte(cypher[last]) {
		return "", false
	}
	end := last + 1
	start := end
	for start > 0 && isIdentByte(cypher[start-1]) {
		start--
	}
	if !strings.EqualFold(cypher[start:end], "FINISH") {
		return "", false
	}
	return strings.TrimSpace(cypher[:start]), true
}

// stripTrailingFinish removes a trailing FINISH clause terminator (Neo4j 5.19+:
// FINISH ends a query without returning rows). FINISH counts only when the
// preceding top-level clause is a reading/writing clause — a trailing FINISH
// after RETURN/WITH/YIELD stays in the text so validation rejects it. The
// preceding clause comes from the shared name-aware scanner
// lastTopLevelClauseWord (validator_strictness.go): one clause scanner for
// both the FINISH position and the dangling-UNWIND rule.
func stripTrailingFinish(cypher string) (string, bool) {
	remainder, ok := trailingBareFinish(cypher)
	if !ok {
		return cypher, false
	}
	if finishProhibitedClauses[lastTopLevelClauseWord(remainder)] {
		return cypher, false
	}
	return remainder, true
}

// topLevelUnionCut returns the offset of the first top-level UNION keyword
// (outside quotes, comments and nested delimiters), or -1.
func topLevelUnionCut(s string) int {
	inQuote := byte(0)
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote != 0 {
			if c == '\\' && inQuote != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inQuote {
				if i+1 < len(s) && s[i+1] == inQuote {
					i++
					continue
				}
				inQuote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQuote = c
			continue
		case '/':
			if i+1 < len(s) && s[i+1] == '/' {
				for i < len(s) && s[i] != '\n' && s[i] != '\r' {
					i++
				}
				continue
			}
			if i+1 < len(s) && s[i+1] == '*' {
				end := strings.Index(s[i+2:], "*/")
				if end < 0 {
					return -1
				}
				i += 2 + end + 1
				continue
			}
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		}
		if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 &&
			matchKeywordAt(s, i, "UNION") {
			return i
		}
	}
	return -1
}

// stripUnionBranchFinishes strips a trailing FINISH from each UNION branch of
// the statement. If no branch ends in FINISH the text is unchanged.
func stripUnionBranchFinishes(cypher string) (string, bool) {
	cut := topLevelUnionCut(cypher)
	if cut < 0 {
		return stripTrailingFinish(cypher)
	}
	before := strings.TrimSpace(cypher[:cut])
	after := strings.TrimSpace(cypher[cut:])
	// Consume UNION and an optional ALL.
	wordEnd := 0
	for wordEnd < len(after) && !isASCIISpace(after[wordEnd]) {
		wordEnd++
	}
	unionWord := after[:wordEnd]
	tail := strings.TrimSpace(after[wordEnd:])
	if nextWord, ok := firstWordUpper(tail); ok && nextWord == "ALL" {
		unionWord += " ALL"
		tail = strings.TrimSpace(tail[len("ALL"):])
	}
	right, rightStripped := stripUnionBranchFinishes(tail)
	left, leftStripped := stripTrailingFinish(before)
	if !leftStripped && !rightStripped {
		return cypher, false
	}
	return strings.TrimSpace(left + " " + unionWord + " " + right), true
}

func firstWordUpper(s string) (string, bool) {
	i := 0
	for i < len(s) && !isASCIISpace(s[i]) {
		i++
	}
	if i == 0 {
		return "", false
	}
	return upperASCII(s[:i]), true
}

// explainProfileConflict reports whether the statement starts with both
// EXPLAIN and PROFILE (either order), which Neo4j rejects.
func explainProfileConflict(cypher string) bool {
	query := strings.TrimSpace(cypher)
	first := ""
	for {
		query = strings.TrimSpace(query[queryGapEnd(query, 0):])
		query, _ = stripCypherPreamble(query)
		mode := ""
		if matchKeywordAt(query, 0, "EXPLAIN") {
			mode = "EXPLAIN"
		} else if matchKeywordAt(query, 0, "PROFILE") {
			mode = "PROFILE"
		} else {
			return false
		}
		if first != "" && first != mode {
			return true
		}
		first = mode
		query = strings.TrimSpace(query[len(mode):])
	}
}
