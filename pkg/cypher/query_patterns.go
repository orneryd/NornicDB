package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// functionCallParenAt reports whether the '(' at text[open] opens a function
// call's arguments (size(kinds), n.f(x)) rather than a node pattern: it
// directly follows a name that isn't a clause keyword (MATCH(n), WHERE(...)
// open patterns and expressions).
func functionCallParenAt(text string, open int) bool {
	end := open
	start := end
	for start > 0 && cyphertext.NamePartAt(text, start-1) {
		start--
	}
	if start == end {
		return false
	}
	switch upperASCII(text[start:end]) {
	case "MATCH", "CREATE", "MERGE", "WHERE", "AND", "OR", "XOR", "NOT", "WITH", "RETURN", "UNWIND", "IN", "DELETE", "SET", "REMOVE", "FOREACH", "YIELD":
		return false
	}
	return true
}

// extractNodeVariables extracts node variable names from a MATCH pattern,
// plain or backtick-quoted (`n n`, as written). A parenthesis that opens a
// function call's arguments or an expression starting with a subquery
// (EXISTS { … } AND …) is not a node.
func extractNodeVariables(matchClause string) []string {
	var vars []string
	for i := 0; i < len(matchClause); i++ {
		if isCypherStringQuoteByte(matchClause[i]) {
			i = skipQuotedSemanticText(matchClause, i) - 1
			continue
		}
		if matchClause[i] == '{' {
			if end := cyphertext.FindMatchingDelimiter(matchClause, i, '{', '}', cyphertext.OpeningDelimiter); end >= 0 {
				i = end
				continue
			}
		}
		if dynamicLabelStartsAt(matchClause, i) {
			// $(e): a dynamic label's expression, not a node.
			if end := cyphertext.FindMatchingDelimiter(matchClause, i+strings.IndexByte(matchClause[i:], '('), '(', ')', cyphertext.OpeningDelimiter); end >= 0 {
				i = end
				continue
			}
		}
		if matchClause[i] != '(' || functionCallParenAt(matchClause, i) {
			continue
		}
		j := i + 1
		for j < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, j) {
			j++
		}
		if _, _, subquery := subqueryExpressionKeywordAt(matchClause, j); subquery {
			continue
		}
		name, next, ok := scanSymbolicName(matchClause, j)
		if !ok {
			continue
		}
		if next < len(matchClause) {
			for next < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, next) {
				next++
			}
			if next < len(matchClause) && matchClause[next] != ':' && matchClause[next] != ')' && matchClause[next] != '{' && matchClause[next] != '$' {
				continue
			}
		}
		vars = append(vars, name)
	}
	return vars
}
func extractMatchClause(ctx context.Context, query string) string {
	matchIdx := findKeywordIndex(ctx, query, "MATCH")
	if matchIdx < 0 {
		return ""
	}
	end := len(query)
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "ORDER", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(ctx, query[matchIdx:], keyword); idx > 0 {
			if matchIdx+idx < end {
				end = matchIdx + idx
			}
		}
	}
	return query[matchIdx:end]
}
