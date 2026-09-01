package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// extractCallArguments parses top-level CALL arguments as interface{} literals.
// It supports numbers, booleans, null, quoted strings, and leaves complex values as raw strings.
func extractCallArguments(cypher string) ([]interface{}, error) {
	open := strings.Index(cypher, "(")
	if open == -1 {
		return nil, nil
	}
	close := findMatchingCallParen(cypher, open)
	if close == -1 || close < open {
		return nil, nil
	}

	body := strings.TrimSpace(cypher[open+1 : close])
	if body == "" {
		return []interface{}{}, nil
	}

	parts := splitProcedureTopLevelComma(body)
	args := make([]interface{}, 0, len(parts))
	for _, p := range parts {
		v, err := parseProcedureArgLiteral(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	return args, nil
}

func findMatchingCallParen(s string, open int) int {
	if open < 0 || open >= len(s) || s[open] != '(' {
		return -1
	}
	depth := 1
	inQuote := false
	quoteChar := rune(0)
	escaped := false

	for i, ch := range s[open+1:] {
		if inQuote {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quoteChar {
				inQuote = false
				quoteChar = 0
			}
			continue
		}

		switch ch {
		case '\'', '"':
			inQuote = true
			quoteChar = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return open + 1 + i
			}
		}
	}
	return -1
}

func splitProcedureTopLevelComma(s string) []string {
	var out []string
	var cur strings.Builder
	paren := 0
	bracket := 0
	brace := 0
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range s {
		if inQuote {
			cur.WriteRune(ch)
			if ch == quoteChar && (i == 0 || s[i-1] != '\\') {
				inQuote = false
				quoteChar = 0
			}
			continue
		}

		switch ch {
		case '\'', '"':
			inQuote = true
			quoteChar = ch
			cur.WriteRune(ch)
		case '(':
			paren++
			cur.WriteRune(ch)
		case ')':
			paren--
			cur.WriteRune(ch)
		case '[':
			bracket++
			cur.WriteRune(ch)
		case ']':
			bracket--
			cur.WriteRune(ch)
		case '{':
			brace++
			cur.WriteRune(ch)
		case '}':
			brace--
			cur.WriteRune(ch)
		case ',':
			if paren == 0 && bracket == 0 && brace == 0 {
				out = append(out, cur.String())
				cur.Reset()
			} else {
				cur.WriteRune(ch)
			}
		default:
			cur.WriteRune(ch)
		}
	}

	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func parseProcedureArgLiteral(s string) (interface{}, error) {
	if s == "" {
		return "", nil
	}
	lower := strings.ToLower(s)
	switch lower {
	case "null":
		return nil, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}

	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
		return s[1 : len(s)-1], nil
	}
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") && len(s) >= 2 {
		return s[1 : len(s)-1], nil
	}

	// Keep placeholders and complex structures as raw strings for procedure handlers.
	if strings.HasPrefix(s, "$") ||
		strings.HasPrefix(s, "[") ||
		strings.HasPrefix(s, "{") ||
		strings.Contains(s, ".") && !looksNumeric(s) {
		return s, nil
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	return s, nil
}

func looksNumeric(s string) bool {
	_, errI := strconv.ParseInt(s, 10, 64)
	if errI == nil {
		return true
	}
	_, errF := strconv.ParseFloat(s, 64)
	return errF == nil
}

func validateYieldColumnsExist(columns []string, yield *yieldClause) error {
	if yield == nil || yield.yieldAll || len(yield.items) == 0 {
		return nil
	}
	available := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		available[c] = struct{}{}
	}
	for _, item := range yield.items {
		if _, ok := available[item.name]; !ok {
			return localizedError(localization.CypherCommandRoutingUnknownYieldColumn(item.name), nil)
		}
	}
	return nil
}
