package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// extractCallArguments parses top-level CALL arguments as interface{} literals.
// It supports numbers, booleans, null, quoted strings, and leaves complex values as raw strings.
func extractCallArguments(cypher string) ([]interface{}, error) {
	var call functionCallSpan
	found := false
	for _, candidate := range readFunctionSyntax(cypher).calls {
		if candidate.procedure {
			call = candidate
			found = true
			break
		}
	}
	if !found || call.open < 0 || call.close(cypher) < 0 {
		return nil, nil
	}
	parts := call.arguments(nil)
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
	for _, group := range readFunctionSyntax(s).groups {
		if group.open == open {
			return group.close
		}
	}
	return -1
}

func splitProcedureTopLevelComma(s string) []string {
	return readFunctionSyntax(s).appendArguments(nil, 0, false)
}

func parseProcedureArgLiteral(s string) (interface{}, error) {
	if s == "" {
		return "", nil
	}
	lower := lowerASCII(s)
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
