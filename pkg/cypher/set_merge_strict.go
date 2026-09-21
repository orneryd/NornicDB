package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// parseSetMergeMapLiteralStrict parses an inline map literal used by SET +=.
// Unlike permissive property parsing helpers, this enforces Cypher semantics:
// malformed maps must return an error instead of becoming an empty map/no-op.
func (e *StorageExecutor) parseSetMergeMapLiteralStrict(ctx context.Context, s string) (map[string]interface{}, error) {
	expressions, err := parseSetMergeMapExpressionsStrict(s)
	if err != nil {
		return nil, err
	}

	props := make(map[string]interface{}, len(expressions))
	for key, value := range expressions {
		props[key] = e.parseValue(ctx, value)
	}
	return props, nil
}

// parseSetMergeMapExpressionsStrict validates an inline SET += map while
// preserving each value expression for evaluation against an individual row.
func parseSetMergeMapExpressionsStrict(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, localizedError(localization.CypherMergeMapLiteralEnclosureRequired(), nil)
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]string{}, nil
	}

	expressions := make(map[string]string)
	pairs := splitTopLevelCommaKeepEmpty(inner)
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return nil, localizedError(localization.CypherMergeMapEntryEmpty(), nil)
		}

		colonIdx := findTopLevelMapKeyValueSeparator(pair)
		if colonIdx <= 0 || colonIdx == len(pair)-1 {
			return nil, localizedError(localization.CypherMergeMapEntryInvalid(pair), nil)
		}

		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		if key == "" {
			return nil, localizedError(localization.CypherMergeMapKeyEmpty(), nil)
		}
		value := strings.TrimSpace(pair[colonIdx+1:])
		if value == "" {
			return nil, localizedError(localization.CypherMergeMapValueEmpty(key), nil)
		}

		expressions[key] = value
	}

	return expressions, nil
}

// splitTopLevelCommaKeepEmpty is like splitTopLevelComma but preserves empty
// entries so strict callers can reject malformed forms such as trailing commas.
func splitTopLevelCommaKeepEmpty(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}

	var parts []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	depth := 0

	for i, r := range input {
		switch r {
		case '\'':
			if !inDouble && (i == 0 || input[i-1] != '\\') {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && (i == 0 || input[i-1] != '\\') {
				inDouble = !inDouble
			}
		case '(', '[', '{':
			if !inSingle && !inDouble {
				depth++
			}
		case ')', ']', '}':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		case ',':
			if !inSingle && !inDouble && depth == 0 {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}

	parts = append(parts, strings.TrimSpace(current.String()))
	return parts
}
