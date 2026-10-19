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
	expressions := make(map[string]string)
	if err := forEachStrictMapEntry(s, func(key, value string) {
		expressions[key] = value
	}); err != nil {
		return nil, err
	}
	return expressions, nil
}

// validateMapLiteralSyntax checks a map literal with the strict grammar of
// parseSetMergeMapExpressionsStrict without building anything (hot CREATE
// paths validate every row's property map).
func validateMapLiteralSyntax(s string) error {
	return forEachStrictMapEntry(s, func(string, string) {})
}

// forEachStrictMapEntry is the single strict map-literal scanner: {} required,
// entries split at top-level commas (quotes and brackets respected), no empty
// entry, a top-level ':' with a non-empty key and value. It calls entry for
// each key / value expression and allocates nothing itself.
func forEachStrictMapEntry(s string, entry func(key, value string)) error {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return localizedError(localization.CypherMergeMapLiteralEnclosureRequired(), nil)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return nil
	}
	check := func(pair string) error {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return localizedError(localization.CypherMergeMapEntryEmpty(), nil)
		}
		colonIdx := findTopLevelMapKeyValueSeparator(pair)
		if colonIdx <= 0 || colonIdx == len(pair)-1 {
			return localizedError(localization.CypherMergeMapEntryInvalid(pair), nil)
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		if key == "" {
			return localizedError(localization.CypherMergeMapKeyEmpty(), nil)
		}
		value := strings.TrimSpace(pair[colonIdx+1:])
		if value == "" {
			return localizedError(localization.CypherMergeMapValueEmpty(key), nil)
		}
		entry(key, value)
		return nil
	}
	for _, pair := range readFunctionSyntax(inner).appendArguments(nil, 0, true) {
		if err := check(pair); err != nil {
			return err
		}
	}
	return nil
}

// splitTopLevelCommaKeepEmpty is like splitTopLevelComma but preserves empty
// entries so strict callers can reject malformed forms such as trailing commas.
func splitTopLevelCommaKeepEmpty(input string) []string {
	return readFunctionSyntax(input).appendArguments(nil, 0, true)
}
