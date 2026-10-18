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
	if err := validateMapLiteralSyntax(s); err != nil {
		return nil, err
	}
	return e.materializePropertyMap(ctx, s, e.fabricRecordBindings, true, false)
}

// parseSetMergeMapExpressionsStrict validates an inline SET += map while
// preserving each value expression for evaluation against an individual row.
func parseSetMergeMapExpressionsStrict(s string) (map[string]string, error) {
	expressions := make(map[string]string)
	if err := forEachMapEntry(s, func(key, value string) {
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
	return forEachMapEntry(s, func(string, string) {})
}

// forEachMapEntry validates map structure over the canonical Cypher token
// reader. It owns map grammar, not quotation, comments or delimiter scanning.
func forEachMapEntry(s string, entry func(key, value string)) error {
	s = strings.TrimSpace(StripComments(s))
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return localizedError(localization.CypherMergeMapLiteralEnclosureRequired(), nil)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	var buffer [8]string
	for _, pair := range appendTopLevelCommaParts(buffer[:0], inner, true) {
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
	}
	return nil
}
