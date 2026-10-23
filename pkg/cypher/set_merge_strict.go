package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"

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
	text := cyphertext.TrimTrivia(s)
	reader := cyphertext.NewReader(text, 0)
	open, ok := reader.Next()
	if !ok || !open.Is(text, "{") || findMatchingDelimiter(text, open.Start, '{', '}') != len(text)-1 {
		return localizedError(localization.CypherMergeMapLiteralEnclosureRequired(), nil)
	}
	parts := cyphertext.CommaSegments(text[open.End : len(text)-1])
	for raw, ok := parts.Next(); ok; raw, ok = parts.Next() {
		pair := cyphertext.TrimTrivia(raw)
		if pair == "" {
			return localizedError(localization.CypherMergeMapEntryEmpty(), nil)
		}
		parsed, valid := parseBraceEntry(pair, literalBrace)
		if !valid {
			return localizedError(localization.CypherMergeMapEntryInvalid(pair), nil)
		}
		key := normalizePropertyKey(parsed.key)
		if key == "" {
			return localizedError(localization.CypherMergeMapKeyEmpty(), nil)
		}
		entry(key, cyphertext.TrimTrivia(parsed.value))
	}
	return nil
}
