package cypher

import (
	"context"
)

// parseSetMergeMapLiteralStrict parses an inline map literal used by SET +=.
// Unlike permissive property parsing helpers, this enforces Cypher semantics:
// malformed maps must return an error instead of becoming an empty map/no-op.
func (e *StorageExecutor) parseSetMergeMapLiteralStrict(ctx context.Context, s braceView) (map[string]interface{}, error) {
	if err := validateMapLiteralSyntax(s); err != nil {
		return nil, err
	}
	return e.materializePropertyMap(ctx, s, e.fabricRecordBindings, true, false)
}

// parseSetMergeMapExpressionsStrict validates an inline SET += map while
// preserving each value expression for evaluation against an individual row.
func parseSetMergeMapExpressionsStrict(view braceView) (map[string]string, error) {
	expressions := make(map[string]string)
	entries, err := view.requireKeyedEntries()
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		expressions[entry.key()] = entry.value.text()
	}
	return expressions, nil
}

// validateMapLiteralSyntax checks a map literal with the strict grammar of
// parseSetMergeMapExpressionsStrict without building anything (hot CREATE
// paths validate every row's property map).
func validateMapLiteralSyntax(view braceView) error {
	_, err := view.requireKeyedEntries()
	return err
}

// forEachMapEntry consumes the brace owner's parsed entries. It adapts key
// values for property storage; it owns no entry or separator grammar.
