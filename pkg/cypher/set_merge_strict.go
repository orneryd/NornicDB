package cypher

import (
	"context"
	"strings"

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
	if err := forEachMapEntry(s, mapSourceText, func(key, value string) {
		expressions[key] = value
	}, nil); err != nil {
		return nil, err
	}
	return expressions, nil
}

// validateMapLiteralSyntax checks a map literal with the strict grammar of
// parseSetMergeMapExpressionsStrict without building anything (hot CREATE
// paths validate every row's property map).
func validateMapLiteralSyntax(s string) error {
	return forEachMapEntry(s, mapSourceText, func(string, string) {}, nil)
}

type mapEntryText uint8

const (
	mapSourceText mapEntryText = iota
	mapCodeText
)

// forEachMapEntry validates map structure over the canonical Cypher token
// reader. It owns map grammar, not quotation, comments or delimiter scanning.
//
// starts is the source-occurrence sidecar: when it is not nil it collects where
// each entry's value begins in s, in entry order, read from the same comma scan
// and key/value split that produce the text. PROFILE is the only reader that
// asks for positions; every other reader passes nil, reads the same entries and
// measures none of them.
func forEachMapEntry(s string, source mapEntryText, entry func(key, value string), starts *[]int) error {
	sourceText := s
	var commentEdits []cyphertext.SourceEdit
	if source == mapSourceText {
		if starts == nil {
			s = StripComments(s)
		} else {
			s = stripCommentText(s, &commentEdits)
		}
	}
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeMapLiteralEnclosureRequired())
	}
	body := trimmed[1 : len(trimmed)-1]
	inner := strings.TrimSpace(body)
	var parts rowValuePartList
	innerStart := 0
	var view *cyphertext.Source
	if starts != nil {
		innerStart = leadingExpressionSpace(s) + 1 + leadingExpressionSpace(body)
		var buffer [8]expressionFragment
		parts.spans = appendExpressionParts(buffer[:0], inner, true)
		if len(commentEdits) != 0 {
			original := cyphertext.Source{Original: sourceText, Text: sourceText}
			view = original.Rewrite(s, commentEdits)
		}
	} else {
		var buffer [8]string
		parts.texts = appendTopLevelCommaParts(buffer[:0], inner, true)
	}
	for index := range parts.count() {
		pair, pairStart := parts.at(index)
		if pair == "" {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeMapEntryEmpty())
		}
		colonIdx := findTopLevelMapKeyValueSeparator(pair)
		if colonIdx <= 0 || colonIdx == len(pair)-1 {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeMapEntryInvalid(pair))
		}
		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		if key == "" {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeMapKeyEmpty())
		}
		value := strings.TrimSpace(pair[colonIdx+1:])
		if value == "" {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeMapValueEmpty(key))
		}
		if starts != nil {
			start := innerStart + pairStart + colonIdx + 1 + leadingExpressionSpace(pair[colonIdx+1:])
			if view != nil {
				start, _ = view.OriginalOffset(start, true, cyphertext.SourceExtent)
			}
			*starts = append(*starts, start)
		}
		entry(key, value)
	}
	return nil
}
