package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// validateStaticMapKeys owns map grammar; the shared lexical reader owns every
// token boundary, including trivia around CALL scopes and map selectors.
func validateStaticMapKeys(query string) error {
	schemaGrammar := mapUsesSchemaGrammar(query)
	scanner := newSubqueryBraceScanner(query, 0)
	var patternContext *mapPatternContext
	for span, subquery, ok := scanner.next(); ok; span, subquery, ok = scanner.next() {
		if schemaGrammar || !span.Is(query, "{") {
			continue
		}
		close := findMatchingDelimiter(query, span.Start, '{', '}')
		if close < 0 {
			continue
		}
		owner := mapBraceSyntax(query, span.Start, scanner.prior, subquery, &patternContext)
		if owner == subqueryBrace {
			if err := validateStaticMapKeys(query[span.End:close]); err != nil {
				return err
			}
			scanner.skipBody(close)
			continue
		}
		if owner == quantifiedBrace {
			continue
		}
		parts := cyphertext.CommaSegments(query[span.End:close])
		for part, ok := parts.Next(); ok; part, ok = parts.Next() {
			if _, valid := parseBraceEntry(part, owner); !valid {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "map literal keys must be valid symbolic names")
			}
		}
	}
	if at, invalid := scanner.reader.UnterminatedAt(); invalid {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreInvalidInput(query[at:]))
	}
	return nil
}
func mapUsesSchemaGrammar(query string) bool {
	reader := cyphertext.NewReader(query, 0)
	first, ok := reader.Next()
	if !ok || first.Kind != cyphertext.Word || !strings.EqualFold(first.Text(query), "CREATE") {
		return false
	}
	second, ok := reader.Next()
	if !ok {
		return false
	}
	if second.Kind == cyphertext.Word && strings.EqualFold(second.Text(query), "CONSTRAINT") {
		return findKeywordIndex(query, "REQUIRE") >= 0
	}
	for token := second; token.End > token.Start; {
		if token.Is(query, "(") {
			break
		}
		if token.Kind == cyphertext.Word && strings.EqualFold(token.Text(query), "INDEX") {
			return findKeywordIndex(query, "OPTIONS") >= 0
		}
		token, ok = reader.Next()
		if !ok {
			break
		}
	}
	return false
}

// mapEntryTokens recognises a key followed by a colon without treating comments
// as part of either the key or its value expression.
func mapEntryTokens(part string) (key, value string, ok bool) {
	reader := cyphertext.NewReader(part, 0)
	first, exists := reader.Next()
	if !exists {
		return "", "", false
	}
	colon, exists := reader.Next()
	if !exists || !colon.Is(part, ":") {
		return "", "", false
	}
	return first.Text(part), part[colon.End:], true
}

func singleMapName(text string) string {
	reader := cyphertext.NewReader(text, 0)
	token, ok := reader.Next()
	if !ok {
		return ""
	}
	if _, more := reader.Next(); more {
		return ""
	}
	name := token.Text(text)
	if _, ok := isOneSymbolicName(name); ok {
		return name
	}
	return ""
}

func validMapLiteralKey(key string) bool {
	reader := cyphertext.NewReader(key, 0)
	token, ok := reader.Next()
	if !ok {
		return false
	}
	if _, more := reader.Next(); more {
		return false
	}
	raw := token.Text(key)
	if _, ok := isOneSymbolicName(raw); ok {
		return true
	}
	return isWholeCypherQuotedString(raw)
}

func undefinedStandaloneMapValue(expression string) string {
	text := cyphertext.TrimTrivia(expression)
	reader := cyphertext.NewReader(text, 0)
	open, ok := reader.Next()
	if !ok || !open.Is(text, "{") {
		return ""
	}
	close := findMatchingDelimiter(text, open.Start, '{', '}')
	if close != len(text)-1 {
		return ""
	}
	parts := cyphertext.CommaSegments(text[open.End:close])
	for part, ok := parts.Next(); ok; part, ok = parts.Next() {
		_, value, keyed := mapEntryTokens(part)
		if !keyed {
			continue
		}
		if name := singleMapName(value); name != "" && !strings.EqualFold(name, "null") && !strings.EqualFold(name, "true") && !strings.EqualFold(name, "false") {
			return name
		}
		if nested := undefinedStandaloneMapValue(value); nested != "" {
			return nested
		}
	}
	return ""
}

func inProjectionClause(query string, position int) bool {
	switch mapClauseBefore(query[:position]) {
	case "RETURN", "WITH", "UNWIND", "SET":
		return true
	}
	return false
}
