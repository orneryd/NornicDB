package cypher

import "strings"

// lexicalExpressionText returns expression code independently of optional query
// normalization. Comment-looking quoted values and names remain data, and a
// comment between tokens must not join them into a different identifier.
// Expression readers share the existing comment lexer rather than inspecting
// comment contents while recognizing identifiers or enclosing groups.
func lexicalExpressionText(text string) string {
	if strings.IndexByte(text, '/') >= 0 {
		text = StripComments(text)
	}
	return strings.TrimSpace(text)
}
