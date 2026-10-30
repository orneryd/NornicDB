package cypher

// isListLiteral reports whether text is one list literal: it starts with '['
// and that '[' closes at its end. The brackets are matched by
// cyphertext.FindMatchingDelimiter, which skips quoted text (backslash-escaped quotes
// included), quoted names and comments. An expression over a list literal,
// ['a', 'b'][i], [1] + [2] or [x][0..1], is not one: its readers evaluate it
// as an expression, per row. Every reader of list literal text asks this
// (TestListLiteralReadersUseIsListLiteral); none checks "starts with '[' and
// ends with ']'" on its own.
func isListLiteral(text string) bool {
	return len(text) >= 2 && text[0] == '[' && text[len(text)-1] == ']' && findMatchingBracket(text, 0) == len(text)-1
}
