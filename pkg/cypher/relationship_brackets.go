package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// Relationship brackets in pattern text are found by one scanner for every
// reader (CREATE / MERGE shape checks, relationship variable scoping, the MERGE
// relationship parser and OPTIONAL MATCH endpoints). A '[' starts a
// relationship only when it follows '-' (as in -[ and <-[); a '[' inside a
// property map (and so inside a subquery expression in a property value), a
// list, a string literal or a backtick-quoted name does not.

type relationshipBracketPolicy uint8

const (
	requireClosedRelationship relationshipBracketPolicy = iota
	reportUnclosedRelationship
)

// readRelationshipBracket owns both closed lookup and malformed-pattern
// reporting. Validation retains the opening offset of an unclosed bracket;
// execution readers receive no bracket until both delimiters are present.
func readRelationshipBracket(view braceView, from int, policy relationshipBracketPolicy) (open, close int) {
	text := view.text()
	for index := from; index < len(text); index++ {
		switch c := text[index]; {
		case (cypherLexicalByte(c)) == cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
		case (cypherLexicalByte(c)) == '/':
			if end := queryCommentEnd(text, index); end >= 0 {
				index = end - 1
			}
		case cyphertext.IsOpenBrace(cypherLexicalByte(c)):
			if end, complete := view.close(index); complete && end > index {
				index = end
			}
		case (cypherLexicalByte(c)) == '[':
			previous := index - 1
			for previous >= 0 && cyphertext.IsWhitespaceAt(text, previous) {
				previous--
			}
			if previous < 0 || text[previous] != '-' {
				continue
			}
			close := findMatchingBracket(text, index)
			if close < 0 && policy == requireClosedRelationship {
				return -1, -1
			}
			return index, close
		}
	}
	return -1, -1
}

// patternHasRelationship reports whether pattern text has a relationship
// (->, <-, -[ or ]-) outside string literals, quoted names and property maps; a
// subquery expression in a property value ({k: COUNT { (a)-->() }}) is not
// part of the pattern.
func patternHasRelationship(view braceView) bool {
	pattern := view.text()
	for index := 0; index < len(pattern); index++ {
		switch c := pattern[index]; {
		case (cypherLexicalByte(c)) == cypherQuoteClass:
			index = skipCypherQuotedText(pattern, index, c) - 1
		case (cypherLexicalByte(c)) == '/':
			if end := queryCommentEnd(pattern, index); end >= 0 {
				index = end - 1
			}
		case cyphertext.IsOpenBrace(cypherLexicalByte(c)):
			if end, complete := view.close(index); complete && end > index {
				index = end
			}
		case (cypherLexicalByte(c)) == '-':
			if index+1 < len(pattern) && (pattern[index+1] == '>' || pattern[index+1] == '[') {
				return true
			}
		case (cypherLexicalByte(c)) == '<', (cypherLexicalByte(c)) == ']':
			if index+1 < len(pattern) && pattern[index+1] == '-' {
				return true
			}
		}
	}
	return false
}
