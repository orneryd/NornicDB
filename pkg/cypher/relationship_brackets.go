package cypher

// Relationship brackets in pattern text are found by one scanner for every
// reader (CREATE / MERGE shape checks, relationship variable scoping, the MERGE
// relationship parser and OPTIONAL MATCH endpoints). A '[' starts a
// relationship only when it follows '-' (as in -[ and <-[); a '[' inside a
// property map (and so inside a subquery expression in a property value), a
// list, a string literal or a backtick-quoted name does not.

// nextRelationshipBracket returns the first relationship bracket of text at or
// after from, as the index of its '[' and of the matching ']', or -1, -1 when
// there is none (or it is not closed).
func nextRelationshipBracket(text string, from int) (open, close int) {
	for index := from; index < len(text); index++ {
		switch c := text[index]; c {
		case '\'', '"':
			end := index + 1
			for end < len(text) && (text[end] != c || isBackslashEscaped(text, end)) {
				end++
			}
			index = end
		case '`':
			end := index + 1
			for end < len(text) && text[end] != '`' {
				end++
			}
			index = end
		case '{':
			if end := findMatchingDelimiter(text, index, '{', '}'); end > index {
				index = end
			}
		case '[':
			previous := index - 1
			for previous >= 0 && isASCIISpace(text[previous]) {
				previous--
			}
			if previous < 0 || text[previous] != '-' {
				continue
			}
			close := findMatchingBracket(text, index)
			if close < 0 {
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
func patternHasRelationship(pattern string) bool {
	for index := 0; index < len(pattern); index++ {
		switch c := pattern[index]; c {
		case '\'', '"':
			end := index + 1
			for end < len(pattern) && (pattern[end] != c || isBackslashEscaped(pattern, end)) {
				end++
			}
			index = end
		case '`':
			end := index + 1
			for end < len(pattern) && pattern[end] != '`' {
				end++
			}
			index = end
		case '{':
			if end := findMatchingDelimiter(pattern, index, '{', '}'); end > index {
				index = end
			}
		case '-':
			if index+1 < len(pattern) && (pattern[index+1] == '>' || pattern[index+1] == '[') {
				return true
			}
		case '<', ']':
			if index+1 < len(pattern) && pattern[index+1] == '-' {
				return true
			}
		}
	}
	return false
}

// firstRelationshipBracket returns the first relationship bracket of text
// (nextRelationshipBracket), or -1, -1.
func firstRelationshipBracket(text string) (open, close int) {
	return nextRelationshipBracket(text, 0)
}
