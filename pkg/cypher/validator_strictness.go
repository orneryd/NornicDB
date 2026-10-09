package cypher

import "strings"

func validateExpressionOperandCompleteness(expression string) error {
	expression = strings.TrimSpace(expression)
	for {
		inner, enclosed := stripEnclosingExpressionParentheses(expression)
		if !enclosed {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	last := len(expression) - 1
	if expression == "" || (expression != "*" && strings.ContainsAny(expression[last:], "+-*/%^") && !labelWildcardAt(expression, last)) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: expected an expression after operator")
	}
	return nil
}

func validateExpressionLexicalTokens(text string) error {
	for index := 0; index < len(text); index++ {
		character := text[index]
		if character == '\'' || character == '"' || character == '`' {
			quote := character
			for index++; index < len(text); index++ {
				if text[index] == '\\' && quote != '`' {
					index++
					continue
				}
				if text[index] == quote {
					if quote == '`' && index+1 < len(text) && text[index+1] == '`' {
						index++
						continue
					}
					break
				}
			}
			continue
		}
		if character == '/' && index+1 < len(text) {
			if text[index+1] == '/' {
				for index < len(text) && text[index] != '\n' {
					index++
				}
				continue
			}
			if text[index+1] == '*' {
				if end := strings.Index(text[index+2:], "*/"); end >= 0 {
					index += end + 3
					continue
				}
			}
		}
		malformedOptional := matchKeywordAt(text, index, "OPTIONAL") &&
			(index == 0 || !clauseKeywordUsedAsName(text, index, index+len("OPTIONAL"), "OPTIONAL")) &&
			strings.HasPrefix(text[skipSpaces(text, index+len("OPTIONAL")):], "<tab>")
		if character == '\\' || malformedOptional {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: invalid expression token")
		}
	}
	return nil
}

// Validator strictness helpers (#514 family): forms Neo4j rejects but the
// Nornic validator accepted — the NOT IN operator, trailing/leading commas in
// list literals, adjacent string literals (Cypher has no doubled-quote
// escape), and a dangling UNWIND with no following clause.

// containsNotInOperator reports whether the text contains the two-keyword
// sequence `NOT IN` (only whitespace between them) outside quotes and
// comments. `x NOT IN list` is not Cypher; the negation is `NOT x IN list`.
func containsNotInOperator(cypher string) bool {
	for i := 0; i < len(cypher); i++ {
		c := cypher[i]
		if c == '\'' || c == '"' || c == '`' {
			i = skipCypherQuotedText(cypher, i, c) - 1
			continue
		}
		if c == '/' && i+1 < len(cypher) && (cypher[i+1] == '/' || cypher[i+1] == '*') {
			i = queryCommentEnd(cypher, i) - 1
			continue
		}
		if !matchKeywordAt(cypher, i, "NOT") {
			continue
		}
		next := queryGapEnd(cypher, i+len("NOT"))
		if matchKeywordAt(cypher, next, "IN") {
			return true
		}
	}
	return false
}

// containsTrailingListComma reports whether a list literal has a comma with no
// expression after it (`[1, 2,]`) or no expression before it (`[, 1]`) —
// both are syntax errors in Neo4j.
func containsTrailingListComma(cypher string) bool {
	for i := 0; i < len(cypher); i++ {
		c := cypher[i]
		if c == '\'' || c == '"' || c == '`' {
			i = skipCypherQuotedText(cypher, i, c) - 1
			continue
		}
		if c == '/' && i+1 < len(cypher) && (cypher[i+1] == '/' || cypher[i+1] == '*') {
			i = queryCommentEnd(cypher, i) - 1
			continue
		}
		switch c {
		case ',':
			if queryGapEnd(cypher, i+1) < len(cypher) && cypher[queryGapEnd(cypher, i+1)] == ']' {
				return true
			}
		case '[':
			if next := queryGapEnd(cypher, i+1); next < len(cypher) && cypher[next] == ',' {
				return true
			}
		}
	}
	return false
}

// hasAdjacentStringLiterals reports whether two string literals sit next to
// each other with nothing between them (`'a”b'`). Cypher's string escape is
// a backslash; a doubled quote is not an escape, so `'a”b'` is two adjacent
// literals and Neo4j rejects it.
func hasAdjacentStringLiterals(cypher string) bool {
	for i := 0; i < len(cypher); i++ {
		c := cypher[i]
		if c != '\'' && c != '"' {
			continue
		}
		end := i + 1
		for end < len(cypher) {
			if cypher[end] == '\\' {
				end += 2
				continue
			}
			if cypher[end] == c {
				break
			}
			end++
		}
		if end >= len(cypher) {
			// Unterminated literal; the existing validation rejects it.
			return false
		}
		if end+1 < len(cypher) && (cypher[end+1] == '\'' || cypher[end+1] == '"') {
			return true
		}
		i = end
	}
	return false
}

// validatorClauseKeywords are the clause keywords lastTopLevelClauseWord scans
// for when deciding whether a statement dangles.
var validatorClauseKeywords = []string{
	"RETURN", "WITH", "YIELD", "MATCH", "OPTIONAL", "CREATE", "MERGE",
	"SET", "UNWIND", "CALL", "FOREACH", "DELETE", "DETACH", "REMOVE",
	"LOAD", "UNION", "SHOW", "LET", "FILTER", "FOR",
}

// validatorKeywordFirst is a first-byte index into validatorClauseKeywords so
// the scan tries only keywords whose initial letter matches (allocation-free
// and linear: a token-boundary byte checks at most a couple of candidates).
var validatorKeywordFirst = func() [256][]int {
	table := [256][]int{}
	indexOf := func(b byte) int {
		if b >= 'a' && b <= 'z' {
			return int(b)
		}
		if b >= 'A' && b <= 'Z' {
			return int(b + 32)
		}
		return 0
	}
	for i, keyword := range validatorClauseKeywords {
		b := keyword[0]
		idx := indexOf(b)
		table[idx] = append(table[idx], i)
	}
	return table
}()

func foldByte(b byte) int {
	if b >= 'a' && b <= 'z' {
		return int(b)
	}
	if b >= 'A' && b <= 'Z' {
		return int(b + 32)
	}
	return 0
}

// lastTopLevelClauseWord returns the last top-level clause keyword in s, or "".
// Keywords inside quotes, comments or nested delimiters do not count; keywords
// used as names (aliases, property keys) do not count either.
func lastTopLevelClauseWord(s string) string {
	inQuote := byte(0)
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	last := ""
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote != 0 {
			if c == '\\' && inQuote != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inQuote {
				if i+1 < len(s) && s[i+1] == inQuote {
					i++
					continue
				}
				inQuote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQuote = c
			continue
		case '/':
			if i+1 < len(s) && s[i+1] == '/' {
				for i < len(s) && s[i] != '\n' && s[i] != '\r' {
					i++
				}
				continue
			}
			if i+1 < len(s) && s[i+1] == '*' {
				end := strings.Index(s[i+2:], "*/")
				if end < 0 {
					return last
				}
				i += 2 + end + 1
				continue
			}
		case '(':
			parenDepth++
			continue
		case ')':
			parenDepth--
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			bracketDepth--
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			braceDepth--
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 {
			continue
		}
		if i > 0 && isIdentCharByte(s[i-1]) {
			continue
		}
		for _, keywordIndex := range validatorKeywordFirst[foldByte(c)] {
			keyword := validatorClauseKeywords[keywordIndex]
			if matchKeywordAt(s, i, keyword) &&
				!clauseKeywordUsedAsName(s, i, i+len(keyword), keyword) {
				// A WITH inside an expression is not a clause start:
				// `x STARTS WITH 'a'`, `x ENDS WITH 'a'` and the type
				// predicate `IS :: TIME WITH TIME ZONE`. A real WITH clause
				// cannot follow STARTS/ENDS or precede TIME ZONE.
				if keyword == "WITH" && isExpressionWith(s, i, i+len(keyword)) {
					continue
				}
				// FOR is only an iteration clause in the shared grammar: in
				// schema and alias DDL it is a specifier (CREATE INDEX …
				// FOR (n:Label), CREATE ALIAS … FOR DATABASE …), not a
				// dangling reading clause.
				if keyword == "FOR" && !forIterationClauseAt(s, i) {
					continue
				}
				last = keyword
				break
			}
		}
	}
	return last
}

// forIterationClauseAt reports whether the FOR keyword at s[pos] starts a
// shared-grammar iteration clause (FOR variable IN expression) rather than a
// DDL FOR specifier (CREATE INDEX … FOR (n:Label), CREATE ALIAS … FOR
// DATABASE …). It scans without allocating, mirroring parsePipelineIteration's
// whitespace-only prefix.
func forIterationClauseAt(s string, pos int) bool {
	end := skipSpaces(s, pos+len("FOR"))
	_, end, ok := scanSymbolicName(s, end)
	if !ok {
		return false
	}
	end = skipSpaces(s, end)
	if end+2 > len(s) || !strings.EqualFold(s[end:end+2], "IN") {
		return false
	}
	end += 2
	return end == len(s) || !isAlphaNumericByte(s[end])
}

// isExpressionWith reports whether the top-level word WITH at s[start:end] is
// part of an expression rather than a clause: it follows STARTS or ENDS
// (comments and whitespace may sit between), or it is followed by TIME ZONE.
func isExpressionWith(s string, start, end int) bool {
	if previous := previousSignificantWord(s, start); strings.EqualFold(previous, "STARTS") || strings.EqualFold(previous, "ENDS") {
		return true
	}
	next := nextSignificantWords(s, end)
	return strings.EqualFold(next[0], "TIME") && strings.EqualFold(next[1], "ZONE")
}

// previousSignificantWord returns the word immediately before position i,
// skipping whitespace and comments, or "".
func previousSignificantWord(s string, i int) string {
	j := i
	for {
		for j > 0 && isWhitespace(s[j-1]) {
			j--
		}
		if j >= 2 && s[j-2] == '*' && s[j-1] == '/' {
			open := strings.LastIndex(s[:j-1], "/*")
			if open < 0 {
				return ""
			}
			j = open
			continue
		}
		if j >= 2 && s[j-2] == '/' && s[j-1] == '/' {
			line := strings.LastIndex(s[:j], "\n")
			if line >= 0 {
				j = line
				continue
			}
			return ""
		}
		break
	}
	end := j
	for j > 0 && isIdentCharByte(s[j-1]) {
		j--
	}
	if j == end {
		return ""
	}
	return s[j:end]
}

// nextSignificantWords returns the two words that follow position j, skipping
// whitespace and comments; missing words are "".
func nextSignificantWords(s string, j int) [2]string {
	var result [2]string
	read := func() string {
		for {
			for j < len(s) && isWhitespace(s[j]) {
				j++
			}
			if j+1 < len(s) && s[j] == '/' && s[j+1] == '*' {
				if close := strings.Index(s[j+2:], "*/"); close >= 0 {
					j += 2 + close + 2
					continue
				}
				return ""
			}
			if j+1 < len(s) && s[j] == '/' && s[j+1] == '/' {
				if nl := strings.IndexAny(s[j:], "\r\n"); nl >= 0 {
					j += nl
					continue
				}
				return ""
			}
			break
		}
		start := j
		for j < len(s) && isIdentCharByte(s[j]) {
			j++
		}
		return s[start:j]
	}
	result[0] = read()
	result[1] = read()
	return result
}
