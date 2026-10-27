package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

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
		if end, quoted := skipCypherLexicalToken(text, index); quoted {
			index = end - 1
			continue
		}
		character := text[index]
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
		if isCypherQuoteByte(c) {
			i = skipCypherQuotedText(cypher, i, c) - 1
			continue
		}
		if c == '/' && i+1 < len(cypher) && (cypher[i+1] == '/' || cypher[i+1] == '*') {
			i = queryCommentEnd(cypher, i) - 1
			continue
		}
		// NOT is a valid local iterator name, read by the same declaration
		// owner as the list/quantifier evaluators, rather than an infix NOT.
		if c == '[' || c == '(' && bindingFunctionBeforeParen(cypher, i) != "" {
			if name, at, _, declared := bindingDeclaration(cypher, i+1); declared && strings.EqualFold(name, "NOT") {
				end, _ := cyphertext.ScanIdentifier(cypher, at)
				i = end.End - 1
				continue
			}
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
		if isCypherQuoteByte(c) {
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
		if isCypherBacktickByte(c) {
			i = skipCypherQuotedText(cypher, i, c) - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(cypher, i); end >= 0 {
				i = end - 1
				continue
			}
		}
		if !isCypherStringQuoteByte(c) {
			continue
		}
		end, closed := scanCypherQuotedText(cypher, i, c)
		if !closed {
			// Unterminated literal; the existing validation rejects it.
			return false
		}
		// The shared lexer recognizes doubled quote bytes as one quoted span;
		// Cypher's string grammar rejects that spelling, so retain the strict
		// check inside the already-delimited token.
		for index := i + 1; index+1 < end; index++ {
			if cypher[index] == '\\' {
				index++
				continue
			}
			if cypher[index] == c && cypher[index+1] == c {
				return true
			}
		}
		if end < len(cypher) && (isCypherStringQuoteByte(cypher[end])) {
			return true
		}
		i = end - 1
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
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	last := ""
	for i := 0; i < len(s); i++ {
		if end, quoted := skipCypherLexicalToken(s, i); quoted {
			i = end - 1
			continue
		}
		c := s[i]
		switch c {
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
		if i > 0 && cyphertext.NamePartAt(s, i-1) {
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
	return end == len(s) || !cyphertext.NamePartAt(s, end)
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
	// Scan opaque extents forward through the existing significant-byte
	// owner, then retain this reader's reverse name-continuation question.
	j := lastLiveByte(s, i) + 1
	end := j
	for j > 0 && cyphertext.NamePartAt(s, j-1) {
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
		var unclosed int
		j, unclosed = cyphertext.GapEnd(s, j)
		if unclosed >= 0 {
			return ""
		}
		start := j
		j = cyphertext.NamePartEnd(s, j)
		return s[start:j]
	}
	result[0] = read()
	result[1] = read()
	return result
}
