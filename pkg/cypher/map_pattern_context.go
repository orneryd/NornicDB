package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
)

// mapDelimiter records the syntactic owner of one still-open delimiter.
// Only the nearest owner controls a brace: a function/list expression inside
// a node pattern is still an expression, not the node's property map.
type mapDelimiter struct {
	open           int
	kind           byte
	pattern        bool
	existsArgument bool
	existsCall     bool
}

func (context *mapPatternContext) insidePatternDelimiter(query string, end int) bool {
	var stack []mapDelimiter
	previous, prior := -1, -1
	for index := 0; index < end; index++ {
		c := query[index]
		if isCypherQuoteByte(c) {
			index = skipCypherQuotedText(query, index, c) - 1
			prior, previous = previous, index
			continue
		}
		if c == '/' {
			if close := queryCommentEnd(query, index); close > index {
				index = close - 1
				continue
			}
		}
		if cyphertext.IsWhitespaceAt(query, index) {
			continue
		}
		if len(stack) > 0 && stack[len(stack)-1].kind == '(' && stack[len(stack)-1].pattern && matchKeywordAt(query, index, "WHERE") {
			parent := &stack[len(stack)-1]
			label := previous >= 0 && (query[previous] == ':' || query[previous] == '|' || query[previous] == '&' || query[previous] == '!' || equalFoldASCII(wordBefore(query, previous+1), "IS"))
			next := queryGapEnd(query, index+len("WHERE"))
			namedWhere := previous == parent.open && (next >= len(query) || query[next] == ':' || query[next] == '{' || query[next] == ')' || matchKeywordAt(query, next, "WHERE"))
			if !label && !namedWhere {
				parent.pattern = false
			}
		}
		switch c {
		case '(':
			pattern := mapNodeDelimiter(query, index, previous, prior, stack, context)
			existsCall := context.existsCall(query, previous+1)
			existsArgument := existsCall || len(stack) > 0 && stack[len(stack)-1].existsArgument
			stack = append(stack, mapDelimiter{open: index, kind: c, pattern: pattern, existsArgument: existsArgument, existsCall: existsCall})
		case '[':
			pattern := mapRelationshipDelimiter(query, index, previous, context)
			stack = append(stack, mapDelimiter{open: index, kind: c, pattern: pattern})
		case '{':
			stack = append(stack, mapDelimiter{open: index, kind: c})
		case ')', ']', '}':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
		prior, previous = previous, index
	}
	return len(stack) > 0 && stack[len(stack)-1].pattern
}

func mapNodeDelimiter(query string, open, previous, prior int, stack []mapDelimiter, context *mapPatternContext) bool {
	word := ""
	if previous >= 0 {
		word = wordBefore(query, previous+1)
	}
	// Qualified names are function calls, even when the final component
	// spells a clause keyword or built-in path function.
	if word != "" && context.qualified[previous+1-len(word)] {
		return false
	}
	if isPatternClauseKeyword(word) || equalFoldASCII(word, "shortestPath") || equalFoldASCII(word, "allShortestPaths") || context.selected(query, open, stack) {
		return true
	}
	if word != "" {
		switch upperASCII(word) {
		case "WHERE", "AND", "OR", "XOR", "NOT":
		default:
			return false // A function's argument list, including spaced calls.
		}
	}
	close := context.close(open)
	if close >= 0 {
		next := queryGapEnd(query, close+1)
		// Property/index access after a grouped value is an expression,
		// including a group following arithmetic subtraction.
		if next < len(query) && (query[next] == '.' || query[next] == '[') {
			if len(stack) > 0 && stack[len(stack)-1].existsArgument {
				return false
			}
			return mapPatternScope(query, open, stack, context)
		}

		if next < len(query) && query[next] == '<' && next+1 < len(query) && query[next+1] == '-' {
			return true
		}
		if next < len(query) && query[next] == '-' {
			next = skipSpaces(query, next+1)
			if next < len(query) && (query[next] == '-' || query[next] == '[' || query[next] == '>') {
				return true
			}
		}
	}
	// EXISTS accepts a parenthesized pattern predicate. Its argument-list
	// delimiter is an expression owner, but the immediate inner node is a
	// pattern owner; malformed property maps must retain their syntax error.
	if len(stack) > 0 && stack[len(stack)-1].kind == '(' && stack[len(stack)-1].existsCall {
		return true
	}
	if prior >= 0 && ((query[previous] == '>' && query[prior] == '-') || (query[previous] == '-' && (query[prior] == ')' || query[prior] == ']' || query[prior] == '-'))) {
		return true
	}
	// A grouping after WHERE/AND/OR/NOT is still an expression.
	if word != "" {
		return false
	}
	if previous >= 0 && query[previous] != '(' && query[previous] != ',' && query[previous] != ')' && query[previous] != '=' {
		return false
	}
	return mapPatternScope(query, open, stack, context)
}

func mapPatternClause(query string) bool {
	return isPatternClauseKeyword(mapClauseBefore(query))
}

func mapClauseBefore(query string) string {
	last, clause := -1, ""
	for _, keyword := range []string{"MATCH", "CREATE", "MERGE", "WHERE", "RETURN", "WITH", "UNWIND", "SET", "DELETE", "REMOVE", "CALL", "FILTER", "LET", "YIELD"} {
		positions := findAllTopLevelPipelineKeywordPositions(query, keyword)
		if len(positions) > 0 && positions[len(positions)-1] > last {
			last = positions[len(positions)-1]
			clause = keyword
		}
	}
	return clause
}

func mapRelationshipDelimiter(query string, open, previous int, context *mapPatternContext) bool {
	if previous < 0 || query[previous] != '-' {
		return false
	}
	close := context.close(open)
	if close < 0 {
		return false
	}
	next := queryGapEnd(query, close+1)
	if next >= len(query) || query[next] != '-' {
		return false
	}
	next = queryGapEnd(query, next+1)
	if next < len(query) && query[next] == '>' {
		next = queryGapEnd(query, next+1)
	}
	return next < len(query) && query[next] == '('
}

func mapPatternScope(query string, open int, stack []mapDelimiter, context *mapPatternContext) bool {
	if len(stack) == 0 {
		return context.scope(query, 0).pattern(open)
	}
	parent := stack[len(stack)-1]
	if parent.kind != '{' {
		return parent.pattern
	}
	return context.subqueries[parent.open] && context.scope(query, parent.open+1).pattern(open)
}

// Matching delimiters and clause/selector positions are indexed once per
// statement scope. Nested expression groups and sibling patterns then use
// constant-time delimiter lookups rather than rescanning overlapping suffixes.
type mapPatternContext struct {
	closes     map[int]int
	scopes     map[int]*mapPatternClauseIndex
	qualified  map[int]bool
	subqueries map[int]bool
}
type mapPatternClausePosition struct {
	position int
	pattern  bool
}
type mapPatternClauseIndex struct {
	clauses  []mapPatternClausePosition
	selected map[int]bool
}

func newMapPatternContext(query string) *mapPatternContext {
	context := &mapPatternContext{closes: make(map[int]int), scopes: make(map[int]*mapPatternClauseIndex), qualified: make(map[int]bool)}
	var stack []int
	previous := -1
	for index := 0; index < len(query); index++ {
		c := query[index]
		if isCypherQuoteByte(c) {
			index = skipCypherQuotedText(query, index, c) - 1
			previous = index
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(query, index); end > index {
				index = end - 1
				continue
			}
		}
		if size := queryWhitespaceAt(query, index); size > 0 {
			index += size - 1
			continue
		}
		if cyphertext.NameStartAt(query, index) && (index == 0 || !cyphertext.NamePartAt(query, index-1)) && previous >= 0 && query[previous] == '.' {
			context.qualified[index] = true
		}
		if c == '{' && subqueryExpressionAfterLiveByte(query, index, previous) {
			if context.subqueries == nil {
				context.subqueries = make(map[int]bool)
			}
			context.subqueries[index] = true
		}
		previous = index
		switch c {
		case '(', '[', '{':
			stack = append(stack, index)
		case ')', ']', '}':
			if len(stack) == 0 {
				continue
			}
			open := stack[len(stack)-1]
			if query[open] == '(' && c == ')' || query[open] == '[' && c == ']' || query[open] == '{' && c == '}' {
				context.closes[open] = index
				stack = stack[:len(stack)-1]
			}
		}
	}
	return context
}
func (context *mapPatternContext) close(open int) int {
	if close, ok := context.closes[open]; ok {
		return close
	}
	return -1
}
func (context *mapPatternContext) scope(query string, start int) *mapPatternClauseIndex {
	if scope := context.scopes[start]; scope != nil {
		return scope
	}
	scope := &mapPatternClauseIndex{selected: make(map[int]bool)}
	context.scopes[start] = scope
	end := len(query)
	if start > 0 {
		if close := context.close(start - 1); close >= 0 {
			end = close
		}
	}
	text := query[start:end]
	for _, keyword := range []string{"MATCH", "CREATE", "MERGE", "WHERE", "RETURN", "WITH", "UNWIND", "SET", "DELETE", "REMOVE", "CALL", "FILTER", "LET", "YIELD"} {
		for _, position := range findAllTopLevelPipelineKeywordPositions(text, keyword) {
			scope.clauses = append(scope.clauses, mapPatternClausePosition{start + position, isPatternClauseKeyword(keyword)})
		}
	}
	sort.Slice(scope.clauses, func(i, j int) bool { return scope.clauses[i].position < scope.clauses[j].position })
	// The canonical prefix reader consumes only the bounded selector grammar.
	// Invoke it at identifier starts once, never for every subsequent node.
	for index := start; index < end; index++ {
		c := query[index]
		if isCypherQuoteByte(c) {
			index = skipCypherQuotedText(query, index, c) - 1
			continue
		}
		if c == '/' {
			if close := queryCommentEnd(query, index); close > index {
				index = close - 1
				continue
			}
		}
		if !cyphertext.NameStartAt(query, index) || index > start && cyphertext.NamePartAt(query, index-1) {
			continue
		}
		if prefix, ok, _ := scanPathPatternPrefix(query, index, end); ok && scope.pattern(prefix.end) {
			scope.selected[prefix.end] = true
		}
	}
	return scope
}
func (scope *mapPatternClauseIndex) pattern(position int) bool {
	index := sort.Search(len(scope.clauses), func(i int) bool { return scope.clauses[i].position >= position })
	return index > 0 && scope.clauses[index-1].pattern
}
func (context *mapPatternContext) selected(query string, open int, stack []mapDelimiter) bool {
	start := 0
	if len(stack) > 0 {
		parent := stack[len(stack)-1]
		if parent.kind != '{' || !context.subqueries[parent.open] {
			return false
		}
		start = parent.open + 1
	}
	return context.scope(query, start).selected[open]
}

func (context *mapPatternContext) existsCall(query string, end int) bool {
	word := wordBefore(query, end)
	return equalFoldASCII(word, "EXISTS") && !context.qualified[end-len(word)]
}
