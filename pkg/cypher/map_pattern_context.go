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
	previous := -1
	reader := cyphertext.NewReader(query, 0)
	var previousToken, priorToken cyphertext.Token
	for token, ok := reader.Next(); ok && token.Start < end; token, ok = reader.Next() {
		index := token.Start
		c := byte(0)
		if token.Kind == cyphertext.Symbol {
			c = query[index]
		}
		previous = previousToken.End - 1
		if len(stack) > 0 && stack[len(stack)-1].kind == '(' && stack[len(stack)-1].pattern && token.Kind == cyphertext.Word && equalFoldASCII(token.Text(query), "WHERE") {
			parent := &stack[len(stack)-1]
			label := previous >= 0 && (query[previous] == ':' || query[previous] == '|' || query[previous] == '&' || query[previous] == '!' || equalFoldASCII(previousToken.Text(query), "IS"))
			next := mapDelimiterTail(query, index+len("WHERE")-1)
			namedWhere := previous == parent.open && (next >= len(query) || query[next] == ':' || query[next] == '{' || query[next] == ')' || matchKeywordAt(query, next, "WHERE"))
			if !label && !namedWhere {
				parent.pattern = false
			}
		}
		switch c {
		case '(':
			pattern := mapNodeDelimiter(query, index, previousToken, priorToken, stack, context)
			existsCall := previousToken.Kind == cyphertext.Word && equalFoldASCII(previousToken.Text(query), "EXISTS") && !context.qualified[previousToken.Start]
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
		priorToken, previousToken = previousToken, token
	}
	return len(stack) > 0 && stack[len(stack)-1].pattern
}

func mapNodeDelimiter(query string, open int, previousToken, priorToken cyphertext.Token, stack []mapDelimiter, context *mapPatternContext) bool {
	previous, prior := previousToken.End-1, priorToken.End-1
	word := ""
	if previousToken.Kind == cyphertext.Word {
		word = previousToken.Text(query)
	}
	// Qualified names are function calls, even when the final component
	// spells a clause keyword or built-in path function.
	if word != "" && context.qualified[previousToken.Start] {
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
		next := mapDelimiterTail(query, close)
		// Property/index access after a grouped value is an expression,
		// including a group following arithmetic subtraction.
		if next < len(query) && (query[next] == '.' || query[next] == '[') {
			if len(stack) > 0 && stack[len(stack)-1].existsArgument {
				return false
			}
			return mapPatternScope(query, open, stack, context)
		}

		if next < len(query) && query[next] == '<' && mapDelimiterTail(query, next) < len(query) && query[mapDelimiterTail(query, next)] == '-' {
			return true
		}
		if next < len(query) && query[next] == '-' {
			next = mapDelimiterTail(query, next)
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

func mapDelimiterTail(query string, close int) int { return cyphertext.SkipTrivia(query, close+1) }

func mapRelationshipDelimiter(query string, open, previous int, context *mapPatternContext) bool {
	if previous < 0 || query[previous] != '-' {
		return false
	}
	close := context.close(open)
	if close < 0 {
		return false
	}
	next := mapDelimiterTail(query, close)
	if next >= len(query) || query[next] != '-' {
		return false
	}
	next = mapDelimiterTail(query, next)
	if next < len(query) && query[next] == '>' {
		next = mapDelimiterTail(query, next)
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
	var buffer [32]int
	stack := buffer[:0]
	scanner := newSubqueryBraceScanner(query, 0)
	for token, subquery, ok := scanner.next(); ok; token, subquery, ok = scanner.next() {
		index := token.Start
		if token.Kind == cyphertext.Word && scanner.prior.Is(query, ".") {
			context.qualified[index] = true
		}
		if subquery {
			if context.subqueries == nil {
				context.subqueries = make(map[int]bool)
			}
			context.subqueries[index] = true
		}
		if token.Kind != cyphertext.Symbol {
			continue
		}
		switch c := query[index]; c {
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
	reader := cyphertext.NewReader(query, start)
	for token, ok := reader.Next(); ok && token.Start < end; token, ok = reader.Next() {
		if token.Kind != cyphertext.Word {
			continue
		}
		if prefix, ok, _ := scanPathPatternPrefix(query, token.Start, end); ok && scope.pattern(prefix.end) {
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
