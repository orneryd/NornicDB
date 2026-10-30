// Author: #1663 implementation lane. Original pattern context consumes the
// lexical record and the existing statement-CST owner in this shared layer.
package antlr

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

func (context *mapPatternContext) insidePatternDelimiter(_ string, end int) bool {
	return context.patternBraces[end]
}

func (context *mapPatternContext) indexPatternDelimiters(query string, infos []cyphertext.BraceToken) {
	var stack []mapDelimiter
	previous, prior := -1, -1
	for _, info := range infos {
		index := info.Start
		if len(stack) > 0 && stack[len(stack)-1].kind == '(' && stack[len(stack)-1].pattern && info.Keyword(query, "WHERE") {
			parent := &stack[len(stack)-1]
			label := previous >= 0 && (query[previous] == ':' || query[previous] == '|' || query[previous] == '&' || query[previous] == '!' || context.endingAt(previous+1).Keyword(query, "IS"))
			next := mapDelimiterTail(query, info.End-1)
			namedWhere := previous == parent.open && (next >= len(query) || query[next] == ':' || query[next] == '{' || query[next] == ')' || braceKeywordAt(query, next, "WHERE"))
			if !label && !namedWhere {
				parent.pattern = false
			}
		}
		if info.Kind == cyphertext.Symbol {
			switch query[index] {
			case '(':
				pattern := mapNodeDelimiter(query, index, previous, prior, stack, context)
				existsCall := context.existsCall(query, previous+1)
				existsArgument := existsCall || len(stack) > 0 && stack[len(stack)-1].existsArgument
				stack = append(stack, mapDelimiter{open: index, kind: '(', pattern: pattern, existsArgument: existsArgument, existsCall: existsCall})
			case '[':
				stack = append(stack, mapDelimiter{open: index, kind: '[', pattern: mapRelationshipDelimiter(query, index, previous, context)})
			case '{':
				context.patternBraces[index] = len(stack) > 0 && stack[len(stack)-1].pattern
				stack = append(stack, mapDelimiter{open: index, kind: '{'})
			case ')', ']', '}':
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
			}
		}
		prior, previous = previous, info.End-1
	}
}
func mapNodeDelimiter(query string, open, previous, prior int, stack []mapDelimiter, context *mapPatternContext) bool {
	word := ""
	token := context.endingAt(previous + 1)
	if token.Kind == cyphertext.Word {
		word = token.Text(query)
	}
	// Qualified names are function calls, even when the final component
	// spells a clause keyword or built-in path function.
	if word != "" && context.qualified[token.Start] {
		return false
	}
	if token.Keyword(query, "MATCH") || token.Keyword(query, "CREATE") || token.Keyword(query, "MERGE") || token.Keyword(query, "shortestPath") || token.Keyword(query, "allShortestPaths") || context.selected(query, open, stack) {
		return true
	}
	if word != "" && !token.Keyword(query, "WHERE") && !token.Keyword(query, "AND") && !token.Keyword(query, "OR") && !token.Keyword(query, "XOR") && !token.Keyword(query, "NOT") {
		return false // A function's argument list, including spaced calls.
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
			next = braceGapEnd(query, next+1)
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

func mapDelimiterTail(query string, close int) int {
	next := close + 1
	for next < len(query) {
		if cyphertext.IsWhitespaceAt(query, next) {
			next++
			continue
		}
		if query[next] == '/' {
			if end := queryCommentEnd(query, next); end > next {
				next = end
				continue
			}
		}
		break
	}
	return next
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
	tokens        []cyphertext.BraceToken
	byEnd         map[int]int
	closes        map[int]int
	scopes        map[int]*mapPatternClauseIndex
	qualified     map[int]bool
	subqueries    map[int]bool
	patternBraces map[int]bool
	source        *cyphertext.Source
	grammar       StatementSyntax
}
type mapPatternClausePosition struct {
	position int
	pattern  bool
}
type mapPatternClauseIndex struct {
	clauses  []mapPatternClausePosition
	selected map[int]bool
}

// The original token record supplies body roles, quoted spans and delimiters.
// Pattern context adds one statement-grammar pass over those same tokens.
func newMapPatternContextRecords(source *cyphertext.Source, infos []cyphertext.BraceToken, grammar StatementSyntax) *mapPatternContext {
	query := source.Text
	context := &mapPatternContext{source: source, grammar: grammar, tokens: infos, byEnd: make(map[int]int), closes: make(map[int]int), scopes: make(map[int]*mapPatternClauseIndex), qualified: make(map[int]bool), subqueries: make(map[int]bool), patternBraces: make(map[int]bool)}
	for index, info := range infos {
		context.byEnd[info.End] = index
		if info.Kind == cyphertext.Word && index > 0 && infos[index-1].Is(query, ".") {
			context.qualified[info.Start] = true
		}
		if info.Kind != cyphertext.Symbol {
			continue
		}
		switch query[info.Start] {
		case '(', '[', '{':
			extent, found, err := source.DelimiterSpan(info.Start)
			if err != nil {
				panic(err)
			}
			if found {
				context.closes[info.Start] = extent.End - 1
			}
			if info.Is(query, "{") && info.Body {
				context.subqueries[info.Start] = true
			}

		}
	}
	context.indexPatternDelimiters(query, infos)
	return context
}

func (context *mapPatternContext) endingAt(end int) cyphertext.Token {
	if index, found := context.byEnd[end]; found {
		return context.tokens[index].Token
	}
	return cyphertext.Token{}
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
	// The retained CST owns clause identity. Project its original clause starts
	// into this scope; no fragment scanner or second keyword/name recognizer.
	if context.grammar.Valid() {
		context.grammar.Walk(func(node SyntaxNode) func() {
			pattern := false
			switch node.Rule() {
			case CypherParserRULE_matchSt, CypherParserRULE_createSt, CypherParserRULE_mergeSt:
				pattern = true
			case CypherParserRULE_where, CypherParserRULE_returnSt, CypherParserRULE_withSt, CypherParserRULE_unwindSt, CypherParserRULE_setSt, CypherParserRULE_deleteSt, CypherParserRULE_removeSt, CypherParserRULE_queryCallSt, CypherParserRULE_filterSt, CypherParserRULE_letSt, CypherParserRULE_yieldItems:
			default:
				return nil
			}
			original, _ := node.Span()
			for _, span := range context.source.ProjectFrom(context.source.OriginalView(), cyphertext.Span{Start: original, End: original + 1}) {
				if span.Start < start || span.Start >= end {
					continue
				}
				// Only clauses at this query-body depth govern its pattern sequence.
				nested := false
				for open, close := range context.closes {
					if open >= start && open < span.Start && close >= span.Start {
						nested = true
						break
					}
				}
				if !nested {
					scope.clauses = append(scope.clauses, mapPatternClausePosition{span.Start, pattern})
				}
			}
			return nil
		})
		sort.Slice(scope.clauses, func(i, j int) bool { return scope.clauses[i].position < scope.clauses[j].position })
	} // The canonical prefix reader consumes only the bounded selector grammar.
	// Invoke it at identifier starts once, never for every subsequent node.
	for _, token := range context.tokens {
		if token.Start < start {
			continue
		}
		if token.Start >= end {
			break
		}
		if token.Kind != cyphertext.Word {
			continue
		}
		if prefix, ok, _ := ScanPathPatternPrefix(query, token.Start, end); ok && scope.pattern(prefix.End) {
			scope.selected[prefix.End] = true
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
	token := context.endingAt(end)
	return token.Keyword(query, "EXISTS") && !context.qualified[token.Start]
}
