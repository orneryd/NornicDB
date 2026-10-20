package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
//
// A statement whose backtick-quoted variables were canonicalized is cached
// under the text the client wrote: statements that differ only in quoting
// canonicalize alike but can validate differently (their column names
// differ).
func (e *StorageExecutor) validateSemanticScopes(ctx context.Context, cypher string, recordings ...*scopeRecording) error {
	return e.validateSemanticScopesWithInput(ctx, cypher, nil, recordings...)
}

func (e *StorageExecutor) validateSemanticScopesWithInput(ctx context.Context, cypher string, supplied *semanticStatementInput, recordings ...*scopeRecording) error {
	scopeText := cypher
	if record := firstScopeRecording(recordings); record != nil && record.source != "" {
		scopeText = record.source
	}
	names := quotedVariableNamesFor(ctx, cypher)
	cypher25 := cypherVersionFromContext(ctx) == "25"
	cacheKey := cypher
	if names != nil {
		cacheKey = names.original
	}
	cacheKey = semanticCacheKey(cacheKey, cypher25)
	// A text cached under its own key passed every check here, the lexical
	// one included, so a repeated query skips them all (#823). A rewritten
	// text cached under its original still gets its own lexical check.
	// Literal-bearing texts share one compile verdict through the canonical
	// form (PR #1043 #6), looked up only when the exact text misses: a
	// canonical hit still runs the lexical check on its own text, then
	// returns.
	cached := e.semanticValidationCache.contains(cacheKey)
	var canonical string
	var normalized bool
	if !cached {
		canonical, normalized = normalizeLiteralText(cacheKey)
		cached = normalized && e.semanticValidationCache.contains(canonical)
	}
	if cached && names == nil && !normalized {
		if firstScopeRecording(recordings) != nil {
			return e.validateMatchSemanticScopes(scopeText, cypher25, recordings...)
		}
		return nil
	}
	if err := validateExpressionLexicalTokens(cypher); err != nil {
		return err
	}
	if cached {
		// A canonical hit shared the compile verdict with a sibling that
		// differs only in literal values, but SKIP / LIMIT bounds are
		// value-dependent: LIMIT 2-3 and LIMIT 5-1 share one canonical form,
		// yet only the latter is a non-negative integer. Re-evaluate them on
		// this text before returning (monster retest part 3).
		if err := e.validateStaticPaginationExpressions(cypher); err != nil {
			return err
		}
		if firstScopeRecording(recordings) != nil {
			return e.validateMatchSemanticScopes(scopeText, cypher25, recordings...)
		}
		return nil
	}
	var native *fabric.QueryStructure
	if record := firstScopeRecording(recordings); record != nil {
		native = record.admission
	}
	input := matchingSemanticInput(scopeText, supplied)
	if native != nil {
		input = newSemanticStatementInput(scopeText, native)
	}
	input.names = names
	if record := firstScopeRecording(recordings); record != nil && record.names != nil {
		input.names = record.names
	}
	// Declaration adapters project facts only; an invalid native parse has
	// no declarations to project. Admit its syntax here, once, before a
	// missing descriptor could be mistaken for a valid empty scope. The
	// existing extension boundary is shared with the ANTLR syntax validator.
	if !isNornicExtensionStatement(cypher) {
		if syntax := input.syntax(); syntax != nil && syntax.Err != nil {
			return syntax.Err
		}
	}
	if err := validateListComprehensionVariables(cypher, input); err != nil {
		return err
	}
	if err := validateStaticQuantifierTypes(cypher); err != nil {
		return err
	}
	if err := validateStaticFunctionArguments(cypher, cypher25); err != nil {
		return err
	}
	if err := e.validateStaticPaginationExpressions(cypher, input); err != nil {
		return err
	}
	if err := validateWithProjectionSemantics(cypher, input); err != nil {
		return err
	}
	if err := validateAggregatingProjectionScopes(cypher, input); err != nil {
		return err
	}
	if err := validateExistsSubqueryClauseComposition(cypher); err != nil {
		return err
	}
	if err := validatePatternExpressionPlacement(cypher, input); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";"))
	if strings.HasSuffix(trimmed, ",") {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidProjection",
			"RETURN projection cannot end with an empty item",
		)
	}
	if invalidAggregationInListComprehension(cypher, input) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed inside list-comprehension predicates or projections",
		)
	}
	if err := e.validateDuplicateReturnColumnName(cypher, names); err != nil {
		return err
	}
	if clauses, ok := input.pipeline(true); ok {
		pendingUpdate := false
		for _, clause := range clauses {
			if pendingUpdate && clause.kind == pipelineClauseCall {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidClauseComposition", "WITH is required between an updating clause and CALL")
			}
			switch clause.kind {
			case pipelineClauseWith:
				pendingUpdate = false
			case pipelineClauseCreate, pipelineClauseMerge, pipelineClauseSet, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
				pendingUpdate = true
			}
			// Subquery bodies are checked as their own statements.
			if whereIndex := topLevelKeywordIndex(clause.text, "WHERE"); whereIndex >= 0 &&
				hasUnexpectedIdentifierAfterNumber(maskSubqueryBodies(clause.text[whereIndex+len("WHERE"):])) {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unexpected identifier in WHERE")
			}
			if clause.kind == pipelineClauseWith && withProjectionHasEmptyItem(clause.text, clause.syntax) {
				return emptyProjectionItemError("WITH")
			}
			if clause.kind == pipelineClauseReturn {
				body := strings.TrimSpace(clause.text[len("RETURN"):])
				if err := validateReturnAggregationSemantics(body); err != nil {
					return err
				}
				// The clause is scanned with its keyword, which tells a
				// keyword-named first item from a clause (RETURN union[0], #894).
				clauseText := strings.TrimSpace(clause.text)
				end := len(clauseText)
				for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT", "UNION"} {
					if index := topLevelKeywordIndex(clauseText, keyword); index >= len("RETURN") && index < end {
						end = index
					}
				}
				items := clauseText[len("RETURN"):end]
				if projectionHasEmptyItem(items) {
					return emptyProjectionItemError("RETURN")
				}
				projectionItems, _ := cutDistinct(strings.TrimSpace(items))
				for _, item := range splitTopLevelComma(projectionItems) {
					expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
					if err := validateExpressionOperandCompleteness(expression); err != nil {
						return err
					}
					if err := projectionAliasError(item, clause.syntax); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := e.validateMatchSemanticScopesInput(scopeText, cypher25, input, recordings...); err != nil {
		return err
	}
	// Every function a statement calls, in any clause, is known (#1166).
	// Neo4j reports the statement's other semantic errors first.
	if err := validateKnownFunctions(cypher); err != nil {
		return err
	}
	if normalized {
		e.semanticValidationCache.add(canonical)
		e.semanticValidationCache.add(cacheKey)
	} else {
		e.semanticValidationCache.add(cacheKey)
	}
	return nil
}

// emptyProjectionItemError is Neo4j's SyntaxError for a RETURN or WITH item
// list with an empty item (RETURN 1,,2 / RETURN , 1 / WITH a,, b).
func emptyProjectionItemError(clause string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidProjection",
		clause+" projection cannot contain an empty item",
	)
}

// withProjectionHasEmptyItem reports whether a WITH clause's item list,
// before its WHERE / ORDER BY / SKIP / LIMIT, has an empty item.
func withProjectionHasEmptyItem(clause string, syntax ...*fabric.QueryClause) bool {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH", syntax...)
	return projectionHasEmptyItem(body)
}

// projectionHasEmptyItem reports whether a projection item list has an
// empty item: a leading, trailing or doubled top-level comma.
func projectionHasEmptyItem(items string) bool {
	items = strings.TrimSpace(items)
	if items == "" || strings.HasPrefix(items, ",") || strings.HasSuffix(items, ",") {
		return true
	}
	for _, item := range splitTopLevelComma(items) {
		if strings.TrimSpace(item) == "" {
			return true
		}
	}
	return false
}

// validateDuplicateReturnColumnName rejects a RETURN with two items of the
// same column name. names, when the statement was canonicalized, gives the
// items as the client wrote them, which name the columns.
func (e *StorageExecutor) validateDuplicateReturnColumnName(cypher string, names *quotedVariableNames) error {
	if duplicate := e.duplicateReturnColumnName(cypher, names); duplicate != "" {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"ColumnNameConflict",
			"Multiple RETURN items project the same column name: "+duplicate,
		)
	}
	return nil
}

func hasUnexpectedIdentifierAfterNumber(expression string) bool {
	for index := 0; index < len(expression); index++ {
		if end, skipped := skipCypherLexicalToken(expression, index); skipped {
			index = end - 1
			continue
		}
		if expression[index] < '0' || expression[index] > '9' || (index > 0 && isIdentByte(expression[index-1])) {
			continue
		}
		end := index + 1
		for end < len(expression) && (expression[end] >= '0' && expression[end] <= '9' || expression[end] == '.') {
			end++
		}
		next := skipSpaces(expression, end)
		if next > end {
			if name, _, ok := scanIdentifierToken(expression, next); ok {
				switch upperASCII(name) {
				case "AND", "OR", "XOR", "IN", "IS", "THEN", "ELSE", "END", "AS", "STARTS", "ENDS", "CONTAINS", "ORDER", "SKIP", "LIMIT", "UNION":
				default:
					return true
				}
			}
		}
		index = end - 1
	}
	return false
}

func validateWithProjectionSemantics(cypher string, inputs ...*semanticStatementInput) error {
	clauses, ok := sharedSemanticClauses(cypher, inputs...)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		if clause.kind != pipelineClauseWith {
			continue
		}
		body := projectionSemanticBody(clause.text, "WITH", clause.syntax)
		if err := validateReturnAggregationSemantics(body); err != nil {
			return err
		}
		seen := make(map[string]struct{})
		for _, raw := range splitTopLevelComma(body) {
			item := strings.TrimSpace(raw)
			if item == "" || item == "{}" {
				continue
			}
			expression, alias := parseProjectionExprAlias(item)
			if err := validateExpressionOperandCompleteness(expression); err != nil {
				return err
			}
			explicitAlias := projectionAliasIndex(item) > 0
			// true, false and null are literals, not variables, even when a
			// variable of that name was declared (isLiteralKeyword).
			if !explicitAlias && expression != "*" && (simpleSemanticIdentifier(expression) == "" || isLiteralKeyword(expression)) {
				if containsMalformedCreateClauseToken(expression) {
					continue
				}
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"NoExpressionAlias",
					"expressions in WITH must be explicitly aliased",
				)
			}
			name := alias
			if name == "*" {
				continue
			}
			if _, duplicate := seen[name]; duplicate {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"ColumnNameConflict",
					"multiple WITH items project the same column name: "+name,
				)
			}
			seen[name] = struct{}{}
		}
	}
	return nil
}

func containsMalformedCreateClauseToken(expression string) bool {
	for index := 0; index < len(expression); {
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		if strings.EqualFold(name, "CREAT") {
			cursor := skipSpaces(expression, next)
			return cursor < len(expression) && expression[cursor] == '('
		}
		index = next
	}
	return false
}

// validatePatternExpressionPlacement enforces the rule that a pattern
// expression only tests whether its pattern exists: it is valid wherever a
// boolean is expected (maskPredicatePatterns), and never as a value of its
// own in a projection or an updating expression (Neo4j 5.26.30, #907).
func validatePatternExpressionPlacement(cypher string, inputs ...*semanticStatementInput) error {
	clauses, ok := sharedSemanticClauses(cypher, inputs...)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseReturn, pipelineClauseWith:
			keyword := "RETURN"
			if clause.kind == pipelineClauseWith {
				keyword = "WITH"
			}
			for _, item := range splitTopLevelComma(projectionSemanticBody(clause.text, keyword, clause.syntax)) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
				if containsIllegalProjectedPatternExpression(maskPredicatePatterns(expression)) {
					return invalidPatternExpressionPlacementError()
				}
			}
		case pipelineClauseSet:
			if containsIllegalProjectedPatternExpression(maskPredicatePatterns(clause.text)) {
				return invalidPatternExpressionPlacementError()
			}
		}
	}
	return nil
}

// containsIllegalProjectedPatternExpression distinguishes a raw pattern value
// from a legal pattern comprehension. It recursively examines ordinary list
// expressions while treating `[pattern | projection]` as one valid scalar
// expression. Relationship brackets (`-[r]->`) are not list delimiters.
func containsIllegalProjectedPatternExpression(expression string) bool {
	expression = maskSubqueryBodies(expression)
	// shortestPath(...) / allShortestPaths(...) are path functions, not
	// legacy pattern expressions: their pattern argument stays valid in value
	// position (RETURN / WITH / SET projections).
	expression = maskPathFunctionCalls(expression)
	segmentStart := 0
	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' || (index > 0 && expression[index-1] == '-') {
			continue
		}
		close := findMatchingBracket(expression, index)
		if close < 0 {
			continue
		}
		if containsRelationshipPattern(expression[segmentStart:index]) {
			return true
		}
		candidate := expression[index : close+1]
		if _, _, patternComprehension := splitPatternComprehension(candidate); !patternComprehension {
			if containsIllegalProjectedPatternExpression(expression[index+1 : close]) {
				return true
			}
		}
		index = close
		segmentStart = close + 1
	}
	return containsRelationshipPattern(expression[segmentStart:])
}

// maskPathFunctionCalls blanks the argument spans of shortestPath(...) and
// allShortestPaths(...) so pattern scanners do not mistake their relationship
// syntax for a legacy pattern expression. The returned string keeps its byte
// length so every caller index stays valid.
func maskPathFunctionCalls(expression string) string {
	// Zero-allocation fast path: ordinary projections contain neither call.
	if indexASCIIFold(expression, "shortestpath") < 0 && indexASCIIFold(expression, "allshortestpaths") < 0 {
		return expression
	}
	masked := []byte(expression)
	for _, name := range []string{"shortestpath", "allshortestpaths"} {
		searchStart := 0
		for searchStart < len(expression) {
			idx := indexASCIIFold(expression[searchStart:], name)
			if idx < 0 {
				break
			}
			idx += searchStart
			if !functionNameStartsAt(expression, idx) {
				searchStart = idx + 1
				continue
			}
			openParen := skipSpaces(expression, idx+len(name))
			if openParen >= len(expression) || expression[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(expression, openParen)
			if closeParen < 0 {
				break
			}
			for position := idx; position <= closeParen; position++ {
				if masked[position] != '\n' && masked[position] != '\r' {
					masked[position] = ' '
				}
			}
			searchStart = closeParen + 1
		}
	}
	return string(masked)
}

// maskPredicatePatterns blanks, keeping byte offsets, every pattern in
// expression that stands where a boolean is expected, which Neo4j allows
// anywhere: the argument of exists(), an operand of NOT, AND, OR or XOR, a
// CASE WHEN condition, or the WHERE of a list comprehension or quantifier.
// A pattern left is a value (RETURN (n)-->() AS x).
func maskPredicatePatterns(expression string) string {
	var masked []byte
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
			continue
		case '(':
		default:
			continue
		}
		if index > 0 && expression[index-1] == '[' { // a pattern comprehension
			continue
		}
		chainEnd, chain := relationshipChainEnd(expression, index, len(expression))
		if !chain {
			continue
		}
		if patternInBooleanPosition(expression, index, chainEnd) {
			if masked == nil {
				masked = []byte(expression)
			}
			for position := index; position < chainEnd; position++ {
				masked[position] = ' '
			}
		}
		index = chainEnd - 1
	}
	if masked == nil {
		return expression
	}
	return string(masked)
}

// patternInBooleanPosition reports whether the pattern expression[start:end]
// is a boolean operand: inside exists(...), or between a boolean keyword
// (NOT, AND, OR, XOR, WHEN, WHERE, or the start of the expression) and a
// boolean boundary (AND, OR, XOR, THEN, |, a closing parenthesis, or the
// end), with grouping parentheses around it allowed. A whole projection that
// is only the pattern is a value.
func patternInBooleanPosition(expression string, start, end int) bool {
	before := strings.TrimRight(expression[:start], " \t\r\n")
	after := strings.TrimLeft(expression[end:], " \t\r\n")
	for strings.HasSuffix(before, "(") && strings.HasPrefix(after, ")") {
		call := strings.TrimRight(before[:len(before)-1], " \t\r\n")
		if name := trailingIdentifier(call); name != "" && !isBooleanKeyword(name) {
			return strings.EqualFold(name, "exists")
		}
		before = call
		after = strings.TrimLeft(after[1:], " \t\r\n")
	}
	leftKeyword := ""
	switch name := trailingIdentifier(before); {
	case before == "":
	case isBooleanKeyword(name):
		leftKeyword = name
	default:
		return false
	}
	rightBoundary := after == "" || strings.HasPrefix(after, ")") || strings.HasPrefix(after, "|")
	for _, keyword := range []string{"AND", "OR", "XOR", "THEN"} {
		if matchKeywordAt(after, 0, keyword) {
			rightBoundary = true
		}
	}
	if !rightBoundary {
		return false
	}
	// The pattern alone, with nothing before it, is a projected value unless
	// a boolean operator follows it.
	return leftKeyword != "" || (after != "" && !strings.HasPrefix(after, ")"))
}

// isBooleanKeyword reports whether name introduces a boolean operand: NOT,
// AND, OR, XOR, WHEN or WHERE.
func isBooleanKeyword(name string) bool {
	switch upperASCII(name) {
	case "NOT", "AND", "OR", "XOR", "WHEN", "WHERE":
		return true
	}
	return false
}

// trailingIdentifier is the identifier text ends with, or "".
func trailingIdentifier(text string) string {
	end := len(text)
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	return text[start:end]
}

func invalidPatternExpressionPlacementError() error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"UnexpectedSyntax",
		"pattern expressions are only valid as predicates in WHERE",
	)
}

func validateExistsSubqueryClauseComposition(cypher string) error {
	for index := 0; index < len(cypher); index++ {
		name, next, ok := scanIdentifierToken(cypher, index)
		if !ok || !strings.EqualFold(name, "exists") {
			continue
		}
		open := skipSpaces(cypher, next)
		if open >= len(cypher) || cypher[open] != '{' {
			continue
		}
		close := findMatchingDelimiter(cypher, open, '{', '}')
		if close < 0 {
			continue
		}
		body := cypher[open+1 : close]
		if clauses, parsed := splitPipelineClauses(body); parsed {
			for _, clause := range clauses {
				switch clause.kind {
				case pipelineClauseCreate, pipelineClauseDelete, pipelineClauseMerge, pipelineClauseRemove, pipelineClauseSet:
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"InvalidClauseComposition",
						"existential subqueries cannot contain updating clauses",
					)
				}
			}
		}
		index = close
	}
	return nil
}

func (e *StorageExecutor) duplicateReturnColumnName(cypher string, names *quotedVariableNames) string {
	returnPositions := findAllTopLevelPipelineKeywordPositions(cypher, "RETURN")
	unionPositions := append(
		findAllTopLevelPipelineKeywordPositions(cypher, "UNION"),
		findAllTopLevelPipelineKeywordPositions(cypher, "UNION ALL")...,
	)
	for returnIndex, position := range returnPositions {
		end := len(cypher)
		if returnIndex+1 < len(returnPositions) && returnPositions[returnIndex+1] < end {
			end = returnPositions[returnIndex+1]
		}
		for _, unionPosition := range unionPositions {
			if unionPosition > position && unionPosition < end {
				end = unionPosition
			}
		}
		body := strings.TrimSpace(cypher[position+len("RETURN") : end])
		if names != nil {
			body = strings.TrimSpace(names.originalText(position+len("RETURN"), end))
		}
		seen := make(map[string]struct{})
		for _, item := range e.parseReturnItems(body) {
			name := item.alias
			if name == "" {
				name = strings.TrimSpace(item.expr)
			}
			if name == "" || name == "*" {
				continue
			}
			// Cypher identifiers and projected column names are case-sensitive.
			// Only an exact duplicate is a conflict; for example n.Name and
			// n.name are distinct result columns.
			if _, exists := seen[name]; exists {
				return name
			}
			seen[name] = struct{}{}
		}
	}
	return ""
}

func invalidAggregationInListComprehension(cypher string, inputs ...*semanticStatementInput) bool {
	// A missing native input is the admission owner's proved absence of local
	// syntax, not permission to reinterpret every relationship bracket.
	syntax := matchingSemanticInput(cypher, inputs...).syntax()
	if syntax == nil || syntax.Err != nil {
		return false
	}
	for _, scope := range syntax.Input.Scopes() {
		if scope.Kind != fabric.ExpressionLocalScope || scope.Function != "" {
			continue
		}
		for _, span := range []fabric.ExpressionSpan{scope.Predicate, scope.Projection} {
			if span.End > span.Start && containsAggregateFunc(syntax.Input.Slice(span.Start, span.End).Text()) {
				return true
			}
		}
	}
	return false
}
