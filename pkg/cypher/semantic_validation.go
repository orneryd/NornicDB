package cypher

import (
	"context"
	"strings"
)

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
//
// A statement whose backtick-quoted variables were canonicalized is cached
// under the text the client wrote: statements that differ only in quoting
// canonicalize alike but can validate differently (their column names
// differ).
func (e *StorageExecutor) validateSemanticScopes(ctx context.Context, cypher string) error {
	names := quotedVariableNamesFor(ctx, cypher)
	cacheKey := cypher
	if names != nil {
		cacheKey = names.original
	}
	// A text cached under its own key passed every check here, the lexical
	// one included, so a repeated query skips them all (#823). A rewritten
	// text cached under its original still gets its own lexical check.
	cached := e.semanticValidationCache.contains(cacheKey)
	if cached && names == nil {
		return nil
	}
	if err := validateExpressionLexicalTokens(cypher); err != nil {
		return err
	}
	if cached {
		return nil
	}
	if err := e.validateCallSubqueryScopes(cypher); err != nil {
		return err
	}
	if err := validateStaticQuantifierTypes(cypher); err != nil {
		return err
	}
	if err := validateStaticFunctionArguments(cypher); err != nil {
		return err
	}
	if err := e.validateStaticPaginationExpressions(cypher); err != nil {
		return err
	}
	if err := validateWithProjectionSemantics(cypher); err != nil {
		return err
	}
	if err := validateExistsSubqueryClauseComposition(cypher); err != nil {
		return err
	}
	if err := validatePatternExpressionPlacement(cypher); err != nil {
		return err
	}
	if err := validateStaticPropertyAccessTypes(cypher); err != nil {
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
	if invalidAggregationInListComprehension(cypher) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed inside list-comprehension predicates or projections",
		)
	}
	if err := e.validateDuplicateReturnColumnName(cypher, names); err != nil {
		return err
	}
	if clauses, ok := splitPipelineClauses(cypher); ok {
		for _, clause := range clauses {
			// Subquery bodies are checked as their own statements.
			if whereIndex := topLevelKeywordIndex(clause.text, "WHERE"); whereIndex >= 0 &&
				hasUnexpectedIdentifierAfterNumber(maskSubqueryBodies(clause.text[whereIndex+len("WHERE"):])) {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unexpected identifier in WHERE")
			}
			if clause.kind == pipelineClauseWith && withProjectionHasEmptyItem(clause.text) {
				return emptyProjectionItemError("WITH")
			}
			if clause.kind == pipelineClauseReturn {
				body := strings.TrimSpace(clause.text[len("RETURN"):])
				if err := validateReturnAggregationSemantics(body); err != nil {
					return err
				}
				end := len(body)
				for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
					if index := topLevelKeywordIndex(body, keyword); index >= 0 && index < end {
						end = index
					}
				}
				if index := topLevelKeywordIndex(body, "UNION"); index >= 0 && index < end {
					end = index
				}
				if projectionHasEmptyItem(body[:end]) {
					return emptyProjectionItemError("RETURN")
				}
				items, _ := cutDistinct(strings.TrimSpace(body[:end]))
				for _, item := range splitTopLevelComma(items) {
					expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
					if err := validateExpressionOperandCompleteness(expression); err != nil {
						return err
					}
					if aliasIndex := projectionAliasIndex(item); aliasIndex >= 0 {
						alias := strings.TrimSpace(item[aliasIndex+len("AS"):])
						if simpleSemanticIdentifier(alias) == "" && !(len(alias) >= 2 && alias[0] == '`' && alias[len(alias)-1] == '`') {
							return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: invalid RETURN alias")
						}
					}
					if err := validateKnownFunctionsInExpression(expression); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := e.validateCreateSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateMergeSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateMatchSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateSetSemanticScopes(cypher); err != nil {
		return err
	}
	e.semanticValidationCache.add(cacheKey)
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
func withProjectionHasEmptyItem(clause string) bool {
	body, _ := projectionSemanticBodyAndTail(clause, "WITH")
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

// validateCallSubqueryScopes applies the update-clause scope check
// (validateSetSemanticScopes: SET, REMOVE, DELETE) to the body of every
// top-level CALL { … } and CALL (vars) { … } subquery, recursively for nested
// subqueries. A body sees only what it imports, as in Neo4j: an importing WITH
// at its start, or the variables of CALL (vars), which are checked as a
// leading WITH vars. An undefined variable in a subquery update clause is
// therefore rejected before any route runs the subquery. CALL (*) and a body
// starting with WITH * import the whole outer scope and are not checked here.
func (e *StorageExecutor) validateCallSubqueryScopes(cypher string) error {
	if !containsFold(cypher, "CALL") {
		return nil
	}
	for _, position := range findAllTopLevelPipelineKeywordPositions(cypher, "CALL") {
		index := skipSpaces(cypher, position+len("CALL"))
		imports := ""
		scoped := false
		if index < len(cypher) && cypher[index] == '(' {
			closeParen := findMatchingCallParen(cypher, index)
			if closeParen < 0 {
				continue
			}
			imports = strings.TrimSpace(cypher[index+1 : closeParen])
			scoped = true
			if imports != "" && imports != "*" {
				for _, name := range splitTopLevelComma(imports) {
					if simpleSemanticIdentifier(strings.TrimSpace(name)) == "" {
						return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
							"CALL imports must contain only variable names")
					}
				}
			}
			index = skipSpaces(cypher, closeParen+1)
		}
		if index >= len(cypher) || cypher[index] != '{' {
			continue // a procedure call
		}
		closeBrace := e.findMatchingBrace(cypher, index)
		if closeBrace < 0 {
			continue
		}
		body := strings.TrimSpace(cypher[index+1 : closeBrace])
		if stripped, finishes := stripUnionBranchFinishes(body); finishes {
			body = stripped
		}
		if !scoped && startsWithKeywordFold(body, "WITH") {
			if clauses, ok := splitPipelineClauses(body); ok && len(clauses) > 0 {
				projection, tail := projectionSemanticBodyAndTail(clauses[0].text, "WITH")
				importsOuter := false
				for _, variable := range expressionFreeVariables(projection) {
					if isIdentifierReferenced(cypher[:position], variable) {
						importsOuter = true
					}
				}
				if importsOuter && (strings.TrimSpace(tail) != "" || topLevelKeywordIndex(projection, "AS") >= 0) {
					return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport",
						"Importing WITH must contain only simple references to outside variables")
				}
			}
		}
		if imports == "*" || startsWithKeywordFold(body, "WITH *") {
			continue
		}
		if scoped && imports != "" {
			body = "WITH " + imports + " " + body
		}
		if body == "" {
			continue
		}
		if scoped && imports == "" {
			if err := e.validateMatchSemanticScopes(body); err != nil {
				return err
			}
		}
		if err := e.validateSetSemanticScopes(body); err != nil {
			return err
		}
		if err := e.validateCallSubqueryScopes(body); err != nil {
			return err
		}
	}
	return nil
}

func hasUnexpectedIdentifierAfterNumber(expression string) bool {
	quote := byte(0)
	for index := 0; index < len(expression); index++ {
		if quote != 0 {
			if expression[index] == '\\' {
				index++
			} else if expression[index] == quote {
				quote = 0
			}
			continue
		}
		if expression[index] == '\'' || expression[index] == '"' || expression[index] == '`' {
			quote = expression[index]
			continue
		}
		if expression[index] < '0' || expression[index] > '9' || (index > 0 && isIdentCharByte(expression[index-1])) {
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

func validateWithProjectionSemantics(cypher string) error {
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	for _, clause := range clauses {
		if clause.kind != pipelineClauseWith {
			continue
		}
		body := projectionSemanticBody(clause.text, "WITH")
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
			if !explicitAlias && expression != "*" && simpleSemanticIdentifier(expression) == "" {
				if containsMalformedCreateClauseToken(expression) {
					continue
				}
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"NoExpressionAlias",
					"expressions in WITH must be explicitly aliased",
				)
			}
			name := normalizeProjectionColumnName(alias)
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

// validatePatternExpressionPlacement enforces the openCypher rule that legacy
// pattern expressions are predicates confined to WHERE. They cannot be
// projected as values or embedded in an updating expression.
func validatePatternExpressionPlacement(cypher string) error {
	clauses, ok := splitPipelineClauses(cypher)
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
			for _, item := range splitTopLevelComma(projectionSemanticBody(clause.text, keyword)) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
				if containsIllegalProjectedPatternExpression(expression) {
					return invalidPatternExpressionPlacementError()
				}
			}
		case pipelineClauseSet:
			if containsIllegalProjectedPatternExpression(clause.text) {
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
		close := matchingListBracket(expression, index)
		if close < 0 {
			continue
		}
		if containsRelExistencePattern(expression[segmentStart:index]) {
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
	return containsRelExistencePattern(expression[segmentStart:])
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
			if idx > 0 && isWordChar(expression[idx-1]) {
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
		close := matchingSemanticBrace(cypher, open)
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

func invalidAggregationInListComprehension(cypher string) bool {
	for start := 0; start < len(cypher); start++ {
		if cypher[start] != '[' {
			continue
		}
		end := matchingListBracket(cypher, start)
		if end < 0 {
			continue
		}
		_, _, predicate, projection, comprehension := parseListComprehension(cypher[start+1 : end])
		if comprehension && (containsAggregateFunc(predicate) || containsAggregateFunc(projection)) {
			return true
		}
	}
	return false
}

func matchingListBracket(expression string, start int) int {
	depth := 0
	var quote byte
	escaped := false
	for index := start; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if current == '\\' {
				escaped = true
				continue
			}
			if current == quote {
				quote = 0
			}
			continue
		}
		if strings.ContainsRune("'\"`", rune(current)) {
			quote = current
			continue
		}
		switch current {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}
