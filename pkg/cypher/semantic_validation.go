package cypher

import "strings"

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
func (e *StorageExecutor) validateSemanticScopes(cypher string) error {
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
	if duplicate := e.duplicateReturnColumnName(cypher); duplicate != "" {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"ColumnNameConflict",
			"Multiple RETURN items project the same column name: "+duplicate,
		)
	}
	if clauses, ok := splitPipelineClauses(cypher); ok {
		for _, clause := range clauses {
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
				for _, item := range splitTopLevelComma(strings.TrimSpace(body[:end])) {
					expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
					if err := validateGraphFunctionSemanticTypes(expression, nil); err != nil {
						return err
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
	return e.validateSetSemanticScopes(cypher)
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
			explicitAlias := topLevelKeywordIndex(item, "AS") > 0
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

func (e *StorageExecutor) duplicateReturnColumnName(cypher string) string {
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
