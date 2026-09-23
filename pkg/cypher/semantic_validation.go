package cypher

import "strings"

// validateSemanticScopes is the shared compile-time semantic chokepoint used
// by top-level statements and internally executed query branches.
func (e *StorageExecutor) validateSemanticScopes(cypher string) error {
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
	if err := e.validateMatchSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateCreateSemanticScopes(cypher); err != nil {
		return err
	}
	if err := e.validateMergeSemanticScopes(cypher); err != nil {
		return err
	}
	return e.validateSetSemanticScopes(cypher)
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
