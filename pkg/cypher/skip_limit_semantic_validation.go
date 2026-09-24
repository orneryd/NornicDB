package cypher

import (
	"context"
	"fmt"
	"strings"
)

type paginationExpression struct {
	keyword string
	value   string
}

// extractPaginationExpressions returns the SKIP / LIMIT expressions of the
// statement's WITH and RETURN clauses. Each UNION branch has its own SKIP /
// LIMIT, so the branches are read one by one; otherwise the last RETURN of a
// branch would run into the next branch (LIMIT 2 UNION ALL MATCH …).
func extractPaginationExpressions(cypher string) []paginationExpression {
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		var result []paginationExpression
		for _, branch := range branches {
			result = append(result, extractPaginationExpressions(branch)...)
		}
		return result
	}
	clauses, ok := splitPipelineClauses(cypher)
	if !ok {
		return nil
	}
	result := make([]paginationExpression, 0, 2)
	for _, clause := range clauses {
		if clause.kind != pipelineClauseWith && clause.kind != pipelineClauseReturn {
			continue
		}
		for _, keyword := range []string{"SKIP", "LIMIT"} {
			if topLevelKeywordIndex(clause.text, keyword) < 0 {
				continue
			}
			result = append(result, paginationExpression{
				keyword: keyword,
				value:   pipelinePaginationExpression(clause.text, keyword),
			})
		}
	}
	return result
}

func (e *StorageExecutor) validateStaticPaginationExpressions(cypher string) error {
	for _, pagination := range extractPaginationExpressions(cypher) {
		expression := pagination.value
		if expression == "" {
			return paginationCompileTypeError(pagination.keyword, nil)
		}
		if paginationExpressionUsesRowVariable(expression) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"NonConstantExpression",
				pagination.keyword+" requires an expression independent of row variables",
			)
		}
		if strings.Contains(expression, "$") {
			continue
		}
		value, evaluated := e.evaluateRowExpression(expression, pipelineRow{})
		if !evaluated {
			return paginationCompileTypeError(pagination.keyword, value)
		}
		integer, valid := cypherIntegerValue(value)
		if !valid {
			return paginationCompileTypeError(pagination.keyword, value)
		}
		if integer < 0 {
			return paginationCompileNegativeError(pagination.keyword)
		}
	}
	return nil
}

func (e *StorageExecutor) validateRuntimePaginationExpressions(ctx context.Context, cypher string) error {
	if !strings.Contains(cypher, "$") {
		return nil
	}
	params := getParamsFromContext(ctx)
	if len(params) == 0 {
		return nil
	}
	values := make(pipelineRow, len(params))
	for name, value := range params {
		values["$"+name] = value
	}
	for _, pagination := range extractPaginationExpressions(cypher) {
		if !strings.Contains(pagination.value, "$") {
			continue
		}
		value, evaluated := e.evaluateRowExpression(pagination.value, values)
		if !evaluated {
			// Missing parameters retain the existing ParameterMissing path.
			continue
		}
		integer, valid := cypherIntegerValue(value)
		if !valid {
			return newSemanticError(
				"Neo.ClientError.Statement.ArgumentError",
				"InvalidArgumentType",
				fmt.Sprintf("%s requires an INTEGER, got %T", pagination.keyword, value),
			)
		}
		if integer < 0 {
			return newSemanticError(
				"Neo.ClientError.Statement.ArgumentError",
				"NegativeIntegerArgument",
				pagination.keyword+" requires a non-negative INTEGER",
			)
		}
	}
	return nil
}

func paginationExpressionUsesRowVariable(expression string) bool {
	for index := 0; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' || expression[index] == '`' {
			index = numericValidationSkipQuoted(expression, index)
			continue
		}
		name, next, ok := scanIdentifierToken(expression, index)
		if !ok {
			index++
			continue
		}
		previous := index - 1
		for previous >= 0 && isWhitespace(expression[previous]) {
			previous--
		}
		if previous >= 0 && expression[previous] == '$' {
			index = next
			continue
		}
		after := skipSpaces(expression, next)
		if after < len(expression) && expression[after] == '(' {
			index = next
			continue
		}
		if !strings.EqualFold(name, "true") && !strings.EqualFold(name, "false") && !strings.EqualFold(name, "null") {
			return true
		}
		index = next
	}
	return false
}

func paginationCompileTypeError(keyword string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentType",
		fmt.Sprintf("%s requires an INTEGER, got %T", keyword, value),
	)
}

func paginationCompileNegativeError(keyword string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"NegativeIntegerArgument",
		keyword+" requires a non-negative INTEGER",
	)
}
