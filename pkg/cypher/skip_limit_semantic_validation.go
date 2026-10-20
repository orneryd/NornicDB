package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/fabric"
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
func extractPaginationExpressions(cypher string, inputs ...*semanticStatementInput) []paginationExpression {
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		var result []paginationExpression
		for _, branch := range branches {
			result = append(result, extractPaginationExpressions(branch)...)
		}
		return result
	}
	clauses, ok := sharedSemanticClauses(cypher, inputs...)
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

func (e *StorageExecutor) validateStaticPaginationExpressions(cypher string, inputs ...*semanticStatementInput) error {
	if len(inputs) > 0 && inputs[0] != nil {
		if syntax := inputs[0].syntax(); syntax != nil && syntax.Err == nil {
			return e.validateOwnedPagination(syntax)
		}
	}

	braces := newSubqueryBraceScanner(cypher, 0)
	// Check original subquery bodies before imported variables are replaced
	// with their row values. A substituted integer is still a row dependency.
	for index := 0; index < len(cypher); index++ {
		switch cypher[index] {
		case '\'', '"', '`':
			index = skipCypherQuotedText(cypher, index, cypher[index]) - 1
			continue
		case '/':
			if end := queryCommentEnd(cypher, index); end > index {
				index = end - 1
				continue
			}
		case '{':
			if braces.at(index) {
				if close := findMatchingDelimiter(cypher, index, '{', '}'); close >= 0 {
					if err := e.validateStaticPaginationExpressions(cypher[index+1 : close]); err != nil {
						return err
					}
					index = close
				}
			}
		}
	}
	for _, pagination := range extractPaginationExpressions(cypher, inputs...) {
		if err := e.validateStaticPaginationExpression(pagination.keyword, pagination.value); err != nil {
			return err
		}
	}
	return nil
}

// validateStaticPaginationExpression checks one SKIP / LIMIT expression when
// the statement is compiled: it must not use a row variable, and unless it
// reads a parameter it must be a non-negative integer. WITH, RETURN and a
// procedure call's YIELD share it.
func (e *StorageExecutor) validateStaticPaginationExpression(keyword, expression string, inputs ...fabric.ExpressionInput) error {
	if expression == "" {
		return paginationCompileTypeError(keyword, nil)
	}
	if paginationExpressionUsesRowVariable(expression, inputs...) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"NonConstantExpression",
			keyword+" requires an expression independent of row variables",
		)
	}
	if strings.Contains(expression, "$") {
		return nil
	}
	value, evaluated, err := e.evaluateRowValue(expression, pipelineRow{})
	if err != nil {
		return err
	}
	if !evaluated {
		return paginationCompileTypeError(keyword, value)
	}
	integer, valid := cypherIntegerValue(value)
	if !valid {
		return paginationCompileTypeError(keyword, value)
	}
	if integer < 0 {
		return paginationCompileNegativeError(keyword)
	}
	return nil
}

func (e *StorageExecutor) validateRuntimePaginationExpressions(ctx context.Context, cypher string) error {
	if !strings.Contains(cypher, "$") || (!containsFold(cypher, "SKIP") && !containsFold(cypher, "LIMIT")) {
		return nil
	}
	params := getParamsFromContext(ctx)
	if len(params) == 0 {
		return nil
	}
	values := make(pipelineRow, len(params))
	bindParameterRow(ctx, values)
	for _, pagination := range extractPaginationExpressions(cypher) {
		if !strings.Contains(pagination.value, "$") {
			continue
		}
		value, evaluated, err := e.evaluateRowValue(pagination.value, values)
		if err != nil {
			return err
		}
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

// paginationExpressionUsesRowVariable reports whether a SKIP / LIMIT
// expression reads a variable. Parameters and their properties ($p.n), map
// keys, function names and variables the expression binds itself (list
// comprehensions, reduce) are not variables; expressionFreeVariables is the
// one scanner that knows them (#829).
func paginationExpressionUsesRowVariable(expression string, inputs ...fabric.ExpressionInput) bool {
	// Literal values have no variable operand. Reuse the default parser's
	// value owner; no native symbolic-name acquisition is needed for them.
	if _, literal := parseLiteralValueFromComputedRow(expression); literal {
		return false
	}
	return len(expressionFreeVariables(expression, inputs...)) > 0
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

// validateOwnedPagination traverses retained query descriptors. CALL, UNION
// and expression-query bodies never reconstruct a statement from sliced text.
func (e *StorageExecutor) validateOwnedPagination(root *fabric.QueryStructure) error {
	seen := make(map[fabric.ExpressionInput]bool)
	var visit func(*fabric.QueryStructure) error
	visit = func(query *fabric.QueryStructure) error {
		if seen[query.Input] {
			return nil
		}
		seen[query.Input] = true
		for _, clause := range query.Clauses {
			for _, operand := range clause.PaginationOperands() {
				if err := e.validateStaticPaginationExpression(operand.Pagination, operand.Input.Text(), operand.Input); err != nil {
					return err
				}
			}
			if clause.Nested != nil {
				if err := visit(clause.Nested); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return err
	}
	for _, nested := range root.Input.QueryScopes() {
		if err := visit(nested); err != nil {
			return err
		}
	}
	return nil
}
