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

// extractPaginationExpressions consumes the same semantic effect owner as
// execution, including prefixes, UNION branches and every nested expression.
func extractPaginationExpressions(ctx context.Context, cypher string) []paginationExpression {
	if !containsFold(cypher, "SKIP") && !containsFold(cypher, "LIMIT") {
		return nil
	}
	effects, err := pipelineStatementEffectsFor(ctx, cypher)
	if err != nil {
		return nil
	}
	return effects.pagination
}

// validateStaticPaginationExpression checks one SKIP / LIMIT expression when
// the statement is compiled: it must not use a row variable, and unless it
// reads a parameter it must be a non-negative integer. WITH, RETURN and a
// procedure call's YIELD share it.
func (e *StorageExecutor) validateStaticPaginationExpressions(ctx context.Context, expressions ...paginationExpression) error {
	for _, pagination := range expressions {
		keyword, expression := pagination.keyword, pagination.value
		if expression == "" {
			return paginationCompileTypeError(keyword, nil)
		}
		// The shared structural classifier rejects graph-dependent scalar
		// arguments before the pagination validator can evaluate any expression.
		shape, err := describeScalarExpression(ctx, expression)
		if failure := ctx.Err(); failure != nil {
			return failure
		}
		if err != nil {
			return paginationCompileTypeError(keyword, expression)
		}
		if shape.forbidden {
			return paginationCompileTypeError(keyword, expression)
		}
		if paginationExpressionUsesRowVariable(ctx, expression) {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"NonConstantExpression",
				keyword+" requires an expression independent of row variables",
			)
		}
		if strings.Contains(expression, "$") {
			continue
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
	}
	return nil
}

// validateRuntimePaginationClause checks only the projection operator opening
// now. Nested subqueries and later clauses keep their own runtime boundary.
func (e *StorageExecutor) validateRuntimePaginationClause(ctx context.Context, clause string) error {
	if !strings.Contains(clause, "$") {
		return nil
	}
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if topLevelKeywordIndex(clause, keyword) >= 0 {
			if err := e.validateRuntimePaginationExpressions(ctx, paginationExpression{keyword: keyword, value: pipelinePaginationExpression(clause, keyword)}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRuntimePaginationExpressions(ctx context.Context, expressions ...paginationExpression) error {
	for _, pagination := range expressions {
		if !strings.Contains(pagination.value, "$") {
			continue
		}
		params := getParamsFromContext(ctx)
		if len(params) == 0 {
			continue
		}
		values := make(pipelineRow, len(params))
		bindParameterRow(ctx, values)
		value, evaluated, err := e.evaluateRowValue(pagination.value, values)
		if err != nil {
			return err
		}
		if !evaluated {
			continue
		} // Preserve the existing ParameterMissing path.
		integer, valid := cypherIntegerValue(value)
		if !valid {
			return newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentType", fmt.Sprintf("%s requires an INTEGER, got %T", pagination.keyword, value))
		}
		if integer < 0 {
			return newSemanticError("Neo.ClientError.Statement.ArgumentError", "NegativeIntegerArgument", pagination.keyword+" requires a non-negative INTEGER")
		}
	}
	return nil
}

// paginationExpressionUsesRowVariable reports whether a SKIP / LIMIT
// expression reads a variable. Parameters and their properties ($p.n), map
// keys, function names and variables the expression binds itself (list
// comprehensions, reduce) are not variables; expressionFreeVariables is the
// one scanner that knows them (#829).
func paginationExpressionUsesRowVariable(ctx context.Context, expression string) bool {
	return len(expressionFreeVariables(ctx, expression)) > 0
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
