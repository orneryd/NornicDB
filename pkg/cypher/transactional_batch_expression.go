package cypher

import (
	"context"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type scalarExpressionSyntax struct {
	forbidden  bool
	aggregates map[string]struct{}
}

var scalarExpressionSyntaxCache = newBoundedCache[string, scalarExpressionSyntax](4096, boundedCacheStringShardIndex)

// describeScalarExpression classifies the complete grammar tree,
// including constructs hidden inside CASE branches and function arguments.
// Structural admission and result types are separate from runtime values.
func describeScalarExpression(ctx context.Context, expression string) (scalarExpressionSyntax, error) {
	if shape, ok := scalarExpressionSyntaxCache.get(expression); ok {
		return shape, ctx.Err()
	}
	var shape scalarExpressionSyntax
	recordAggregate := func(text string) {
		if shape.aggregates == nil {
			shape.aggregates = make(map[string]struct{})
		}
		shape.aggregates[text] = struct{}{}
	}
	err := cypherantlr.ReadExpressionSyntax(ctx, expression, func(tree antlr4.Tree, source []rune) {
		walkCypherSyntax(tree, func(node antlr4.Tree) func() {
			switch n := node.(type) {
			case *cypherantlr.ExistsSubqueryContext, *cypherantlr.SubqueryExistContext,
				*cypherantlr.CountSubqueryContext, *cypherantlr.CollectSubqueryContext,
				*cypherantlr.PatternComprehensionContext, *cypherantlr.RelationshipsChainPatternContext:
				shape.forbidden = true
			case *cypherantlr.CountAllContext:
				recordAggregate(cypherantlr.SyntaxText(n, source))
			case *cypherantlr.FunctionInvocationContext:
				text := cypherantlr.SyntaxText(n, source)
				if _, _, _, aggregate := parsePipelineAggregate(text); aggregate {
					recordAggregate(text)
				}
			}
			return nil
		})
	})
	if err == nil {
		scalarExpressionSyntaxCache.put(expression, shape)
	}
	return shape, err
}

// classifyTransactionalBatchExpression is the single semantic admission point
// for execution and EXPLAIN. The shared type checker propagates parameter list
// element types; union results such as CASE and coalesce remain runtime values.
func (e *StorageExecutor) classifyTransactionalBatchExpression(ctx context.Context, spec *transactionBatchSpec, params map[string]interface{}) error {
	shape, err := describeScalarExpression(ctx, spec.Expression)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", err.Error())
	}
	if shape.forbidden || len(expressionFreeVariables(spec.Expression)) != 0 {
		return callBatchSizeError(spec.Expression, "Neo.ClientError.Statement.SyntaxError")
	}
	checker := staticOperatorChecker{scope: staticTypeScope{cypher25: cypherVersionFromContext(ctx) == "25"}}
	operand, err := checker.check(spec.Expression)
	if err != nil {
		return err
	}
	if operand.kind == "Float" || (operand.known() && !operand.numeric() && operand.kind != "Null") {
		return callBatchSizeError(operand.display, "Neo.ClientError.Statement.SyntaxError")
	}
	if params != nil {
		checker.params = params
		operand, err = checker.check(spec.Expression)
		if err != nil {
			return err
		}
		if operand.known() && !operand.numeric() && operand.kind != "Null" {
			return callBatchSizeError(operand.display, "Neo.ClientError.Statement.SyntaxError")
		}
	}
	if spec.LiteralKind != "" {
		_, err = e.resolveCallBatchSize(ctx, spec)
	}
	return err
}

func transactionalBatchAggregateError() error {
	return localizedStatusError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidAggregation", localization.CypherSubqueriesBatchAggregateInvalid())
}

const scalarExpressionSyntaxKey = "\x00scalarExpressionSyntax"

// scalarExpressionEvaluationError is the scalar evaluator's policy adapter.
// The admitted syntax is immutable. The existing evaluator selects branches
// and calls this only when it reaches an aggregate atom at its fallback entry.
func scalarExpressionEvaluationError(expression string, values map[string]interface{}) error {
	shape, constrained := values[scalarExpressionSyntaxKey].(scalarExpressionSyntax)
	if constrained {
		if _, reached := shape.aggregates[strings.TrimSpace(expression)]; reached {
			return transactionalBatchAggregateError()
		}
	}
	return nil
}
