package cypher

import (
	"context"
	"strings"

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
	if shape, found := scalarExpressionSyntaxCache.get(expression); found {
		return shape, ctx.Err()
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, expression, cypherantlr.ExpressionEntry, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return scalarExpressionSyntax{}, err
	}
	return describeScalarSyntax(ctx, syntax.Root())
}

func describeScalarSyntax(ctx context.Context, node cypherantlr.SyntaxNode) (scalarExpressionSyntax, error) {
	expression := node.Text()
	if shape, found := scalarExpressionSyntaxCache.get(expression); found {
		return shape, ctx.Err()
	}
	var shape scalarExpressionSyntax
	node.Walk(scalarAdmissionVisitor(&shape))
	if err := ctx.Err(); err != nil {
		return shape, err
	}
	scalarExpressionSyntaxCache.put(expression, shape)
	return shape, nil
}

// scalarAdmissionVisitor observes the same product as binding and graph effects.
// It records forbidden structure and aggregate atoms without evaluating a value.
func scalarAdmissionVisitor(shape *scalarExpressionSyntax) cypherantlr.SyntaxVisitor {
	recordAggregate := func(text string) {
		if shape.aggregates == nil {
			shape.aggregates = make(map[string]struct{})
		}
		shape.aggregates[text] = struct{}{}
	}
	return func(node cypherantlr.SyntaxNode) func() {
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist,
			cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery,
			cypherantlr.CypherParserRULE_patternComprehension, cypherantlr.CypherParserRULE_relationshipsChainPattern:
			shape.forbidden = true
		case cypherantlr.CypherParserRULE_countAll:
			recordAggregate(node.Code())
		case cypherantlr.CypherParserRULE_functionInvocation:
			text := node.Code()
			if _, _, _, aggregate := parsePipelineAggregate(text); aggregate {
				recordAggregate(text)
			}
		}
		return nil
	}
}

// classifyTransactionalBatchExpression is the single semantic admission point
// for execution and EXPLAIN. The shared type checker propagates parameter list
// element types; union results such as CASE and coalesce remain runtime values.
func (e *StorageExecutor) classifyTransactionalBatchExpression(ctx context.Context, spec *transactionBatchSpec, params map[string]interface{}) error {
	var shape scalarExpressionSyntax
	var err error
	if syntax, found := spec.ExpressionSyntax(); found {
		shape, err = describeScalarSyntax(ctx, syntax)
	} else {
		shape, err = describeScalarExpression(ctx, spec.Expression)
	}
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
	if operand.excludes(isIntegerOrNumberTypeName) {
		return callBatchSizeError(operand.display, "Neo.ClientError.Statement.SyntaxError")
	}
	if params != nil {
		checker.params = params
		operand, err = checker.check(spec.Expression)
		if err != nil {
			return err
		}
		if operand.excludes(isNumericTypeName) {
			return callBatchSizeError(operand.display, "Neo.ClientError.Statement.SyntaxError")
		}
	}
	if spec.LiteralKind != "" {
		_, err = e.resolveCallBatchSize(ctx, spec, nil)
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
