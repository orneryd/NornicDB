package cypher

import (
	"context"
	"errors"
	"fmt"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// transactionBatchSpec is the shared grammar's immutable syntax. This module
// owns its admission, error classification and runtime binding for every route.
type transactionBatchSpec = cypherantlr.TransactionBatch

type boundTransactionBatch struct {
	size int
}

var transactionalStatementReads = newBoundedCache[string, []cypherantlr.CallClause](4096, boundedCacheStringShardIndex)
var callClauseReads = newBoundedCache[string, cypherantlr.CallClause](4096, boundedCacheStringShardIndex)

func readTransactionalStatement(ctx context.Context, query string) ([]cypherantlr.CallClause, error) {
	// Only a cheap candidate filter: all classification and boundaries below
	// come from the shared tokens/grammar, including nested expression calls.
	if !containsFold(query, "CALL") {
		return nil, nil
	}
	if calls, ok := transactionalStatementReads.get(query); ok {
		return calls, ctx.Err()
	}
	calls, err := cypherantlr.ReadTransactionalCalls(ctx, query)
	if err == nil {
		transactionalStatementReads.put(query, calls)
	}
	return calls, err
}

func readCallClause(ctx context.Context, clause string) (cypherantlr.CallClause, error) {
	if call, ok := callClauseReads.get(clause); ok {
		return call, ctx.Err()
	}
	call, err := cypherantlr.ReadCallClause(ctx, clause)
	if err == nil {
		callClauseReads.put(clause, call)
	}
	return call, err
}

// validateTransactionalCalls belongs to statement semantics, before EXPLAIN
// and execution diverge and before literal-normalized semantic cache hits.
func (e *StorageExecutor) validateTransactionalCalls(ctx context.Context, query string, params map[string]interface{}) error {
	calls, err := readTransactionalStatement(ctx, query)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", err.Error())
	}
	for _, call := range calls {
		if err := validateCallSubqueryBody(call.Body); err != nil {
			return err
		}
		spec := call.Batch
		if spec == nil {
			continue
		}
		reason := ""
		switch {
		case call.PlacementError == "nested":
			reason = "transactional CALL cannot be nested in another subquery"
		case call.PlacementError == "union":
			reason = "transactional CALL cannot appear in a UNION query"
		case call.PlacementError == "write":
			reason = "transactional CALL cannot follow a write in the containing transaction"
		case spec.Concurrent:
			reason = "concurrent transactional CALL is not supported"
		case spec.ErrorMode != "" || spec.ReportVariable != "":
			reason = "transactional CALL ON ERROR and REPORT STATUS are not supported"
		}
		if reason != "" {
			return transactionalCallContextError(reason)
		}
		if err := e.classifyTransactionalBatchExpression(ctx, spec, params); err != nil {
			return err
		}
	}
	return nil
}

func callBatchSizeError(value interface{}, code string) error {
	return localizedStatusError(code, "InvalidArgumentType", localization.CypherSubqueriesBatchSizeInvalid(fmt.Sprint(value)))
}

func validateCallSubqueryBody(body string) error {
	if body == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCallInTransactions", localization.CypherSubqueriesCallBodyExpected())
	}
	return nil
}

// resolveCallBatchSize binds one admitted CALL using the existing evaluator,
// before that CALL can mutate, including when its input is empty. A runtime
// error in a later CALL preserves an earlier CALL's independent commits, as
// Neo4j does. Static errors instead reject the complete statement above.
func (e *StorageExecutor) resolveCallBatchSize(ctx context.Context, spec *transactionBatchSpec) (int, error) {
	shape, err := describeScalarExpression(ctx, spec.Expression)
	if err != nil {
		return 0, err
	}
	values := parameterRowValues(ctx)
	if len(shape.aggregates) != 0 {
		// Parameter rows are shared. Give this scalar evaluation its own
		// immutable syntax policy without changing the cached parameter scope.
		values = rowValuesInExpressionContext(ctx, values)
		values[scalarExpressionSyntaxKey] = shape
	}
	value, resolved, err := e.evaluateRowValue(spec.Expression, values)
	if err != nil {
		return 0, err
	}
	integer, valid := cypherIntegerValue(value)
	if !resolved || !valid || integer <= 0 || uint64(integer) > uint64(^uint(0)>>1) {
		code := "Neo.ClientError.Statement.SyntaxError"
		if spec.LiteralKind == "" {
			code = "Neo.ClientError.Statement.TypeError"
			if isRuntimeNumber(value) {
				code = "Neo.ClientError.Statement.ArgumentError"
			}
		}
		return 0, callBatchSizeError(value, code)
	}
	return int(integer), nil
}

// bindTransactionalCallBatch is also used by direct operator callers. A
// pipeline passes its binding back here, so a size is evaluated once when
// that operator opens, regardless of how many rows consume it.
func (e *StorageExecutor) bindTransactionalCallBatch(ctx context.Context, spec *transactionBatchSpec, bound *boundTransactionBatch) (*boundTransactionBatch, error) {
	if bound != nil {
		return bound, nil
	}
	size, err := e.resolveCallBatchSize(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &boundTransactionBatch{size: size}, nil
}

// validateTransactionalCallInput owns the context rule that depends on the
// admitted CALL's actual input. An empty input does not open a transaction.
func (e *StorageExecutor) validateTransactionalCallInput(count int) error {
	if count == 0 || e.txContext == nil || !e.txContext.active {
		return nil
	}
	if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok && tx.OperationCount() > 0 {
		return &classifiedCypherError{
			cause:  errors.New("Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)"),
			code:   "Neo.DatabaseError.Statement.ExecutionFailed",
			detail: "InvalidCallInTransactions",
		}
	}
	return newSemanticError("Neo.DatabaseError.Transaction.TransactionStartFailed", "InvalidCallInTransactions",
		"CALL { ... } IN TRANSACTIONS is not allowed inside an explicit transaction")
}

func hasCallInTransactions(query string) bool {
	calls, _ := readTransactionalStatement(context.Background(), query)
	for _, call := range calls {
		if call.Batch != nil {
			return true
		}
	}
	return false
}

type boundCallChain struct {
	batches    map[int]*boundTransactionBatch
	pagination map[int]bool
}

func (e *StorageExecutor) bindTransactionalCallChain(ctx context.Context, plan pipelineCallChainPlan) (boundCallChain, error) {
	bound := boundCallChain{batches: make(map[int]*boundTransactionBatch, len(plan.operators))}
	// Open consumers before their inputs, including pagination in this chain.
	for index := len(plan.operators) - 1; index >= 0; index-- {
		operator := plan.operators[index]
		for _, pagination := range operator.pagination {
			if err := e.validateRuntimePaginationExpression(ctx, pagination); err != nil {
				return boundCallChain{}, err
			}
			if bound.pagination == nil {
				bound.pagination = make(map[int]bool)
			}
			bound.pagination[operator.index] = true
		}
		if operator.spec != nil {
			batch, err := e.bindTransactionalCallBatch(ctx, operator.spec, nil)
			if err != nil {
				return boundCallChain{}, err
			}
			bound.batches[operator.index] = batch
		}
	}
	return bound, nil
}

func transactionalCallContextError(reason string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCallInTransactions", localization.CypherSubqueriesBatchContextInvalid(reason))
}
