package cypher

import (
	"context"
	"reflect"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type expressionFailureKey struct{}

// expressionOperandFailureKey isolates only errors while preserving the
// statement's map ordering and REVEAL metadata in expressionFailureKey.
type expressionOperandFailureKey struct{}

// rowFallbackExpressionKey records dispatch ownership independently of the
// result/error contract, so context-only functions evaluate their children
// without handing the same whole expression back to the row evaluator.
type rowFallbackExpressionKey struct{}

// A fallback may call back into the row evaluator through a legacy reader.
// Carry its depth with its existing dispatch state, so a non-progressing read
// fails through the checked contract instead of exhausting the goroutine stack.
type rowFallbackState struct {
	text  string
	depth int
}

const maxRowFallbackDepth = 64

func handedBackByRowEvaluator(ctx context.Context, expr string) bool {
	state, _ := ctx.Value(rowFallbackExpressionKey{}).(rowFallbackState)
	return state.text != "" && logicalOperandKey(state.text) == logicalOperandKey(expr)
}

// deferredExpressionValue adapts a context evaluator to the logical fold's
// explicit error contract. A later deciding operand can discard this error.
func deferredExpressionValue(ctx context.Context, evaluate func(context.Context) interface{}) (interface{}, bool, error) {
	return deferredExpressionResult(ctx, func(ctx context.Context) (interface{}, bool, error) {
		return evaluate(ctx), true, nil
	})
}

func deferredExpressionResult(ctx context.Context, evaluate func(context.Context) (interface{}, bool, error)) (interface{}, bool, error) {
	ctx = context.WithValue(ctx, expressionOperandFailureKey{}, &expressionFailure{})
	value, ok, err := evaluate(ctx)
	if err == nil {
		err = getExpressionFailure(ctx)
	}
	return value, ok, err
}

// withExpressionFailureSlot gives ctx a slot that collects the first
// expression error of the statement (recordExpressionFailure), unless it
// has one: the running statement's own slot when there is one (no
// allocation), else a new one.
func withExpressionFailureSlot(ctx context.Context) context.Context {
	if ctx.Value(expressionFailureKey{}) != nil {
		return ctx
	}
	if statement, ok := ctx.Value(statementContextKey{}).(*statementContext); ok {
		statement.failureActive.Store(true)
		return ctx
	}
	return context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
}

type expressionFailure struct {
	mu               sync.Mutex
	err              error
	readScopeEngine  *storage.BadgerEngine
	readScopeContext context.Context
	mapKeyOrders     map[uintptr][]string
}

func (failure *expressionFailure) recordMapKeyOrder(value map[string]interface{}, keys []string) {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if failure.mapKeyOrders == nil {
		failure.mapKeyOrders = make(map[uintptr][]string)
	}
	failure.mapKeyOrders[uintptr(reflect.ValueOf(value).UnsafePointer())] = keys
}

func snapshotMapKeyOrders(ctx context.Context) map[uintptr][]string {
	failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	if !ok {
		return nil
	}
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if len(failure.mapKeyOrders) == 0 {
		return nil
	}
	orders := make(map[uintptr][]string, len(failure.mapKeyOrders))
	for pointer, keys := range failure.mapKeyOrders {
		orders[pointer] = append([]string(nil), keys...)
	}
	return orders
}

func recordExpressionFailure(ctx context.Context, err error) {
	if failure := expressionErrorSlot(ctx); failure != nil {
		failure.mu.Lock()
		if failure.err == nil {
			failure.err = err
		}
		failure.mu.Unlock()
	}
}

func getExpressionFailure(ctx context.Context) error {
	if failure := expressionErrorSlot(ctx); failure != nil {
		failure.mu.Lock()
		defer failure.mu.Unlock()
		return failure.err
	}
	return nil
}

func expressionErrorSlot(ctx context.Context) *expressionFailure {
	if failure, ok := ctx.Value(expressionOperandFailureKey{}).(*expressionFailure); ok {
		return failure
	}
	failure, _ := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	return failure
}

// rowValuesInExpressionContext overrides the statement context for a deferred
// operand; nested context evaluators must record their errors in that operand.
func rowValuesInExpressionContext(ctx context.Context, values map[string]interface{}) map[string]interface{} {
	scope := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		scope[name] = value
	}
	scope[rowStatementContextKey] = ctx
	return scope
}

// deferredRowOperand keeps a subquery operand's statement errors local until
// the surrounding OR decides whether they count.
func deferredRowOperand(values map[string]interface{}, evaluate func(map[string]interface{}) (interface{}, bool, error)) (interface{}, bool, error) {
	return deferredExpressionResult(rowStatementContextOrBackground(values), func(ctx context.Context) (interface{}, bool, error) {
		return evaluate(rowValuesInExpressionContext(ctx, values))
	})
}

// rowOperandRecordsFailure identifies operations whose adapters record errors
// through the statement context, rather than returning them explicitly.
func rowOperandRecordsFailure(expression string) bool {
	return mayContainSubqueryExpression(expression)
}
