package cypher

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type expressionFailureKey struct{}

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
	mu              sync.Mutex
	err             error
	readScopeEngine *storage.BadgerEngine
}

func recordExpressionFailure(ctx context.Context, err error) {
	if failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure); ok {
		failure.mu.Lock()
		if failure.err == nil {
			failure.err = err
		}
		failure.mu.Unlock()
	}
}

func getExpressionFailure(ctx context.Context) error {
	if failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure); ok {
		failure.mu.Lock()
		defer failure.mu.Unlock()
		return failure.err
	}
	return nil
}
