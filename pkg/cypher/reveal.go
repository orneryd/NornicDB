package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type revealScopeKey struct{}

type revealScopeState struct {
	engine *storage.BadgerEngine
}

// hasRevealCall detects the presence of reveal(...) in the query text.
func hasRevealCall(query string) bool {
	return containsFunctionCallNamed(query, "reveal")
}

func isRevealFunctionName(name string) bool {
	return len(name) == len("reveal") &&
		asciiUpper(name[0]) == 'R' &&
		asciiUpper(name[1]) == 'E' &&
		asciiUpper(name[2]) == 'V' &&
		asciiUpper(name[3]) == 'E' &&
		asciiUpper(name[4]) == 'A' &&
		asciiUpper(name[5]) == 'L'
}

// setRevealOnEngine enables reveal mode on the underlying BadgerEngine.
// Returns a cleanup function that must be deferred.
func setRevealOnEngine(ctx context.Context, eng storage.Engine, reveal bool) (context.Context, func(), *storage.BadgerEngine) {
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return ctx, func() {}, nil
	}
	if scope, ok := ctx.Value(revealScopeKey{}).(*revealScopeState); ok && scope.engine == be {
		return ctx, func() {}, nil
	}
	failure, hasFailure := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	if hasFailure {
		failure.mu.Lock()
		activeEngine := failure.readScopeEngine
		activeContext := failure.readScopeContext
		failure.mu.Unlock()
		if activeEngine == be {
			return storage.InheritQueryRevealScopes(ctx, activeContext), func() {}, nil
		}
	}
	ctx, cleanup := storage.BeginQueryRevealScopeContext(ctx, eng, reveal)
	if hasFailure {
		failure.mu.Lock()
		activeEngine := failure.readScopeEngine
		if activeEngine == nil {
			failure.readScopeEngine = be
			failure.readScopeContext = ctx
			failure.mu.Unlock()
			return ctx, cleanup, be
		}
		failure.mu.Unlock()
		cleanup()
		if activeEngine == be {
			return ctx, func() {}, nil
		}
	}
	ctx = context.WithValue(ctx, revealScopeKey{}, &revealScopeState{engine: be})
	return ctx, cleanup, nil
}

func clearRevealScope(ctx context.Context, engine *storage.BadgerEngine) {
	if engine == nil {
		return
	}
	failure, ok := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	if !ok {
		return
	}
	failure.mu.Lock()
	if failure.readScopeEngine == engine {
		failure.readScopeEngine = nil
		failure.readScopeContext = nil
	}
	failure.mu.Unlock()
}

// unwrapBadgerEngine walks the engine wrapper chain to find the underlying
// BadgerEngine. Returns nil if the chain does not contain one.
func unwrapBadgerEngine(eng storage.Engine) *storage.BadgerEngine {
	badger, ok := storage.UnwrapEngine(eng).(*storage.BadgerEngine)
	if !ok {
		return nil
	}
	return badger
}
