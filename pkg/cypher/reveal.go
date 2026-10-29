package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type revealScopeKey struct{}

type revealScopeState struct {
	engine *storage.BadgerEngine
	reveal bool
}

// executionReadScope binds the live storage read lease to its issued executor
// and transaction storage, just as composite membership and mode observation do.
// A procedure's fresh context must not recursively acquire its owner's mutex.
type executionReadScope struct {
	context context.Context
	engine  *storage.BadgerEngine
	reveal  bool
}

func (e *StorageExecutor) executionReadScopeOf() *executionReadScope {
	scope := e.readScope
	if scope == nil {
		if wrapper := membershipOfStorage(e.storage).storage; wrapper != nil {
			scope = wrapper.readScope.Load()
		}
	}
	if scope != nil && scope.engine == e.propertyDecayEngine {
		return scope
	}
	return nil
}

func (e *StorageExecutor) bindExecutionReadScope(ctx context.Context) (*StorageExecutor, func()) {
	scope, _ := ctx.Value(revealScopeKey{}).(*revealScopeState)
	if scope == nil {
		return e, func() {}
	}
	bound := e.cloneWithStorage(e.storage)
	bound.readScope = &executionReadScope{context: ctx, engine: scope.engine, reveal: scope.reveal}
	if wrapper, ok := bound.storage.(*transactionStorageWrapper); ok {
		previous := wrapper.readScope.Swap(bound.readScope)
		return bound, func() { wrapper.readScope.Store(previous) }
	}
	return bound, func() {}
}

// hasRevealCall detects the presence of reveal(...) in the query text.
func hasRevealCall(query string) bool {
	for index := 0; index < len(query); {
		if end, skipped := skipCypherLexicalToken(query, index); skipped {
			index = end
			continue
		}

		name, ok := cyphertext.ScanIdentifier(query, index)
		if !ok {
			index++
			continue
		}
		if isRevealFunctionName(name.Value) && (index == 0 || query[index-1] != '.') {
			cursor := queryGapEnd(query, name.End)
			if cursor < len(query) && query[cursor] == '(' {
				return true
			}
		}
		index = name.End
	}
	return false
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
		active := failure.readScope
		failure.mu.Unlock()
		if active != nil && active.engine == be {
			return storage.InheritQueryRevealScopes(ctx, active.ctx), func() {}, nil
		}
	}
	ctx, cleanup := storage.BeginQueryRevealScopeContext(ctx, eng, reveal)
	ctx = context.WithValue(ctx, revealScopeKey{}, &revealScopeState{engine: be, reveal: reveal})
	if hasFailure {
		failure.mu.Lock()
		active := failure.readScope
		if active == nil {
			failure.readScope = &revealReadScope{engine: be, ctx: ctx}
			failure.mu.Unlock()
			return ctx, cleanup, be
		}
		failure.mu.Unlock()
		cleanup()
		if active.engine == be {
			return ctx, func() {}, nil
		}
	}
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
	if failure.readScope != nil && failure.readScope.engine == engine {
		failure.readScope = nil
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
