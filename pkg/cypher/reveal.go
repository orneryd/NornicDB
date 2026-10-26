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
	for index := 0; index < len(query); {
		if end, skipped := skipCypherLexicalToken(query, index); skipped {
			index = end
			continue
		}

		name, next, ok := scanIdentifierToken(query, index)
		if !ok {
			index++
			continue
		}
		if isRevealFunctionName(name) && (index == 0 || query[index-1] != '.') {
			cursor := queryGapEnd(query, next)
			if cursor < len(query) && query[cursor] == '(' {
				return true
			}
		}
		index = next
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
	var member *transactionStorageWrapper
	if binding := storage.MembershipForEngine(eng); binding.Status == storage.MembershipBound {
		member, _ = storage.OwnedCapability[*transactionStorageWrapper](binding.ReadLease)
	}
	if member != nil {
		ctx = member.borrowReadLease(ctx)
	}
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return ctx, func() {}, nil
	}
	if scope, ok := ctx.Value(revealScopeKey{}).(*revealScopeState); ok && scope.engine == be {
		if member != nil {
			return ctx, member.bindReadLease(ctx, func() {}), nil
		}
		return ctx, func() {}, nil
	}
	failure, hasFailure := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	if hasFailure {
		failure.mu.Lock()
		activeEngine := failure.readScopeEngine
		activeContext := failure.readScopeContext
		failure.mu.Unlock()
		if activeEngine == be {
			ctx = storage.InheritQueryRevealScopes(ctx, activeContext)
			if member != nil {
				return ctx, member.bindReadLease(ctx, func() {}), nil
			}
			return ctx, func() {}, nil
		}
	}
	ctx, cleanup := storage.BeginQueryRevealScopeContext(ctx, eng, reveal)
	if member != nil {
		cleanup = member.bindReadLease(ctx, cleanup)
	}
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
	badger, ok := storage.OwnedCapability[*storage.BadgerEngine](storage.UnwrapEngine(eng))
	if !ok {
		return nil
	}
	return badger
}
