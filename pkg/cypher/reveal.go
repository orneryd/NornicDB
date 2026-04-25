package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type revealScopeKey struct{}

type revealScopeState struct {
	engine *storage.BadgerEngine
	reveal bool
}

// hasRevealCall detects the presence of reveal(...) in the query text.
func hasRevealCall(query string) bool {
	for searchFrom := 0; searchFrom < len(query); {
		rel := FindKeywordIndex(query[searchFrom:], "REVEAL")
		if rel < 0 {
			return false
		}
		idx := searchFrom + rel
		pos := idx + len("REVEAL")
		for pos < len(query) && isWhitespace(query[pos]) {
			pos++
		}
		if pos < len(query) && query[pos] == '(' {
			return true
		}
		searchFrom = idx + len("REVEAL")
	}
	return false
}

// setRevealOnEngine enables reveal mode on the underlying BadgerEngine.
// Returns a cleanup function that must be deferred.
func setRevealOnEngine(ctx context.Context, eng storage.Engine, reveal bool) (context.Context, func()) {
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return ctx, func() {}
	}
	if scope, ok := ctx.Value(revealScopeKey{}).(*revealScopeState); ok && scope.engine == be {
		return ctx, func() {}
	}
	cleanup := be.BeginQueryRevealScope(reveal)
	ctx = context.WithValue(ctx, revealScopeKey{}, &revealScopeState{engine: be, reveal: reveal})
	return ctx, cleanup
}

// unwrapBadgerEngine walks the engine wrapper chain to find the underlying
// BadgerEngine. Returns nil if the chain does not contain one.
func unwrapBadgerEngine(eng storage.Engine) *storage.BadgerEngine {
	for {
		switch e := eng.(type) {
		case *storage.BadgerEngine:
			return e
		case interface{ GetInnerEngine() storage.Engine }:
			eng = e.GetInnerEngine()
		case interface{ UnwrapEngine() storage.Engine }:
			eng = e.UnwrapEngine()
		default:
			return nil
		}
	}
}
