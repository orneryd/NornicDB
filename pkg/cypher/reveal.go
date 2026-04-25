package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
)

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
func setRevealOnEngine(eng storage.Engine) func() {
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return func() {}
	}
	be.SetRevealAll(true)
	return func() { be.SetRevealAll(false) }
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
