package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// hasRevealCall detects the presence of reveal(...) in the query text.
func hasRevealCall(query string) bool {
	upper := strings.ToUpper(query)
	return strings.Contains(upper, "REVEAL(")
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
