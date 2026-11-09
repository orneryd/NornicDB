package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 31 October 2026.

// Pin the distinct reload, not the earlier SSA value proven nil at creation.
func routeHeld(symbol string) []string {
	return []string{"load of field ctx of *capture.fixture.request (+5 in " + symbol + ")"}
}

// routeRecord is a record ending the route at the caller that passes its own
// context.
func routeRecord(src, symbol string) ReviewedHeldValue {
	const passes = "func (e *Engine) passes(ctx context.Context) error { return e.fallback(request{ctx: ctx}) }"
	return ReviewedHeldValue{
		EndsEdges: []RouteEdge{{Caller: "(*capture.fixture.Engine).passes", Callee: symbol, Reason: "passes its own context"}},
		Held:      routeHeld(symbol),
		Sources:   map[string]string{symbol: routeDigest(src), "(*capture.fixture.Engine).passes": routeDigest(passes)},
		Reason:    "the named caller passes its own context",
		Approver:  "control",
	}
}

// routeBoundaryRecord ends the walk only where the meaning is established, the
// drain's own call; the 8d45 records ended it at inner, pinning two levels.
func routeBoundaryRecord(src, symbol string) ReviewedHeldValue {
	const drain = "func (e *Engine) drain() error { return e.middle() }"
	return ReviewedHeldValue{
		EndsEdges: []RouteEdge{{Caller: "(*capture.fixture.Engine).drain", Callee: "(*capture.fixture.Engine).middle", Reason: "runs work that is no request's", Kind: BodyBoundary}},
		Held:      routeHeld(symbol),
		Sources:   map[string]string{symbol: routeDigest(src), "(*capture.fixture.Engine).drain": routeDigest(drain)},
		Reason:    "the boundary is the drain's own body",
		Approver:  "control",
	}
}
