// Author: #1683 lane (Claude), 2026-10-30 (NOR-148); ported to release/v1.4.4.
package storage

// EventViewForwarder is implemented by an engine wrapper whose storage events,
// entity IDs and committed reads are exactly those of its inner engine:
// the wrapper forwards them and changes none of them. Accounting, tracing,
// replication and WAL wrappers qualify. A wrapper that filters, translates or
// adds uncommitted state must not implement it.
type EventViewForwarder interface {
	EngineUnwrapper
	PreservesEventView()
}

// eventView identifies what an engine view exposes: one physical publisher's
// events and committed rows, behind one chain of namespace translations. A
// fixed array keeps the comparison allocation-free on the query path; a deeper
// chain of namespace translations is left unproven.
type eventView struct {
	publisher  *BadgerEngine
	namespaces [4]string
	depth      int
}

// SameEventView reports whether a and b are proven views of one event stream
// with one ID space and the same committed rows: the same physical publisher
// behind the same namespace translation, reached only through forwarders. An
// engine it cannot resolve proves nothing, so callers keep their conservative
// path for it.
func SameEventView(a, b Engine) bool {
	if a == nil || b == nil {
		return false
	}
	left, ok := resolveEventView(a)
	if !ok {
		return false
	}
	right, ok := resolveEventView(b)
	return ok && left == right
}

func resolveEventView(engine Engine) (eventView, bool) {
	var view eventView
	// Wrapper chains are short; the bound only stops a malformed cycle.
	for step := 0; step < 64; step++ {
		switch e := engine.(type) {
		case *BadgerEngine:
			if e == nil {
				return eventView{}, false
			}
			view.publisher = e
			return view, true
		case *MemoryEngine:
			if e == nil {
				return eventView{}, false
			}
			// MemoryEngine is the physical publisher's in-memory constructor,
			// not a translating view. Identify it without a second accessor.
			view.publisher = e.BadgerEngine
			return view, view.publisher != nil
		case *NamespacedEngine:
			if e == nil || view.depth == len(view.namespaces) {
				return eventView{}, false
			}
			view.namespaces[view.depth] = e.namespace
			view.depth++
			engine = e.GetInnerEngine()
		case EventViewForwarder:
			engine = e.GetInnerEngine()
		default:
			return eventView{}, false
		}
	}
	return eventView{}, false
}

// PreservesEventView declares that WAL adds no event, ID or committed-read view.
func (w *WALEngine) PreservesEventView() {}

// PreservesEventView declares that tracing adds no event, ID or committed-read view.
func (t *TracedEngine) PreservesEventView() {}

var (
	_ EventViewForwarder = (*WALEngine)(nil)
	_ EventViewForwarder = (*TracedEngine)(nil)
)
