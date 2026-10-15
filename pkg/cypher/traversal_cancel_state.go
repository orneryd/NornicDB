package cypher

// traversalCancelled caches the context channel once for the DFS state. A
// nonblocking channel probe preserves prompt frame/sibling cancellation without
// a Context.Err interface call and its context mutex on every candidate. Once
// observed, cancellation stays latched while every ancestor unwinds.
func (state *TraversalContext) traversalCancelled() bool {
	if state.cancellationObserved {
		return true
	}
	if state.cancelCtx == nil {
		return false
	}
	if !state.cancelInitialized {
		state.cancelInitialized = true
		state.cancelDone = state.cancelCtx.Done()
		state.cancellationObserved = state.cancelCtx.Err() != nil
		if state.cancellationObserved {
			return true
		}
	}
	select {
	case <-state.cancelDone:
		state.cancellationObserved = true
		return true
	default:
		return false
	}
}
