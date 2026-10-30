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
		// statementContext relays parent cancellation asynchronously. Observe
		// its parent directly so a delayed callback cannot exhaust the frontier.
		if statement, ok := state.cancelCtx.Value(statementContextKey{}).(*statementContext); ok {
			state.cancelParentDone = statement.parent.Done()
		}
		if state.cancelCtx.Err() != nil {
			return state.observeTraversalCancellation()
		}
	}
	select {
	case <-state.cancelDone:
		return state.observeTraversalCancellation()
	default:
	}
	// Separate nonblocking receives retain Go's cheap single-channel probe;
	// a multi-case select would enter the general scheduler on every edge.
	select {
	case <-state.cancelParentDone:
		return state.observeTraversalCancellation()
	default:
		return false
	}
}

// memoryAccount is the traversal's account in the statement's memory
// scope; it tracks nothing when no limit is configured.
func (state *TraversalContext) memoryAccount() *memoryAccount {
	if !state.memoryResolved {
		state.memoryResolved = true
		state.memory.scope = memoryScopeFromContext(state.cancelCtx)
	}
	return &state.memory
}

// Cancellation is an expression failure, so a partially built path set never
// becomes a successful statement result on either evaluator/transaction route.
func (state *TraversalContext) observeTraversalCancellation() bool {
	state.cancellationObserved = true
	if err := state.cancelCtx.Err(); err != nil {
		recordExpressionFailure(state.cancelCtx, err)
	}
	return true
}
