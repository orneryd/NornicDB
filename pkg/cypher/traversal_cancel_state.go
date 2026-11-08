package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cancelprobe"
)

// traversalCancelled caches the context channel once for the DFS state. A
// nonblocking channel probe preserves prompt frame/sibling cancellation without
// a Context.Err interface call and its context mutex on every candidate. Once
// observed, cancellation stays latched while every ancestor unwinds.
//
// Only a cancellation of the traversal's own context stops it. The running
// statement's parent is probed too, but it stops the traversal only when the
// traversal's context inherits the statement's cancellation, which its Err
// then reports at once: a context detached from the statement
// (context.WithoutCancel) keeps running after the statement has ended.
func (state *TraversalContext) traversalCancelled() bool {
	if state.cancellationObserved {
		return true
	}
	if !state.cancelInitialized {
		state.cancelInitialized = true
		state.cancelDone = state.cancelCtx.Done()
		// statementContext relays parent cancellation asynchronously. Observe
		// its parent directly so a delayed callback cannot exhaust the frontier.
		if statement, ok := state.cancelCtx.Value(statementContextKey{}).(*statementContext); ok {
			state.cancelParentDone = statement.parent.Done()
		}
		if err := state.cancelCtx.Err(); err != nil {
			return state.observeTraversalCancellation(err)
		}
	}
	// Short-circuit calls preserve two cheap single-channel probes, without
	// introducing a general multi-channel scheduler select on every edge.
	if cancelprobe.Ready(state.cancelDone) {
		return state.observeTraversalCancellation(state.cancelCtx.Err())
	}
	if cancelprobe.Ready(state.cancelParentDone) {
		if err := state.cancelCtx.Err(); err != nil {
			return state.observeTraversalCancellation(err)
		}
		// Detached from the statement: its end is not this traversal's.
		state.cancelParentDone = nil
	}
	return false
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
// becomes a successful statement result on either evaluator/transaction route:
// every observed cancellation records an error.
func (state *TraversalContext) observeTraversalCancellation(err error) bool {
	state.cancellationObserved = true
	if err == nil {
		err = context.Canceled
	}
	recordExpressionFailure(state.cancelCtx, err)
	return true
}
