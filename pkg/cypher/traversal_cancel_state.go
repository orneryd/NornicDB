package cypher

import "context"

// contextCancelProbe is the nonblocking cancellation probe for hot loops (the
// DFS, nornicdb.similarity.pairs). It caches the context's channel once: a
// channel probe preserves prompt cancellation without a Context.Err
// interface call and its context mutex on every step. The zero value with a
// nil context is never cancelled.
type contextCancelProbe struct {
	ctx         context.Context
	initialized bool
	done        <-chan struct{}
	parentDone  <-chan struct{}
}

// newContextCancelProbe is a probe of ctx.
func newContextCancelProbe(ctx context.Context) contextCancelProbe {
	return contextCancelProbe{ctx: ctx}
}

// cancelled reports whether the context, or the statement's parent, is done.
func (p *contextCancelProbe) cancelled() bool {
	if p.ctx == nil {
		return false
	}
	if !p.initialized {
		p.initialized = true
		p.done = p.ctx.Done()
		// statementContext relays parent cancellation asynchronously. Observe
		// its parent directly so a delayed callback cannot exhaust the work.
		if statement, ok := p.ctx.Value(statementContextKey{}).(*statementContext); ok {
			p.parentDone = statement.parent.Done()
		}
		if p.ctx.Err() != nil {
			return true
		}
	}
	select {
	case <-p.done:
		return true
	default:
	}
	// Separate nonblocking receives retain Go's cheap single-channel probe;
	// a multi-case select would enter the general scheduler on every step.
	select {
	case <-p.parentDone:
		return true
	default:
		return false
	}
}

// err is the cancellation the probe observed: the context's error, or its
// statement parent's when only the parent is done yet.
func (p *contextCancelProbe) err() error {
	if p.ctx == nil {
		return nil
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if statement, ok := p.ctx.Value(statementContextKey{}).(*statementContext); ok {
		return statement.parent.Err()
	}
	return nil
}

// traversalCancelled probes the DFS state's context. Once observed,
// cancellation stays latched while every ancestor unwinds.
func (state *TraversalContext) traversalCancelled() bool {
	if state.cancellationObserved {
		return true
	}
	if state.cancelCtx == nil {
		return false
	}
	if !state.cancelProbe.initialized {
		state.cancelProbe.ctx = state.cancelCtx
	}
	if state.cancelProbe.cancelled() {
		return state.observeTraversalCancellation()
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
// becomes a successful statement result on either evaluator/transaction route.
func (state *TraversalContext) observeTraversalCancellation() bool {
	state.cancellationObserved = true
	if err := state.cancelCtx.Err(); err != nil {
		recordExpressionFailure(state.cancelCtx, err)
	}
	return true
}
