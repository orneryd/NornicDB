package cypher

import (
	"context"
	"sync/atomic"
)

type resolvedStatementModeKey struct{}
type resolvedStatementMode struct{ write atomic.Bool }

// resolvedStatementModeOf follows the statement-issued executor and storage
// before consulting context. A handler may supply a new context or construct a
// new executor on its storage; neither changes the statement's reporting owner.
func (e *StorageExecutor) resolvedStatementModeOf(ctx context.Context) *resolvedStatementMode {
	if e.resolvedMode != nil {
		return e.resolvedMode
	}
	// Storage membership owns wrapper traversal and transaction boundaries.
	// Reporting observes that same issued object before consulting context hints.
	if wrapper := membershipOfStorage(e.storage).storage; wrapper != nil {
		if state := wrapper.resolvedMode.Load(); state != nil {
			return state
		}
	}

	state, _ := ctx.Value(resolvedStatementModeKey{}).(*resolvedStatementMode)
	return state
}

// bindResolvedStatementMode issues an executor for this reporting scope. The
// shared/session executor is never mutated. A reused explicit transaction's
// storage loses the binding when the statement returns, so the next statement
// starts its own observation. Nested scopes restore their enclosing binding.
func (e *StorageExecutor) bindResolvedStatementMode(state *resolvedStatementMode) (*StorageExecutor, func()) {
	bound := e
	if e.resolvedMode != state {
		bound = e.cloneWithStorage(e.storage)
		bound.resolvedMode = state
	}
	if wrapper := membershipOfStorage(bound.storage).storage; wrapper != nil {
		previous := wrapper.resolvedMode.Swap(state)
		return bound, func() { wrapper.resolvedMode.Store(previous) }
	}
	return bound, func() {}
}

const resolvedStatementModeMetadata = "resolved_query_type"

// ResolvedStatementQueryType is the execution owner's mode, when available.
// Protocol adapters share this reader; no transport reclassifies argument text.
func ResolvedStatementQueryType(metadata map[string]interface{}) (string, bool) {
	mode, ok := metadata[resolvedStatementModeMetadata].(string)
	switch mode {
	case "r", "w", "rw", "s":
		return mode, ok
	}
	return "", false
}

func resolvedStatementModeResult(result *ExecuteResult, write bool) *ExecuteResult {
	if result == nil {
		return nil
	}
	mode := "r"
	if write {
		mode = "w"
	}
	if current, ok := ResolvedStatementQueryType(result.Metadata); ok && current == mode {
		return result
	}
	copy := *result
	copy.Metadata = make(map[string]interface{}, len(result.Metadata)+1)
	for key, value := range result.Metadata {
		copy.Metadata[key] = value
	}
	copy.Metadata[resolvedStatementModeMetadata] = mode
	return &copy
}

// Only CALL results need a resolved procedure mode beyond the ordinary query
// summary. Every non-writing CALL shares an observation with evaluated nested
// execution: a persisted READ procedure or plugin can run dynamic text without
// declaring ExecutesStatements. This reports mode; storage still owns admission.
func (e *StorageExecutor) observeResolvedStatementMode(ctx context.Context, statement string) (context.Context, func(*ExecuteResult) *ExecuteResult, error) {
	contract, err := readStatementExecutionContract(ctx, statement)
	if err != nil {
		return ctx, nil, err
	}
	if !contract.calls {
		return ctx, nil, nil
	}
	state := e.resolvedStatementModeOf(ctx)
	if state == nil && !contract.changesState() {
		state = &resolvedStatementMode{}
		ctx = context.WithValue(ctx, resolvedStatementModeKey{}, state)
	}
	return ctx, func(result *ExecuteResult) *ExecuteResult {
		write := contract.changesState() || state != nil && state.write.Load()
		return resolvedStatementModeResult(result, write)
	}, nil
}
