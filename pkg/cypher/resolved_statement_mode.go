package cypher

import (
	"context"
	"sync/atomic"
)

type resolvedStatementModeKey struct{}
type resolvedStatementMode struct {
	write  atomic.Bool
	active atomic.Bool
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
// summary. Conditional procedures share an observation with evaluated nested
// execution; fixed procedure contracts need no extra context allocation.
func (e *StorageExecutor) observeResolvedStatementMode(ctx context.Context, statement string) (context.Context, func(*ExecuteResult) *ExecuteResult, func(), error) {
	contract, err := readStatementExecutionContract(ctx, statement)
	if err != nil {
		return ctx, nil, nil, err
	}
	if !contract.calls {
		return ctx, nil, nil, nil
	}
	var release func()
	state, _ := ctx.Value(resolvedStatementModeKey{}).(*resolvedStatementMode)
	if state == nil && contract.dynamic {
		state = &resolvedStatementMode{}
		state.active.Store(true)
		release = func() { state.active.Store(false) }
		ctx = context.WithValue(ctx, resolvedStatementModeKey{}, state)
	}
	return ctx, func(result *ExecuteResult) *ExecuteResult {
		write := contract.writes || state != nil && state.write.Load()
		return resolvedStatementModeResult(result, write)
	}, release, nil
}
