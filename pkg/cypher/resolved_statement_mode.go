package cypher

import (
	"context"
	"sync/atomic"
)

type resolvedStatementModeKey struct{}
type resolvedStatementMode struct{ write atomic.Bool }

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

// CompletedStatementQueryType reports the execution owner's mode, falling back
// to positively observed completed writes. Without either evidence it returns
// an empty optional mode; a protocol with a static routing type retains it.
func CompletedStatementQueryType(metadata map[string]interface{}, wrote bool) string {
    if mode, known := ResolvedStatementQueryType(metadata); known { return mode }
    if wrote { return "w" }
    return ""
}

func resolvedStatementModeResult(result *ExecuteResult, write bool) *ExecuteResult {
	if result == nil {
		return nil
	}
	mode := "r"
	if write {
		mode = "w"
	}
	// Cached results already carry their immutable resolved mode. Retain the
	// cache's result identity and never mutate a published metadata map.
	if existing, ok := ResolvedStatementQueryType(result.Metadata); ok && existing == mode {
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
func (e *StorageExecutor) observeResolvedStatementMode(ctx context.Context, statement string) (context.Context, func(*ExecuteResult) *ExecuteResult, error) {
	contract, err := readStatementExecutionContract(statement)
	if err != nil {
		return ctx, nil, err
	}
	if !contract.calls {
		return ctx, nil, nil
	}
	state, _ := ctx.Value(resolvedStatementModeKey{}).(*resolvedStatementMode)
	if state == nil && contract.dynamic {
		state = &resolvedStatementMode{}
		ctx = context.WithValue(ctx, resolvedStatementModeKey{}, state)
	}
	return ctx, func(result *ExecuteResult) *ExecuteResult {
		write := contract.writes || state != nil && state.write.Load()
		return resolvedStatementModeResult(result, write)
	}, nil
}
