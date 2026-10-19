package cypher

import "context"

type procedureStatementStatsKey struct{}
type procedureStatementStats struct {
	context.Context
	stats    *QueryStats
	executed bool
}

// Value keeps the invocation collector in the same allocation as its context
// frame. Other statement metadata continues through the caller's context.
func (s *procedureStatementStats) Value(key interface{}) interface{} {
	if _, own := key.(procedureStatementStatsKey); own {
		return s
	}
	return s.Context.Value(key)
}

// executeProcedureHandler owns the summary of statement-valued procedures.
// Their output rows (including APOC statistics rows) are independent of the
// statement summary. Replace their hand-built summary with the successful
// nested statements' counters, including iterator/export/custom bodies.
func (e *StorageExecutor) executeProcedureHandler(ctx context.Context, proc registeredProcedure, query string, args []interface{}) (*ExecuteResult, error) {
	totals := &procedureStatementStats{Context: ctx}
	result, err := proc.Handler(totals, e, query, args)
	if err == nil && totals.executed {
		if result == nil {
			result = &ExecuteResult{}
		}
		result.Stats = totals.stats
	}
	return result, err
}
