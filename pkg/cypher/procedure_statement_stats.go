package cypher

import "context"

type procedureStatementStatsKey struct{}
type procedureStatementStats struct {
	context.Context
	stats    *QueryStats
	executed bool
}

// A transaction scope stages contributions in the invocation's existing
// collector type. Only the transaction owner may publish them after commit.
// Nested procedure invocations still fold through their returned summary.
type procedureStatementStatsScope struct {
	procedureStatementStats
	parent *procedureStatementStats
}

func beginProcedureStatementStatsScope(ctx context.Context) (context.Context, *procedureStatementStatsScope) {
	parent, _ := ctx.Value(procedureStatementStatsKey{}).(*procedureStatementStats)
	if parent == nil {
		return ctx, nil
	}
	scope := &procedureStatementStatsScope{
		procedureStatementStats: procedureStatementStats{Context: ctx},
		parent: parent,
	}
	return scope, scope
}

func (s *procedureStatementStatsScope) commit() {
	if s == nil || s.parent == nil {
		return
	}
	if s.executed {
		s.parent.executed = true
		s.parent.stats = mergeQueryStats(s.parent.stats, s.stats)
	}
	s.discard()
}

func (s *procedureStatementStatsScope) discard() {
	if s != nil {
		s.parent = nil
	}
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
