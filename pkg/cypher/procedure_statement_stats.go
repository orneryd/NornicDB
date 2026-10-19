package cypher

import "context"

type procedureStatementStatsKey struct{}
type procedureStatementStats struct {
 stats *QueryStats
 executed bool
}

// executeProcedureHandler owns the summary of statement-valued procedures.
// Their output rows (including APOC statistics rows) are independent of the
// statement summary. Replace their hand-built summary with the successful
// nested statements' counters, including iterator/export/custom bodies.
func (e *StorageExecutor) executeProcedureHandler(ctx context.Context, proc registeredProcedure, query string, args []interface{}) (*ExecuteResult, error) {
 totals := &procedureStatementStats{}
 result, err := proc.Handler(context.WithValue(ctx, procedureStatementStatsKey{}, totals), e, query, args)
 if err == nil && result != nil && totals.executed {
  result.Stats = totals.stats
 }
 return result, err
}
