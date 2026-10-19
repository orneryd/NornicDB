package cypher

import "context"

type expressionStatsKey struct{}

// Expression subqueries return scalar/list values, while their completed
// child summary belongs to the surrounding statement. Each query owns its
// scope so nested statements cannot also charge their caller's counters.
// Reuse the concurrent counter owner used by Fabric: WHERE evaluation may
// run on parallel row workers.
type expressionStatsContext struct {
	context.Context
	queryStatsAccumulator
}

func (s *expressionStatsContext) Value(key interface{}) interface{} {
	if _, own := key.(expressionStatsKey); own {
		return s
	}
	return s.Context.Value(key)
}

func recordSubqueryStats(ctx context.Context, stats *QueryStats) {
	if scope, ok := ctx.Value(expressionStatsKey{}).(*expressionStatsContext); ok {
		scope.add(stats)
	}
}
