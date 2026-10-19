package cypher

import "context"

// Expression subqueries return scalar/list values, while their completed
// child summary belongs to the surrounding statement. Every query scope
// owns its counter accumulator, also used by the Fabric result boundary.
// WHERE evaluation may run on parallel row workers.
type expressionStatsContext struct {
	context.Context
	queryStatsAccumulator
}

func (s *expressionStatsContext) Value(key interface{}) interface{} {
	if _, own := key.(queryStatsAccumulatorKey); own {
		return &s.queryStatsAccumulator
	}
	return s.Context.Value(key)
}
