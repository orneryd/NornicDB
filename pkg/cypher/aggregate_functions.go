package cypher

import "strings"

// cypherAggregateNames are the aggregating functions, in lower case:
// Cypher's own and GDS's gds.graph.project, the Cypher aggregation
// projection (#1194). Every check of whether a call aggregates reads this
// one list. A name comes before a shorter name it starts with (stdevp
// before stdev), for prefix scanning.
var cypherAggregateNames = []string{
	"percentilecont", "percentiledisc", "collect", "count", "sum", "avg", "min", "max", "stdevp", "stdev",
	gdsProjectAggregate,
}

// gdsProjectAggregate is GDS's Cypher aggregation projection.
const gdsProjectAggregate = "gds.graph.project"

// traversalAggFnNames are cypherAggregateNames with their upper-case forms,
// for findAggregateSpans' scan.
var traversalAggFnNames = func() []struct{ lower, upper string } {
	names := make([]struct{ lower, upper string }, len(cypherAggregateNames))
	for i, name := range cypherAggregateNames {
		names[i].lower, names[i].upper = name, strings.ToUpper(name)
	}
	return names
}()

// aggregateStartMask holds the first letters of cypherAggregateNames, and E
// (EXISTS { … }): only a word starting with one of them can begin an
// aggregate or a subquery expression.
var aggregateStartMask = func() uint32 {
	mask := uint32(1) << ('E' - 'A')
	for _, name := range cypherAggregateNames {
		mask |= 1 << (asciiUpper(name[0]) - 'A')
	}
	return mask
}()

// isAggregateFunc reports whether expr is one aggregate call (whitespace
// tolerant).
func isAggregateFunc(expr string) bool { return isAggregateFuncWithCancellation(expr, nil) }
func isAggregateFuncWithCancellation(expr string, probe *pipelineCancellation) bool {
	for _, name := range cypherAggregateNames {
		if probe != nil && !probe.check() {
			return false
		}
		if isFunctionCallWSWithCancellation(expr, name, probe) {
			return true
		}
		if probe != nil && probe.err != nil {
			return false
		}
	}
	return false
}

// traversalComputesAggregate reports whether the traversal pipeline's own
// aggregation computes expr, an upper-case aggregate call: count, sum, avg,
// min, max and collect. Any other aggregate goes through the pipeline's
// aggregate projection (aggregateTraversalOptionalRows).
func traversalComputesAggregate(upper string) bool {
	for _, prefix := range []string{"COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "COLLECT("} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}
