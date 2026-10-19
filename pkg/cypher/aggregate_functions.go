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
func isAggregateFunc(expr string) bool {
	for _, name := range cypherAggregateNames {
		if isFunctionCallWS(expr, name) {
			return true
		}
	}
	return false
}

// traversalComputesAggregate reports whether the traversal pipeline's own
// aggregation computes expr, an upper-case aggregate call: count, sum, avg,
// min, max and collect. Any other aggregate goes through the pipeline's
// aggregate projection (aggregateTraversalOptionalRows).
func traversalComputesAggregate(expression string) bool {
	name, _, ok := parseFunctionCallWS(expression)
	if !ok {
		return false
	}
	switch lowerASCII(name) {
	case "count", "sum", "avg", "min", "max", "collect":
		return true
	}
	return false
}
