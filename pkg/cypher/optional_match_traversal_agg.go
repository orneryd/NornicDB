package cypher

// Implicit-grouping aggregation for the traversal-seeded OPTIONAL MATCH
// pipeline (see optional_match_traversal.go), mirroring Neo4j's aggregation
// model:
//
//   - EagerAggregationPipe.scala + GroupingAggTable/NonGroupingAggTable:
//     non-aggregate RETURN items are the grouping key; aggregate expressions
//     accumulate per group; aggregation over an empty ungrouped input yields
//     one row of identity values (count -> 0, collect -> [], sum -> 0,
//     avg/min/max/stdev -> null).
//   - isolateAggregation.scala (front-end): a RETURN item that CONTAINS
//     aggregates without BEING one (e.g. "count(x) + 1", "{c: count(*)}") is
//     handled by isolating each aggregate subexpression, aggregating it per
//     group, and then evaluating the outer expression with the aggregate
//     results substituted — the same rewrite Neo4j performs into an
//     intermediate WITH clause.
//   - StdevFunction.scala: stdev/stdevp — null on empty input, 0.0 for a
//     single value, else sqrt(M2/(n-1)) (sample) or sqrt(M2/n) (population).
//
// Aggregates skip null arguments; count(*) counts rows; DISTINCT deduplicates
// by value identity. No aggregate shape is rejected: the only error kept is
// an empty argument list (e.g. "count()"), which Neo4j itself rejects at
// compile time ("Insufficient parameters for function 'count'").

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// traversalAggFnNames are the aggregate functions the traversal pipeline
// accumulates, matching the executor-wide aggregateFnNames set. stdevp is
// listed before stdev so prefix scanning matches the longer name first.
var traversalAggFnNames = []string{
	"percentilecont", "percentiledisc", "collect", "count", "sum", "avg", "min", "max", "stdevp", "stdev",
}

// traversalAggSpec is one parsed aggregate call.
type traversalAggSpec struct {
	fn       string // lower-case name from traversalAggFnNames
	inner    string // argument expression text ("" when star)
	distinct bool
	star     bool // count(*)
}

// aggregateSpan is one aggregate call located inside a larger expression.
type aggregateSpan struct {
	start, end int // expr[start:end] is the full call text
}

// findAggregateSpans locates the outermost aggregate calls in expr: an
// aggregate function name at a word boundary, outside quoted strings,
// followed by a balanced parenthesized argument list. Scanning resumes after
// each span, so aggregates nested inside another aggregate's arguments are
// not reported separately.
func findAggregateSpans(expr string) []aggregateSpan {
	var spans []aggregateSpan
	lower := lowerASCII(expr)
	i := 0
	for i < len(lower) {
		c := lower[i]
		if c == '\'' || c == '"' || c == '`' {
			j := i + 1
			for j < len(lower) && (lower[j] != c || isBackslashEscaped(lower, j)) {
				j++
			}
			i = j + 1
			continue
		}
		matched := false
		for _, fn := range traversalAggFnNames {
			if !strings.HasPrefix(lower[i:], fn) {
				continue
			}
			// A qualified function whose terminal component happens to have an
			// aggregate name (for example apoc.coll.sum()) is not a Cypher
			// aggregate. It is evaluated once per row by the same expression path.
			if i > 0 && (isIdentByte(lower[i-1]) || lower[i-1] == '.') {
				continue
			}
			j := i + len(fn)
			for j < len(lower) && isWhitespace(lower[j]) {
				j++
			}
			if j >= len(lower) || lower[j] != '(' {
				continue
			}
			depth := 0
			end := -1
			for k := j; k < len(lower) && end < 0; k++ {
				switch lower[k] {
				case '(':
					depth++
				case ')':
					depth--
					if depth == 0 {
						end = k + 1
					}
				}
			}
			if end < 0 {
				continue
			}
			spans = append(spans, aggregateSpan{start: i, end: end})
			i = end
			matched = true
			break
		}
		if !matched {
			i++
		}
	}
	return spans
}

// parseTraversalAggregateCall parses one whole aggregate call such as
// "count(DISTINCT f.path)". The only rejected form is an empty argument list,
// which Neo4j itself rejects at compile time.
func parseTraversalAggregateCall(expr string) (traversalAggSpec, error) {
	spec := traversalAggSpec{}
	trimmed := strings.TrimSpace(expr)
	open := strings.Index(trimmed, "(")
	if open <= 0 || !strings.HasSuffix(trimmed, ")") {
		return spec, localizedError(localization.CypherMatchingAggregateCallExpected(trimmed), nil)
	}
	name := lowerASCII(strings.TrimSpace(trimmed[:open]))
	for _, fn := range traversalAggFnNames {
		if name == fn {
			spec.fn = fn
			break
		}
	}
	if spec.fn == "" {
		return spec, localizedError(localization.CypherMatchingAggregateCallExpected(trimmed), nil)
	}
	inner := strings.TrimSpace(trimmed[open+1 : len(trimmed)-1])
	if spec.fn == "count" && inner == "*" {
		spec.star = true
		return spec, nil
	}
	inner, spec.distinct = cutDistinct(inner)
	if inner == "" {
		return spec, localizedError(localization.CypherMatchingFunctionParametersInsufficient(spec.fn), nil)
	}
	spec.inner = inner
	return spec, nil
}

// traversalAggPlaceholder returns the synthetic variable name substituted for
// the n-th aggregate span of a mixed item (isolateAggregation's x1/x2 rewrite).
func traversalAggPlaceholder(n int) string {
	return fmt.Sprintf("__nornic_agg_%d", n)
}

// aggregateTraversalOptionalRows evaluates a RETURN projection containing
// aggregates over the joined rows, grouping by the non-aggregate items.
func (e *StorageExecutor) aggregateTraversalOptionalRows(ctx context.Context, rows []traversalOptRow, items []returnItem) ([][]interface{}, error) {
	bindings := make([]pipelineRow, len(rows))
	for index, row := range rows {
		bindings[index] = pipelineRowFromTraversalOptionalRow(row)
	}
	plan := returnProjectionPlanFromItems(items)
	if result, ok := e.pipelineApplyReturnPlan(ctx, bindings, plan, pipelineRowsSource(bindings), false); ok {
		return result.Rows, nil
	}
	if failure := getExpressionFailure(ctx); failure != nil {
		return nil, failure
	}
	return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "could not parse aggregate projection")
}

// finalizeTraversalAggregate reduces one aggregate call's accumulated
// non-null values (already deduplicated when DISTINCT) into the final value:
// count(*) counts rows, count(x) counts non-null values, sum of an empty set
// is 0, avg/min/max of an empty set are null, and stdev/stdevp follow
// Neo4j's StdevFunction (null on empty, 0.0 for a single value).
func (e *StorageExecutor) finalizeTraversalAggregate(spec traversalAggSpec, vals []interface{}, rowCount int64) interface{} {
	switch spec.fn {
	case "count":
		if spec.star {
			return rowCount
		}
		return int64(len(vals))
	case "collect":
		if vals == nil {
			return []interface{}{}
		}
		return vals
	case "sum":
		return sumTraversalAggregateValues(vals)
	case "avg":
		if len(vals) == 0 {
			return nil
		}
		total := 0.0
		for _, v := range vals {
			f, ok := toFloat64(v)
			if !ok {
				return nil
			}
			total += f
		}
		return total / float64(len(vals))
	case "min", "max":
		if len(vals) == 0 {
			return nil
		}
		best := vals[0]
		for _, v := range vals[1:] {
			cmp := e.compareOrderValues(v, best)
			if (spec.fn == "min" && cmp < 0) || (spec.fn == "max" && cmp > 0) {
				best = v
			}
		}
		return best
	case "stdev", "stdevp":
		return stdevTraversalAggregateValues(vals, spec.fn == "stdevp")
	}
	return nil
}

// stdevTraversalAggregateValues implements Neo4j's StdevFunction contract via
// Welford's online algorithm: null when no numeric values, 0.0 for a single
// value, sqrt(M2/(n-1)) for the sample deviation, sqrt(M2/n) for population.
func stdevTraversalAggregateValues(vals []interface{}, population bool) interface{} {
	count := 0
	movingAvg := 0.0
	m2 := 0.0
	for _, v := range vals {
		f, ok := toFloat64(v)
		if !ok {
			continue
		}
		count++
		next := movingAvg + (f-movingAvg)/float64(count)
		m2 += (f - movingAvg) * (f - next)
		movingAvg = next
	}
	if count == 0 {
		return nil
	}
	if count < 2 {
		return 0.0
	}
	if population {
		return math.Sqrt(m2 / float64(count))
	}
	return math.Sqrt(m2 / float64(count-1))
}

// sumTraversalAggregateValues sums values, keeping int64 when every input is
// an integer (Neo4j-compatible) and falling back to float64 otherwise.
func sumTraversalAggregateValues(vals []interface{}) interface{} {
	allInts := true
	var intSum int64
	var floatSum float64
	for _, v := range vals {
		switch n := v.(type) {
		case int64:
			intSum += n
			floatSum += float64(n)
		case int:
			intSum += int64(n)
			floatSum += float64(n)
		default:
			allInts = false
			f, ok := toFloat64(v)
			if !ok {
				return nil
			}
			floatSum += f
		}
	}
	if allInts {
		return intSum
	}
	return floatSum
}
