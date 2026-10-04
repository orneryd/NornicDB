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
	"strings"
)

// traversalAggFnNames are the aggregate functions the traversal pipeline
// accumulates, matching the executor-wide aggregateFnNames set. stdevp is
// listed before stdev so prefix scanning matches the longer name first.
var traversalAggFnNames = []string{
	"percentilecont", "percentiledisc", "collect", "count", "sum", "avg", "min", "max", "stdevp", "stdev",
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
