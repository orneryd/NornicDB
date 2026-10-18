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

// aggregateSpan is one aggregate call located inside a larger expression.
type aggregateSpan struct {
	start, end int // expr[start:end] is the full call text
}

// findAggregateSpans locates the outermost aggregate calls in expr: an
// aggregate function name at a word boundary, outside quoted strings,
// followed by a balanced parenthesized argument list. Scanning resumes after
// each span, so aggregates nested inside another aggregate's arguments are
// not reported separately. An EXISTS / COUNT / COLLECT { … } subquery's body
// is skipped: its aggregates are the subquery's own (#907).
func findAggregateSpans(expr string) []aggregateSpan {
	var spans []aggregateSpan
	i := 0
	for i < len(expr) {
		if end, quoted := skipCypherLexicalToken(expr, i); quoted {
			i = end
			continue
		}
		c := expr[i]
		// Only a letter at a word start can begin an aggregate or a subquery
		// expression. A qualified function whose terminal component happens to
		// have an aggregate name (for example apoc.coll.sum()) is not a Cypher
		// aggregate. It is evaluated once per row by the same expression path;
		// a qualified aggregate (gds.graph.project) is matched by its whole
		// name from its first component.
		folded := asciiUpper(c)
		if folded < 'A' || folded > 'Z' || aggregateStartMask&(uint32(1)<<(folded-'A')) == 0 ||
			i > 0 && (isIdentByte(expr[i-1]) || expr[i-1] == '.') {
			i++
			continue
		}
		matched := false
		for _, fn := range traversalAggFnNames {
			if fn.upper[0] != folded {
				continue
			}
			if !strings.HasPrefix(expr[i:], fn.lower) && !startsWithKeywordFold(expr[i:], fn.upper) {
				continue
			}
			j := i + len(fn.lower)
			for j < len(expr) && isWhitespace(expr[j]) {
				j++
			}
			if j >= len(expr) || expr[j] != '(' {
				continue
			}
			close := findMatchingParen(expr, j)
			if close < 0 {
				continue
			}
			end := close + 1
			spans = append(spans, aggregateSpan{start: i, end: end})
			i = end
			matched = true
			break
		}
		if matched {
			continue
		}
		// EXISTS / COUNT / COLLECT { … } is skipped whole: its aggregates
		// are the subquery's own. An aggregate call is read first: count(…)
		// is not a subquery, and is then read once.
		if folded == 'E' || folded == 'C' {
			if end := subqueryExpressionEnd(expr, i); end > i {
				i = end
				continue
			}
		}
		i++
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

// subqueryExpressionEnd is the index after the EXISTS / COUNT / COLLECT
// { … } subquery that starts at expr[i], or -1.
func subqueryExpressionEnd(expr string, i int) int {
	// Use the shared reader so comments between the keyword and brace do
	// not expose the subquery's aggregates to the outer projection.
	if _, open, ok := subqueryExpressionKeywordAt(expr, i); ok {
		if closing := findMatchingDelimiter(expr, open, '{', '}'); closing > open {
			return closing + 1
		}
	}
	return -1
}
