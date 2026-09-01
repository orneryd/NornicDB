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
	"github.com/orneryd/nornicdb/pkg/storage"
)

// traversalAggFnNames are the aggregate functions the traversal pipeline
// accumulates, matching the executor-wide aggregateFnNames set. stdevp is
// listed before stdev so prefix scanning matches the longer name first.
var traversalAggFnNames = []string{"collect", "count", "sum", "avg", "min", "max", "stdevp", "stdev"}

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
	lower := strings.ToLower(expr)
	i := 0
	for i < len(lower) {
		c := lower[i]
		if c == '\'' || c == '"' || c == '`' {
			j := i + 1
			for j < len(lower) && (lower[j] != c || lower[j-1] == '\\') {
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
			if i > 0 && isIdentByte(lower[i-1]) {
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

// traversalItemsContainAggregate reports whether any RETURN item contains an
// aggregate call (whether or not the whole item is one).
func traversalItemsContainAggregate(items []returnItem) bool {
	for _, item := range items {
		if len(findAggregateSpans(item.expr)) > 0 {
			return true
		}
	}
	return false
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
	name := strings.ToLower(strings.TrimSpace(trimmed[:open]))
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
	if len(inner) > len("DISTINCT") && strings.EqualFold(inner[:len("DISTINCT")], "DISTINCT") && inner[len("DISTINCT")] == ' ' {
		spec.distinct = true
		inner = strings.TrimSpace(inner[len("DISTINCT"):])
	}
	if inner == "" {
		return spec, localizedError(localization.CypherMatchingFunctionParametersInsufficient(spec.fn), nil)
	}
	spec.inner = inner
	return spec, nil
}

// traversalAggItem is one classified RETURN item.
type traversalAggItem struct {
	grouping  bool
	spec      *traversalAggSpec // whole-item aggregate
	rewritten string            // mixed item: expr with spans replaced by placeholders
	specs     []traversalAggSpec
	compiled  []compiledTraversalProjection // per-spec inner projections
	groupFn   compiledTraversalProjection   // grouping item projection
}

// traversalAggPlaceholder returns the synthetic variable name substituted for
// the n-th aggregate span of a mixed item (isolateAggregation's x1/x2 rewrite).
func traversalAggPlaceholder(n int) string {
	return fmt.Sprintf("__nornic_agg_%d", n)
}

// classifyTraversalAggItems splits RETURN items into grouping keys, whole
// aggregates, and mixed expressions, pre-compiling every per-row projection.
func (e *StorageExecutor) classifyTraversalAggItems(ctx context.Context, items []returnItem) ([]traversalAggItem, error) {
	classified := make([]traversalAggItem, len(items))
	for i, item := range items {
		expr := strings.TrimSpace(item.expr)
		spans := findAggregateSpans(expr)
		switch {
		case len(spans) == 0:
			classified[i] = traversalAggItem{grouping: true, groupFn: e.compileTraversalProjection(ctx, expr)}
		case len(spans) == 1 && spans[0].start == 0 && spans[0].end == len(expr):
			spec, err := parseTraversalAggregateCall(expr)
			if err != nil {
				return nil, err
			}
			ti := traversalAggItem{spec: &spec}
			if !spec.star {
				ti.compiled = []compiledTraversalProjection{e.compileTraversalProjection(ctx, spec.inner)}
			}
			classified[i] = ti
		default:
			// Mixed expression: isolate each aggregate span (isolateAggregation).
			var sb strings.Builder
			var specs []traversalAggSpec
			var compiled []compiledTraversalProjection
			last := 0
			for n, span := range spans {
				spec, err := parseTraversalAggregateCall(expr[span.start:span.end])
				if err != nil {
					return nil, err
				}
				sb.WriteString(expr[last:span.start])
				sb.WriteString(traversalAggPlaceholder(n))
				last = span.end
				specs = append(specs, spec)
				if spec.star {
					compiled = append(compiled, nil)
				} else {
					compiled = append(compiled, e.compileTraversalProjection(ctx, spec.inner))
				}
			}
			sb.WriteString(expr[last:])
			classified[i] = traversalAggItem{rewritten: sb.String(), specs: specs, compiled: compiled}
		}
	}
	return classified, nil
}

// traversalAggAccum accumulates one aggregate call's values within one group.
type traversalAggAccum struct {
	values []interface{}
	seen   map[string]bool
}

func (a *traversalAggAccum) add(v interface{}, distinct bool) {
	if v == nil {
		return // Cypher aggregates skip nulls
	}
	if distinct {
		if a.seen == nil {
			a.seen = make(map[string]bool)
		}
		key := joinedValueKey(v)
		if a.seen[key] {
			return
		}
		a.seen[key] = true
	}
	a.values = append(a.values, v)
}

// traversalAggGroup accumulates one implicit group.
type traversalAggGroup struct {
	groupVals []interface{}         // evaluated grouping values by item index
	accums    [][]traversalAggAccum // per item, per aggregate span
	rowCount  int64
}

// aggregateTraversalOptionalRows evaluates a RETURN projection containing
// aggregates over the joined rows, grouping by the non-aggregate items.
func (e *StorageExecutor) aggregateTraversalOptionalRows(ctx context.Context, rows []traversalOptRow, items []returnItem) ([][]interface{}, error) {
	classified, err := e.classifyTraversalAggItems(ctx, items)
	if err != nil {
		return nil, err
	}
	hasGroupKeys := false
	for _, ti := range classified {
		if ti.grouping {
			hasGroupKeys = true
		}
	}

	groups := make(map[string]*traversalAggGroup)
	var order []string
	for _, row := range rows {
		keyParts := make([]string, 0, len(items))
		groupVals := make([]interface{}, len(items))
		for i, ti := range classified {
			if !ti.grouping {
				continue
			}
			v := ti.groupFn(row)
			groupVals[i] = v
			keyParts = append(keyParts, joinedValueKey(v))
		}
		key := strings.Join(keyParts, "|")
		group := groups[key]
		if group == nil {
			group = &traversalAggGroup{groupVals: groupVals, accums: make([][]traversalAggAccum, len(items))}
			for i, ti := range classified {
				switch {
				case ti.spec != nil:
					group.accums[i] = make([]traversalAggAccum, 1)
				case ti.rewritten != "":
					group.accums[i] = make([]traversalAggAccum, len(ti.specs))
				}
			}
			groups[key] = group
			order = append(order, key)
		}
		group.rowCount++
		for i, ti := range classified {
			switch {
			case ti.spec != nil:
				if !ti.spec.star {
					group.accums[i][0].add(ti.compiled[0](row), ti.spec.distinct)
				}
			case ti.rewritten != "":
				for n, spec := range ti.specs {
					if spec.star {
						continue
					}
					group.accums[i][n].add(ti.compiled[n](row), spec.distinct)
				}
			}
		}
	}

	if len(groups) == 0 {
		if hasGroupKeys {
			return [][]interface{}{}, nil
		}
		// Aggregation over an empty ungrouped input: one row of identities
		// (NonGroupingAggTable semantics).
		out := make([]interface{}, len(items))
		for i, ti := range classified {
			switch {
			case ti.spec != nil:
				out[i] = e.finalizeTraversalAggregate(*ti.spec, nil, 0)
			case ti.rewritten != "":
				out[i] = e.evaluateMixedAggregate(ctx, ti, nil, 0)
			}
		}
		return [][]interface{}{out}, nil
	}

	outRows := make([][]interface{}, 0, len(groups))
	for _, key := range order {
		group := groups[key]
		outRow := make([]interface{}, len(items))
		for i, ti := range classified {
			switch {
			case ti.grouping:
				outRow[i] = group.groupVals[i]
			case ti.spec != nil:
				outRow[i] = e.finalizeTraversalAggregate(*ti.spec, group.accums[i][0].values, group.rowCount)
			default:
				outRow[i] = e.evaluateMixedAggregate(ctx, ti, group.accums[i], group.rowCount)
			}
		}
		outRows = append(outRows, outRow)
	}
	return outRows, nil
}

// evaluateMixedAggregate finalizes each aggregate span of a mixed item, then
// evaluates the rewritten outer expression with the results substituted via
// single-"value" pseudo-nodes (the executor's scalar-wrapper convention) —
// the runtime equivalent of isolateAggregation's WITH x1, x2 ... rewrite.
func (e *StorageExecutor) evaluateMixedAggregate(ctx context.Context, ti traversalAggItem, accums []traversalAggAccum, rowCount int64) interface{} {
	nodes := make(map[string]*storage.Node, len(ti.specs))
	for n, spec := range ti.specs {
		var vals []interface{}
		if accums != nil {
			vals = accums[n].values
		}
		result := e.finalizeTraversalAggregate(spec, vals, rowCount)
		nodes[traversalAggPlaceholder(n)] = &storage.Node{Properties: map[string]interface{}{"value": result}}
	}
	return e.evaluateExpressionWithContext(ctx, ti.rewritten, nodes, nil)
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
