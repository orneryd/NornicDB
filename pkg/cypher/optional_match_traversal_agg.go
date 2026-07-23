package cypher

// Implicit-grouping aggregation for the traversal-seeded OPTIONAL MATCH
// pipeline (see optional_match_traversal.go). Non-aggregate RETURN items form
// the grouping key (Cypher implicit grouping); aggregate items accumulate per
// group with standard Cypher null handling (aggregates skip nulls; count(*)
// counts rows).

import (
	"context"
	"fmt"
	"strings"
)

// traversalAggSpec is one parsed aggregate RETURN item.
type traversalAggSpec struct {
	fn       string // lower-case: count, sum, avg, min, max, collect
	inner    string // argument expression text ("" when star)
	distinct bool
	star     bool // count(*)
}

// parseTraversalAggregateCall parses a top-level aggregate call such as
// "count(DISTINCT f.path)" or "collect(t.name)". It rejects shapes it cannot
// evaluate faithfully (trailing operators, stdev/stdevp) with an explicit
// error so the query fails loudly instead of corrupting.
func parseTraversalAggregateCall(expr string) (traversalAggSpec, error) {
	spec := traversalAggSpec{}
	trimmed := strings.TrimSpace(expr)
	lower := strings.ToLower(trimmed)
	for _, fn := range []string{"count", "sum", "avg", "min", "max", "collect"} {
		if strings.HasPrefix(lower, fn+"(") || strings.HasPrefix(lower, fn+" (") {
			spec.fn = fn
			break
		}
	}
	if spec.fn == "" {
		return spec, fmt.Errorf("unsupported aggregate expression %q in OPTIONAL MATCH projection", trimmed)
	}
	open := strings.Index(trimmed, "(")
	if open < 0 || !strings.HasSuffix(trimmed, ")") {
		return spec, fmt.Errorf("unsupported aggregate expression %q in OPTIONAL MATCH projection", trimmed)
	}
	inner := strings.TrimSpace(trimmed[open+1 : len(trimmed)-1])
	if spec.fn == "count" && inner == "*" {
		spec.star = true
		return spec, nil
	}
	if len(inner) >= len("DISTINCT ") && strings.EqualFold(inner[:len("DISTINCT")], "DISTINCT") && inner[len("DISTINCT")] == ' ' {
		spec.distinct = true
		inner = strings.TrimSpace(inner[len("DISTINCT"):])
	}
	if inner == "" {
		return spec, fmt.Errorf("empty aggregate argument in %q", trimmed)
	}
	spec.inner = inner
	return spec, nil
}

// traversalAggGroup accumulates one implicit group.
type traversalAggGroup struct {
	groupVals []interface{}   // evaluated non-aggregate item values by item index
	values    [][]interface{} // accumulated non-null argument values by item index
	seen      []map[string]bool
	rowCount  int64
}

// aggregateTraversalOptionalRows evaluates a RETURN projection containing
// aggregates over the joined rows, grouping by the non-aggregate items.
func (e *StorageExecutor) aggregateTraversalOptionalRows(ctx context.Context, rows []traversalOptRow, items []returnItem) ([][]interface{}, error) {
	specs := make([]*traversalAggSpec, len(items))
	hasGroupKeys := false
	for i, item := range items {
		if isAggregateExpression(item.expr) {
			spec, err := parseTraversalAggregateCall(item.expr)
			if err != nil {
				return nil, err
			}
			specs[i] = &spec
		} else {
			hasGroupKeys = true
		}
	}

	groups := make(map[string]*traversalAggGroup)
	var order []string
	for _, row := range rows {
		keyParts := make([]string, 0, len(items))
		groupVals := make([]interface{}, len(items))
		for i, item := range items {
			if specs[i] != nil {
				continue
			}
			v := e.evalTraversalProjection(ctx, item.expr, row)
			groupVals[i] = v
			keyParts = append(keyParts, joinedValueKey(v))
		}
		key := strings.Join(keyParts, "|")
		group := groups[key]
		if group == nil {
			group = &traversalAggGroup{
				groupVals: groupVals,
				values:    make([][]interface{}, len(items)),
				seen:      make([]map[string]bool, len(items)),
			}
			groups[key] = group
			order = append(order, key)
		}
		group.rowCount++
		for i, spec := range specs {
			if spec == nil || spec.star {
				continue
			}
			v := e.evalTraversalProjection(ctx, spec.inner, row)
			if v == nil {
				continue // Cypher aggregates skip nulls
			}
			if spec.distinct {
				if group.seen[i] == nil {
					group.seen[i] = make(map[string]bool)
				}
				k := joinedValueKey(v)
				if group.seen[i][k] {
					continue
				}
				group.seen[i][k] = true
			}
			group.values[i] = append(group.values[i], v)
		}
	}

	if len(groups) == 0 {
		if hasGroupKeys {
			return [][]interface{}{}, nil
		}
		// Aggregation over an empty row set with no grouping keys yields one
		// row of aggregate identities (count -> 0, collect -> [], ...).
		out := make([]interface{}, len(items))
		for i, item := range items {
			out[i] = aggregateIdentity(item.expr)
		}
		return [][]interface{}{out}, nil
	}

	outRows := make([][]interface{}, 0, len(groups))
	for _, key := range order {
		group := groups[key]
		outRow := make([]interface{}, len(items))
		for i := range items {
			if specs[i] == nil {
				outRow[i] = group.groupVals[i]
				continue
			}
			outRow[i] = e.finalizeTraversalAggregate(*specs[i], group.values[i], group.rowCount)
		}
		outRows = append(outRows, outRow)
	}
	return outRows, nil
}

// finalizeTraversalAggregate reduces one aggregate item's accumulated non-null
// values (already deduplicated when DISTINCT) into the final value, following
// Cypher semantics: count(*) counts rows, count(x) counts non-null values,
// sum of an empty set is 0, avg/min/max of an empty set are null.
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
	}
	return nil
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
