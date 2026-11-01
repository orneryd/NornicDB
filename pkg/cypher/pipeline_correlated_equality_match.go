// Author: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"math"
	"strconv"
	"strings"
)

// pipelineCorrelatedEqualityNodeMatch reads a node MATCH over many input rows
// whose WHERE equates a pattern property with a value of the row (UNWIND
// $rows AS row MATCH (o:L) WHERE o.key = row.key) with one candidate read for
// every row: the candidates are grouped by that property, and each row tests
// only its group, plus the candidates whose value has no group key, against
// the whole WHERE. Rows and candidates keep the per-row order. It is the
// retained operator for what the set-based UNWIND rewrite did with composed
// IN text. ok is false for any other shape.
func (e *StorageExecutor) pipelineCorrelatedEqualityNodeMatch(ctx context.Context, rows []pipelineRow, pattern nodePatternInfo, whereClause, pathVariable string, hint pipelineMatchPhysicalHint) ([]pipelineRow, bool, error) {
	if len(rows) < 2 || whereClause == "" || pathVariable != "" || pattern.variable == "" || len(pattern.labels) == 0 || len(pattern.properties) != 0 || pattern.labelErr != nil {
		return nil, false, nil
	}
	for _, row := range rows {
		if _, bound := row[pattern.variable]; bound {
			return nil, false, nil
		}
	}
	property, probeExpression := correlatedEqualityConjunct(whereClause, pattern.variable)
	if property == "" {
		return nil, false, nil
	}
	candidateHint := hint
	candidateHint.earlyLimit = -1
	candidates, _, err := e.collectPipelineInitialNodeCandidates(ctx, pattern, "", candidateHint)
	if err != nil {
		return nil, true, err
	}
	groups := make(map[string][]int, len(candidates))
	var unkeyed []int
	for index, node := range candidates {
		if node == nil {
			continue
		}
		value, present := node.Properties[property]
		if !present || value == nil {
			continue // null equals nothing
		}
		if key, keyed := correlatedEqualityKey(value); keyed {
			groups[key] = append(groups[key], index)
		} else {
			unkeyed = append(unkeyed, index)
		}
	}
	all := make([]int, len(candidates))
	for index := range all {
		all[index] = index
	}
	var plan *rowPredicatePlan
	if planned := planRowPredicate(whereClause); planned != nil && planned.complete {
		plan = planned
	}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		selected := all
		if value, evaluated := e.evaluateRowExpressionWithContext(ctx, probeExpression, row); evaluated {
			if value == nil {
				selected = nil
			} else if key, keyed := correlatedEqualityKey(value); keyed {
				selected = mergeCandidateIndexes(groups[key], unkeyed)
			}
		}
		if failure := getExpressionFailure(ctx); failure != nil {
			return nil, true, failure
		}
		probe := make(pipelineRow, len(row)+1)
		for name, value := range row {
			probe[name] = value
		}
		for _, index := range selected {
			node := candidates[index]
			probe[pattern.variable] = node
			if plan != nil {
				if !e.evaluateRowPredicatePlan(ctx, plan, map[string]interface{}(probe)) {
					continue
				}
			} else if !e.evaluateMatchWhereCondition(ctx, whereClause, map[string]interface{}(probe)) {
				continue
			}
			joined := make(pipelineRow, len(row)+1)
			for name, value := range row {
				joined[name] = value
			}
			joined[pattern.variable] = node
			out = append(out, joined)
		}
	}
	return out, true, nil
}

// correlatedEqualityConjunct finds a top-level AND conjunct variable.property =
// expression (either side) whose expression does not read variable.
func correlatedEqualityConjunct(whereClause, variable string) (property, probe string) {
	for _, conjunct := range splitTopLevelAndConjuncts(whereClause) {
		left, right, ok := splitTopLevelEqualityCallSubquery(conjunct)
		if !ok || strings.HasPrefix(right, "~") || strings.HasSuffix(left, "<") || strings.HasSuffix(left, ">") || strings.HasSuffix(left, "!") {
			continue
		}
		for _, side := range [][2]string{{left, right}, {right, left}} {
			if name, prop, isProperty := parseCallSubqueryVarProp(side[0]); isProperty && name == variable && !referencesVariable(side[1], variable) {
				return prop, side[1]
			}
		}
	}
	return "", ""
}

// correlatedEqualityKey groups values Cypher equality can equate: equal values
// share a key (1 and 1.0 too); a value without a key is tested by every row.
func correlatedEqualityKey(value interface{}) (string, bool) {
	number := func(v float64) (string, bool) {
		if math.IsNaN(v) {
			return "", false
		}
		if v == 0 {
			v = 0 // -0.0 = 0.0
		}
		return "n" + strconv.FormatFloat(v, 'g', -1, 64), true
	}
	switch typed := value.(type) {
	case string:
		return "s" + typed, true
	case bool:
		if typed {
			return "b1", true
		}
		return "b0", true
	case int64:
		return number(float64(typed))
	case int:
		return number(float64(typed))
	case float64:
		return number(typed)
	}
	return "", false
}

// mergeCandidateIndexes merges two ascending index lists.
func mergeCandidateIndexes(left, right []int) []int {
	if len(right) == 0 {
		return left
	}
	merged := make([]int, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) || j < len(right) {
		if j >= len(right) || i < len(left) && left[i] < right[j] {
			merged = append(merged, left[i])
			i++
		} else {
			merged = append(merged, right[j])
			j++
		}
	}
	return merged
}
