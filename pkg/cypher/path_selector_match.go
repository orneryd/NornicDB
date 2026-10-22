package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// pathSelector is a selective path selector as the statement rewrite
// writes it (path_selector.go): ANY k or SHORTEST k [GROUPS], with the path
// mode.
type pathSelector struct {
	// shortest is SHORTEST: paths are taken by increasing length. ANY takes
	// any k paths; NornicDB takes the shortest, which are some of them.
	shortest bool
	// count is the count as written: digits or a parameter.
	count string
	// groups takes every path of each of the count shortest lengths.
	groups bool
	// acyclic keeps only the paths whose nodes are all distinct.
	acyclic bool
}

// splitPathSelectorTerm splits the WHERE of a selected pattern into its
// selector, the selector's predicate (the pattern's own predicates) and the
// clause's WHERE conjuncts. ok is false when where doesn't start with a
// selector.
func splitPathSelectorTerm(where string) (*pathSelector, string, []string, bool) {
	if len(where) < len(pathSelectorFunction) || !equalFoldASCII(where[:len(pathSelectorFunction)], pathSelectorFunction) {
		return nil, "", nil, false
	}
	terms := splitTopLevelAndConjuncts(where)
	call := strings.TrimSpace(terms[0])
	open := strings.IndexByte(call, '(')
	if open < 0 || findMatchingParen(call, open) != len(call)-1 {
		return nil, "", nil, false
	}
	args := splitTopLevelComma(call[open+1 : len(call)-1])
	if len(args) < 5 {
		return nil, "", nil, false
	}
	unquote := func(text string) string { return strings.Trim(strings.TrimSpace(text), "'") }
	selector := &pathSelector{
		shortest: unquote(args[0]) == "SHORTEST",
		count:    strings.TrimSpace(args[1]),
		groups:   strings.TrimSpace(args[2]) == "true",
		acyclic:  unquote(args[3]) == "ACYCLIC",
	}
	predicate := strings.TrimSpace(strings.Join(args[4:], ","))
	if strings.EqualFold(predicate, "true") {
		predicate = ""
	}
	return selector, predicate, terms[1:], true
}

// parseSelectedPathMatch reads a selected pattern, written
// p = shortestPath(pattern) by the statement rewrite, for the selected path
// search (path_selector_search.go). The pattern's own predicates constrain
// the paths to select from; the clause's WHERE conjuncts that read only the
// endpoints select the pairs, and the others filter the selected paths.
func (e *StorageExecutor) parseSelectedPathMatch(pattern, pathVariable string, others []string, selector *pathSelector, predicate string, terms []string) (*shortestPathMatch, bool, error) {
	return e.parseSelectedPathMatchContext(context.Background(), pattern, pathVariable, others, selector, predicate, terms)
}

func (e *StorageExecutor) parseSelectedPathMatchContext(ctx context.Context, pattern, pathVariable string, others []string, selector *pathSelector, predicate string, terms []string) (*shortestPathMatch, bool, error) {
	if len(others) > 0 || pathVariable == "" {
		// The statement rewrite names the path and allows no other pattern
		// (path_selector.go); a hand-written selector call may do neither.
		return nil, true, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorMultiplePatterns())
	}
	plan, filters, ok := compileSelectedPathPlanContext(ctx, pattern, pathVariable, predicate, terms)
	if ctx.Err() != nil {
		return nil, true, ctx.Err()
	}
	if !ok {
		return nil, true, errSelectedPathPattern(pattern)
	}
	// The compiled plan is shared (compileSelectedPathPlan): ACYCLIC goes on
	// a copy.
	if selector.acyclic {
		acyclic := *plan
		acyclic.acyclic = true
		plan = &acyclic
	}
	return &shortestPathMatch{
		selector:      selector,
		pathVariable:  pathVariable,
		pattern:       pattern,
		plan:          plan,
		startVariable: plan.start.variable,
		endVariable:   plan.end.variable,
		postWhere:     strings.Join(filters, " AND "),
	}, true, nil
}

// pipelineApplySelectedPathMatch runs a selected pattern's MATCH for each
// row: the selector's paths for each start and end node pair, filtered by
// the clause's path conjuncts. An OPTIONAL MATCH keeps a row that selects
// nothing, with the clause's new variables null.
func (e *StorageExecutor) pipelineApplySelectedPathMatch(ctx context.Context, rows []pipelineRow, m *shortestPathMatch, optional bool) ([]pipelineRow, error) {
	count, err := pathSelectorCount(ctx, m.selector.count)
	if err != nil {
		return nil, err
	}
	out := make([]pipelineRow, 0, len(rows))
	for batch, batchErr := range materializedBatches(ctx, len(rows)) {
		if batchErr != nil {
			return nil, batchErr
		}
		for _, row := range rows[batch.start:batch.end] {
			selected, err := e.selectPaths(ctx, m, row, count)
			if err != nil {
				return nil, err
			}
			found := 0
			for innerBatch, innerErr := range materializedBatches(ctx, len(selected)) {
				if innerErr != nil {
					return nil, innerErr
				}
				for _, bound := range selected[innerBatch.start:innerBatch.end] {
					if m.postWhere != "" && !e.evaluateWithWhereCondition(ctx, m.postWhere, map[string]interface{}(bound)) {
						if err := getExpressionFailure(ctx); err != nil {
							return nil, err
						}
						continue
					}
					out = append(out, bound)
					found++
				}
			}
			if optional && found == 0 {
				out = append(out, m.unmatchedRow(row))
			}
		}
	}
	return out, nil
}

// pathSelectorCount is a selector's count: its digits, or its parameter's
// value, which must be a positive integer, as in Neo4j. A parameter may
// reach the step as its value's literal.
func pathSelectorCount(ctx context.Context, count string) (int, error) {
	var value interface{}
	if strings.HasPrefix(count, "$") {
		value = getParamsFromContext(ctx)[strings.Trim(count[1:], "`")]
	} else {
		value = pathSelectorCountLiteral(count)
	}
	var number int64
	switch typed := value.(type) {
	case int64:
		number = typed
	case int:
		number = int64(typed)
	case int32:
		number = int64(typed)
	default:
		return 0, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherMatchingPathSelectorCountType(pathSelectorCountTypeName(value)))
	}
	if number <= 0 {
		return 0, localizedStatusError("Neo.ClientError.General.InvalidArguments", "InvalidArguments",
			localization.CypherMatchingPathSelectorCountInvalid(strconv.FormatInt(number, 10)))
	}
	return int(number), nil
}

// pathSelectorCountLiteral is the value of a count literal: an integer,
// null, a float, a string, a boolean, a list or a map (only the type of
// those that aren't integers matters).
func pathSelectorCountLiteral(text string) interface{} {
	text = strings.TrimSpace(text)
	if number, err := strconv.ParseInt(text, 10, 64); err == nil {
		return number
	}
	if _, err := strconv.ParseFloat(text, 64); err == nil {
		return float64(0)
	}
	switch {
	case strings.EqualFold(text, "null"), text == "":
		return nil
	case strings.EqualFold(text, "true"), strings.EqualFold(text, "false"):
		return true
	case strings.HasPrefix(text, "["):
		return []interface{}{}
	case strings.HasPrefix(text, "{"):
		return map[string]interface{}{}
	}
	return text
}

// pathSelectorCountTypeName is Neo4j's name for the type of a count that
// isn't an integer.
func pathSelectorCountTypeName(value interface{}) string {
	switch value.(type) {
	case nil:
		return "NO_VALUE"
	case float64, float32:
		return "Double"
	case string:
		return "String"
	case bool:
		return "Boolean"
	case []interface{}:
		return "List"
	case map[string]interface{}:
		return "Map"
	}
	return "Any"
}
