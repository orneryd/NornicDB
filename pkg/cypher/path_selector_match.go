package cypher

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
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
// p = shortestPath(pattern) by the statement rewrite. A pattern of one
// relationship (variable-length or not) between two node patterns is
// searched from each start to each end; any other is matched whole. The
// pattern's own predicates constrain the paths to select from; the clause's
// WHERE conjuncts that read only the endpoints or earlier variables select
// the endpoints, and the others filter the selected paths.
func (e *StorageExecutor) parseSelectedPathMatch(ctx context.Context, pattern, pathVariable string, others []string, selector *pathSelector, predicate string, terms []string) (*shortestPathMatch, bool, error) {
	if len(others) > 0 || pathVariable == "" {
		// The statement rewrite names the path and allows no other pattern
		// (path_selector.go); a hand-written selector call may do neither.
		return nil, true, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorMultiplePatterns())
	}
	m := &shortestPathMatch{selector: selector, pathVariable: pathVariable}
	startPattern, endPattern, ok := shortestPathEndpointPatterns(pattern)
	var traversal *TraversalMatch
	if ok {
		traversal = e.parseTraversalPattern(ctx, pattern)
	}
	if traversal == nil || traversal.IsChained {
		m.pattern = pattern
		m.pathWhere = predicate
		m.postWhere = strings.Join(terms, " AND ")
		return m, true, nil
	}
	m.traversal = traversal
	m.startPattern, m.endPattern = startPattern, endPattern
	m.startVariable, m.endVariable = traversal.StartNode.variable, traversal.EndNode.variable
	if m.startVariable == "" {
		m.startVariable = generatedVariablePrefix + "sp_start"
		m.startPattern = "(" + m.startVariable + m.startPattern[1:]
		traversal.StartNode.variable = m.startVariable
	}
	if m.endVariable == "" {
		m.endVariable = generatedVariablePrefix + "sp_end"
		m.endPattern = "(" + m.endVariable + m.endPattern[1:]
		traversal.EndNode.variable = m.endVariable
	}
	traversal.PathVariable = m.pathVariable
	readsPath := func(term string) bool {
		relationship := traversal.Relationship.Variable
		return referencesVariable(term, m.pathVariable) || relationship != "" && referencesVariable(term, relationship)
	}
	var endpointTerms, pathTerms, postTerms []string
	for _, term := range splitTopLevelAndConjuncts(predicate) {
		if term = strings.TrimSpace(term); term == "" {
			continue
		}
		if readsPath(term) {
			pathTerms = append(pathTerms, term)
		} else {
			endpointTerms = append(endpointTerms, term)
		}
	}
	for _, term := range terms {
		if term = strings.TrimSpace(term); readsPath(term) {
			postTerms = append(postTerms, term)
		} else {
			endpointTerms = append(endpointTerms, term)
		}
	}
	m.endpointWhere = strings.Join(endpointTerms, " AND ")
	m.pathWhere = strings.Join(pathTerms, " AND ")
	m.postWhere = strings.Join(postTerms, " AND ")
	return m, true, nil
}

// pipelineApplySelectedPathMatch runs a selected pattern's MATCH for each
// row: for each start and end node pair, the selector's paths, filtered by
// the clause's path conjuncts. An OPTIONAL MATCH keeps a row that selects
// nothing, with the clause's new variables null.
func (e *StorageExecutor) pipelineApplySelectedPathMatch(ctx context.Context, rows []pipelineRow, m *shortestPathMatch, optional bool) ([]pipelineRow, error) {
	count, err := pathSelectorCount(ctx, m.selector.count)
	if err != nil {
		return nil, err
	}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		var selected []pipelineRow
		if m.pattern != "" {
			selected, err = e.selectMatchedPaths(ctx, m, row, count)
		} else {
			selected, err = e.selectSearchedPaths(ctx, m, row, count)
		}
		if err != nil {
			return nil, err
		}
		found := 0
		for _, bound := range selected {
			if m.postWhere != "" && !e.evaluateWithWhereCondition(ctx, m.postWhere, map[string]interface{}(bound)) {
				if err := getExpressionFailure(ctx); err != nil {
					return nil, err
				}
				continue
			}
			out = append(out, bound)
			found++
		}
		if optional && found == 0 {
			out = append(out, m.unmatchedRow(row))
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

// selectSearchedPaths returns, for each start and end pair of one row, the
// row with each selected path bound.
func (e *StorageExecutor) selectSearchedPaths(ctx context.Context, m *shortestPathMatch, row pipelineRow, count int) ([]pipelineRow, error) {
	pairs, err := e.shortestPathEndpointPairs(ctx, m, row)
	if err != nil {
		return nil, err
	}
	var out []pipelineRow
	for _, pair := range pairs {
		start, _ := pair[m.startVariable].(*storage.Node)
		end, _ := pair[m.endVariable].(*storage.Node)
		if start == nil || end == nil {
			continue
		}
		paths, err := e.selectedPathsBetween(ctx, m, start, end, pair, count)
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			out = append(out, e.bindShortestPath(ctx, m, pair, path))
		}
	}
	return out, nil
}

// selectedPathsBetween returns the selector's paths from start to end:
// the paths that satisfy the pattern's predicates, by increasing length,
// until count paths (count lengths for GROUPS) are taken. A start that is
// the end has the path of length 0 when the minimum length is 0, and paths
// that return to it otherwise. Without predicates, one shortest path (or
// all of them, for one group) is the breadth-first search's answer.
func (e *StorageExecutor) selectedPathsBetween(ctx context.Context, m *shortestPathMatch, start, end *storage.Node, row pipelineRow, count int) ([]PathResult, error) {
	relationship := m.traversal.Relationship
	selector := m.selector
	from := relationship.MinHops
	if start.ID != end.ID {
		// A shortest walk repeats no relationship or node, so its length is
		// the least any selected path can have, and with none there is none.
		shortest, err := e.shortestPath(ctx, start, end, relationship.Types, relationship.Direction, relationship.MaxHops)
		if err != nil || shortest == nil {
			return nil, err
		}
		if m.pathWhere == "" && len(relationship.Properties) == 0 && relationship.MinHops <= shortest.Length && count == 1 {
			if !selector.groups {
				return []PathResult{*shortest}, nil
			}
			return e.allShortestPaths(ctx, start, end, relationship.Types, relationship.Direction, relationship.MaxHops)
		}
		from = max(from, shortest.Length)
	}
	var selected []PathResult
	lengths := 0
	for length := from; length <= relationship.MaxHops; length++ {
		var paths []PathResult
		deepest := length
		if length == 0 {
			paths = []PathResult{{Nodes: []*storage.Node{start}}}
		} else {
			search := e.newTraversalContext(ctx, start, &relationship)
			search.minHops, search.maxHops = length, length
			search.endNodeID = end.ID
			paths = e.findPaths(search, start, []*storage.Node{start}, nil, 0, nil)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			deepest = search.deepest
		}
		paths = e.filterPathsByWhere(ctx, paths, m.traversal, m.pathWhere, map[string]interface{}(row))
		if selector.acyclic {
			paths = acyclicPaths(paths)
		}
		if len(paths) > 0 {
			lengths++
			if !selector.groups && len(paths) > count-len(selected) {
				paths = paths[:count-len(selected)]
			}
			selected = append(selected, paths...)
			if selector.groups && lengths >= count || !selector.groups && len(selected) >= count {
				break
			}
		}
		if deepest < length {
			// No path from start is this long, so none is longer.
			break
		}
	}
	return selected, nil
}

// acyclicPaths returns the paths whose nodes are all distinct.
func acyclicPaths(paths []PathResult) []PathResult {
	kept := paths[:0]
	for _, path := range paths {
		if distinctNodeIDs(pathResultNodeIDs(&path)) {
			kept = append(kept, path)
		}
	}
	return kept
}

// selectMatchedPaths matches a selected pattern the search can't run
// (more than one relationship, or none) whole, with its predicates, and
// selects from the paths of each start and end node pair by increasing
// length.
func (e *StorageExecutor) selectMatchedPaths(ctx context.Context, m *shortestPathMatch, row pipelineRow, count int) ([]pipelineRow, error) {
	clause := "MATCH " + m.pathVariable + " = " + m.pattern
	if m.pathWhere != "" {
		clause += " WHERE " + m.pathWhere
	}
	rows, err := e.pipelineMatchRows(ctx, row, clause)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		row    pipelineRow
		length int
	}
	var order []string
	groups := make(map[string][]candidate)
	for _, matched := range rows {
		ids, ok := pathNodeIDs(matched[m.pathVariable])
		if !ok || len(ids) == 0 || m.selector.acyclic && !distinctNodeIDs(ids) {
			continue
		}
		key := string(ids[0]) + "\x00" + string(ids[len(ids)-1])
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], candidate{row: matched, length: len(ids) - 1})
	}
	var out []pipelineRow
	for _, key := range order {
		candidates := groups[key]
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].length < candidates[j].length })
		lengths, taken := 0, 0
		for i, c := range candidates {
			if i == 0 || c.length != candidates[i-1].length {
				lengths++
			}
			if m.selector.groups && lengths > count || !m.selector.groups && taken >= count {
				break
			}
			out = append(out, c.row)
			taken++
		}
	}
	return out, nil
}
