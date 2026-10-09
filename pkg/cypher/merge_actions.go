package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mergeClauseActions is a MERGE clause split into its pattern and its
// ON CREATE SET / ON MATCH SET clauses, each kind in the order written.
// Neo4j 5.26 runs repeated actions as separate SET clauses, so m.y = m.x in a
// second ON MATCH SET reads the value the first one set (#907).
type mergeClauseActions struct {
	pattern           string
	onCreate, onMatch mergeActionClauses
}

// mergeActionClauses are the action clauses of one kind, each its "SET ..."
// text as a slice of the MERGE clause. The first two need no allocation.
type mergeActionClauses struct {
	inline   [2]string
	count    int
	overflow []string
}

func (clauses *mergeActionClauses) add(set string) {
	if clauses.count < len(clauses.inline) {
		clauses.inline[clauses.count] = set
	} else {
		clauses.overflow = append(clauses.overflow, set)
	}
	clauses.count++
}

func (clauses mergeActionClauses) len() int { return clauses.count }

// set is the index-th clause's "SET ..." text.
func (clauses mergeActionClauses) set(index int) string {
	if index < len(clauses.inline) {
		return clauses.inline[index]
	}
	return clauses.overflow[index-len(clauses.inline)]
}

// assignments is the index-th clause's assignment list (after SET).
func (clauses mergeActionClauses) assignments(index int) string {
	return strings.TrimSpace(clauses.set(index)[len("SET"):])
}

// setText is every clause as one SET text for pipelineApplySet, each a SET
// of its own ("SET a SET b"); "" for none.
func (clauses mergeActionClauses) setText() string {
	switch clauses.count {
	case 0:
		return ""
	case 1:
		return clauses.inline[0]
	}
	text := clauses.inline[0]
	for index := 1; index < clauses.count; index++ {
		text += " " + clauses.set(index)
	}
	return text
}

// mergeActionAssignments returns the assignments of every clause, in order.
func mergeActionAssignments(clauses mergeActionClauses) []string {
	var assignments []string
	for index := 0; index < clauses.len(); index++ {
		assignments = append(assignments, splitSetAssignments(clauses.assignments(index))...)
	}
	return assignments
}

// splitMergeClauseActions splits the text after MERGE ("pattern [ON CREATE
// SET a] [ON MATCH SET b] ...", any number of actions in any order). It is
// the one parser of MERGE actions: the pipeline applies them
// (pipelineApplyMerge), and the statement analysers and the AST builder read
// them. It allocates only past two clauses of a kind or eight in all.
func splitMergeClauseActions(mergeBody string) mergeClauseActions {
	type action struct {
		position, set int
		onCreate      bool
	}
	var buffer [8]action
	actions := buffer[:0]
	for _, keyword := range [...]string{"ON CREATE SET", "ON MATCH SET"} {
		for from := 0; ; {
			position := keywordIndexFromDefault(mergeBody, keyword, from)
			if position < 0 {
				break
			}
			end, _ := keywordSpanAt(mergeBody, position, keyword)
			current := action{position: position, set: end - len("SET"), onCreate: keyword == "ON CREATE SET"}
			// Insertion keeps them in text order.
			actions = append(actions, current)
			for index := len(actions) - 1; index > 0 && actions[index-1].position > current.position; index-- {
				actions[index], actions[index-1] = actions[index-1], actions[index]
			}
			from = end
		}
	}
	if len(actions) == 0 {
		return mergeClauseActions{pattern: strings.TrimSpace(mergeBody)}
	}
	parts := mergeClauseActions{pattern: strings.TrimSpace(mergeBody[:actions[0].position])}
	for index, current := range actions {
		stop := len(mergeBody)
		if index+1 < len(actions) {
			stop = actions[index+1].position
		}
		set := strings.TrimSpace(mergeBody[current.set:stop])
		if current.onCreate {
			parts.onCreate.add(set)
		} else {
			parts.onMatch.add(set)
		}
	}
	return parts
}

// applyMergeActions applies a MERGE's ON CREATE or ON MATCH SET text
// (mergeActionClauses.setText) to the rows the MERGE produced, through the shared SET
// applier, and adds what it wrote to stats.
func (e *StorageExecutor) applyMergeActions(ctx context.Context, rows []pipelineRow, actions string, stats *QueryStats) error {
	if actions == "" {
		return nil
	}
	setStats, ok, err := e.pipelineApplySet(ctx, rows, actions)
	if err != nil {
		return err
	}
	if !ok {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeInvalidAction(actions))
	}
	addQueryStats(stats, setStats)
	return nil
}

// isMultiRelationshipPattern reports whether pattern (after an optional path
// assignment) is a path of more than one relationship.
func (e *StorageExecutor) isMultiRelationshipPattern(pattern string) bool {
	// Fewer than two relationship brackets: one relationship at most, no
	// parse needed.
	if open, _ := firstRelationshipBracket(pattern); open < 0 || strings.Count(pattern[open+1:], "[") == 0 {
		return false
	}
	return len(mergePathSegments(pattern)) > 1
}

// mergePathSegments splits a MERGE pattern (after an optional path
// assignment) into its single-relationship segments, node to node:
// (a)-[:R]->(b)-[:S]-(c) is (a)-[:R]->(b) and (b)-[:S]-(c). Each segment is
// read by the relationship MERGE shape owner (parseMergeRelationshipShape),
// so every direction, undirected included, reads as in a single-relationship
// MERGE. nil for text whose brackets don't match.
func mergePathSegments(pattern string) []string {
	_, body := parseCreatePathAssignment(pattern)
	var nodes [][2]int
	for index := 0; index < len(body); index++ {
		// findMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch character := body[index]; character {
		case '(', '[':
			opener, closer := rune(character), ')'
			if character == '[' {
				closer = ']'
			}
			end := findMatchingDelimiter(body, index, opener, closer)
			if end < 0 {
				return nil
			}
			if character == '(' {
				nodes = append(nodes, [2]int{index, end})
			}
			index = end
		}
	}
	if len(nodes) < 2 {
		return nil
	}
	segments := make([]string, 0, len(nodes)-1)
	for index := 0; index+1 < len(nodes); index++ {
		segments = append(segments, body[nodes[index][0]:nodes[index+1][1]+1])
	}
	return segments
}

// directedMergePath is pattern with each undirected relationship
// (a)-[:R]-(b) written left to right, (a)-[:R]->(b): MERGE creates an
// undirected relationship in that direction, as the single-relationship
// MERGE does (parseMergeRelationshipShape), while CREATE needs a direction.
func directedMergePath(pattern string) string {
	var builder strings.Builder
	builder.Grow(len(pattern) + 4)
	last := 0
	for index := 0; index < len(pattern); index++ {
		// findMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch pattern[index] {
		case '(':
			if end := findMatchingDelimiter(pattern, index, '(', ')'); end > index {
				index = end
			}
		case '[':
			end := findMatchingDelimiter(pattern, index, '[', ']')
			if end < 0 {
				return pattern
			}
			before := strings.TrimRight(pattern[:index], " \t\r\n")
			after := strings.TrimLeft(pattern[end+1:], " \t\r\n")
			if !strings.HasSuffix(before, "<-") && strings.HasPrefix(after, "-(") {
				builder.WriteString(pattern[last : end+1])
				builder.WriteString("->")
				last = len(pattern) - len(after) + 1
			}
			index = end
		}
	}
	if last == 0 {
		return pattern
	}
	builder.WriteString(pattern[last:])
	return builder.String()
}

// pipelineMergePath runs, for one row, a MERGE of a path with more than one
// relationship as Neo4j 5.26 does: the whole path matches (every match is a
// row) or the whole path is created, reusing the row's bound variables
// (#907). created reports which. A null or NaN property in the pattern is a
// SemanticError, as for a single relationship (validateMergePatternProperties).
func (e *StorageExecutor) pipelineMergePath(ctx context.Context, row pipelineRow, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (rows []pipelineRow, stats *QueryStats, created bool, err error) {
	for _, segment := range mergePathSegments(pattern) {
		shape, shapeErr := e.parseMergeRelationshipShape(segment)
		if shapeErr != nil {
			return nil, nil, false, shapeErr
		}
		for _, content := range [...]string{shape.startContent, shape.endContent} {
			if _, properties := splitNodePatternProperties("(" + content + ")"); properties != "" {
				if err := validateMergePatternProperties(e.parseMergeProperties(ctx, properties, nodeContext, relContext), "node"); err != nil {
					return nil, nil, false, err
				}
			}
		}
		if shape.relProps != "" {
			if err := validateMergePatternProperties(e.parseMergeProperties(ctx, shape.relProps, nodeContext, relContext), "relationship"); err != nil {
				return nil, nil, false, err
			}
		}
	}
	matched, err := e.pipelineMatchRows(ctx, row, "MATCH "+pattern)
	if err != nil {
		return nil, nil, false, err
	}
	if len(matched) > 0 {
		return matched, nil, false, nil
	}
	// One row: the create reports not-ok only with an error.
	createdRows, result, _, err := e.pipelineApplyCreateClauses(ctx, []pipelineRow{row}, []pipelineClause{{kind: pipelineClauseCreate, text: "CREATE " + directedMergePath(pattern)}})
	if err != nil {
		if !mergeCreateConflict(err) {
			return nil, nil, false, err
		}
		// A concurrent MERGE created part of the path first: match again,
		// as the node and relationship MERGE routes recover (mergeCreateConflict).
		recovered, matchErr := e.pipelineMatchRows(ctx, row, "MATCH "+pattern)
		if matchErr != nil || len(recovered) == 0 {
			return nil, nil, false, err
		}
		return recovered, nil, false, nil
	}
	return createdRows, result.Stats, true, nil
}
