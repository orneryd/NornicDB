package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// mergeActionSpans optionally retains accepted offsets from the same parser.
// Ordinary execution keeps the original compact string product; advisory raw
// source consumers supply this stack-owned output to retain exact provenance.
type mergeActionSpans struct {
	pattern           cyphertext.Span
	onCreate, onMatch mergeClauseSpans
}

type mergeClauseSpans struct {
	inline   [2]cyphertext.Span
	overflow []cyphertext.Span
}

func (spans *mergeClauseSpans) add(index int, span cyphertext.Span) {
	if index < len(spans.inline) {
		spans.inline[index] = span
	} else {
		spans.overflow = append(spans.overflow, span)
	}
}

func mergeSourceSpan(text string, base int) cyphertext.Span {
	start := len(text) - len(strings.TrimLeftFunc(text, cyphertext.IsWhitespace))
	return cyphertext.Span{Start: base + start, End: base + max(start, len(strings.TrimRightFunc(text, cyphertext.IsWhitespace)))}
}

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

// assignments is the index-th clause's assignment list (after SET).
func (clauses mergeActionClauses) assignments(index int) string {
	return strings.TrimSpace(inlineOverflowAt(clauses.inline[:], clauses.overflow, index)[len("SET"):])
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
		text += " " + inlineOverflowAt(clauses.inline[:], clauses.overflow, index)
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
func splitMergeClauseActions(mergeBody string, spans *mergeActionSpans) mergeClauseActions {
	if spans != nil {
		*spans = mergeActionSpans{}
	}
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
		if spans != nil {
			spans.pattern = mergeSourceSpan(mergeBody, 0)
		}
		return mergeClauseActions{pattern: strings.TrimSpace(mergeBody)}
	}
	parts := mergeClauseActions{pattern: strings.TrimSpace(mergeBody[:actions[0].position])}
	if spans != nil {
		spans.pattern = mergeSourceSpan(mergeBody[:actions[0].position], 0)
	}
	for index, current := range actions {
		stop := len(mergeBody)
		if index+1 < len(actions) {
			stop = actions[index+1].position
		}
		set := strings.TrimSpace(mergeBody[current.set:stop])
		if current.onCreate {
			if spans != nil {
				spans.onCreate.add(parts.onCreate.len(), mergeSourceSpan(mergeBody[current.set:stop], current.set))
			}
			parts.onCreate.add(set)
		} else {
			if spans != nil {
				spans.onMatch.add(parts.onMatch.len(), mergeSourceSpan(mergeBody[current.set:stop], current.set))
			}
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
	if open, _ := readRelationshipBracket(pattern, 0, requireClosedRelationship); open < 0 || strings.Count(pattern[open+1:], "[") == 0 {
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
		// cyphertext.FindMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch character := body[index]; character {
		case '(', '[':
			opener, closer := rune(character), ')'
			if character == '[' {
				closer = ']'
			}
			end := cyphertext.FindMatchingDelimiter(body, index, opener, closer, cyphertext.OpeningDelimiter)
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
		// cyphertext.FindMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch pattern[index] {
		case '(':
			if end := cyphertext.FindMatchingDelimiter(pattern, index, '(', ')', cyphertext.OpeningDelimiter); end > index {
				index = end
			}
		case '[':
			end := cyphertext.FindMatchingDelimiter(pattern, index, '[', ']', cyphertext.OpeningDelimiter)
			if end < 0 {
				return pattern
			}
			before := strings.TrimRightFunc(pattern[:index], cyphertext.IsWhitespace)
			after := strings.TrimLeftFunc(pattern[end+1:], cyphertext.IsWhitespace)
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
	// The match is the statement rewrite's MATCH: a property map reading
	// the pattern's own variables (b {k: a.k}) is a WHERE predicate there.
	match, _, err := desugarLabelExpressions("MATCH "+pattern, nil, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil, nil, false, err
	}
	// Syntax rewriting moves maps that read this path's variables into
	// predicates/assignments. Materialize independent identity maps now; the
	// dependent assignments use the checked SET owner after the path binds.
	identityPattern := strings.TrimSpace(match[len("MATCH"):])
	identityTail := ""
	if where := topLevelKeywordIndex(identityPattern, "WHERE"); where >= 0 {
		identityTail = identityPattern[where:]
		identityPattern = strings.TrimSpace(identityPattern[:where])
	}
	ctx, identityPattern, err = e.bindPatternProperties(ctx, identityPattern, row)
	if err != nil {
		return nil, nil, false, err
	}
	match = "MATCH " + identityPattern
	if identityTail != "" {
		match += " " + identityTail
	}
	for index, segment := range mergePathSegments(identityPattern) {
		shape, shapeErr := e.parseMergeRelationshipShape(segment)
		if shapeErr != nil {
			return nil, nil, false, shapeErr
		}
		for side, content := range [...]string{shape.startContent, shape.endContent} {
			if index > 0 && side == 0 {
				continue // the previous segment's end node, already checked and admitted
			}
			variable, labels, properties, parseErr := e.parseMergeNodePattern(ctx, "("+content+")", nodeContext, relContext)
			if parseErr != nil {
				return nil, nil, false, parseErr
			}
			if err := validateMergePatternProperties(properties, "node"); err != nil {
				return nil, nil, false, err
			}
			if nodeContext[variable] == nil {
				if err := prepareMergeKeys(ctx, e.getStorage(ctx), labels, properties, mergeNodeWritesFromContext(ctx).writes(variable)); err != nil {
					return nil, nil, false, err
				}
			}
		}
		if shape.relProps != "" {
			evaluated, err := e.parseMergeProperties(ctx, shape.relProps, nodeContext, relContext)
			if err != nil {
				return nil, nil, false, err
			}
			if err := validateMergePatternProperties(evaluated, "relationship"); err != nil {
				return nil, nil, false, err
			}
		}
	}
	// MERGE identity is stored identity, unlike the visible inline MATCH view.
	// A private executor keeps this distinction local to the identity lookup;
	// returned bindings remain raw for writes and the outer query projects them.
	identity := e.cloneWithStorage(e.storage)
	identity.storedPropertyIdentity = true
	matched, err := identity.pipelineMatchRows(ctx, row, match)
	if err != nil {
		return nil, nil, false, err
	}
	if len(matched) > 0 {
		return matched, nil, false, nil
	}
	// The created path: a map entry reading the pattern's own variables
	// (the match's WHERE, the only predicates a MERGE pattern can have) is
	// set once its elements exist, as Neo4j's MERGE evaluates it; CREATE
	// itself can't read an element it creates.
	createPattern, assignments := identityPattern, ""
	if where := topLevelKeywordIndex(match, "WHERE"); where >= 0 {
		createPattern = strings.TrimSpace(match[len("MATCH"):where])
		assignments = strings.Join(splitTopLevelAndConjuncts(match[where+len("WHERE"):]), ", ")
	}
	// One row: the create reports not-ok only with an error.
	createdRows, result, _, err := e.pipelineApplyCreateClauses(ctx, []pipelineRow{row}, []pipelineClause{{kind: pipelineClauseCreate, text: "CREATE " + directedMergePath(createPattern)}})
	if err == nil && assignments != "" {
		err = e.applyMergeActions(ctx, createdRows, "SET "+assignments, result.Stats)
	}
	if err != nil {
		if !mergeCreateConflict(err) {
			return nil, nil, false, err
		}
		// A concurrent MERGE created part of the path first: match again,
		// as the node and relationship MERGE routes recover (mergeCreateConflict).
		recovered, matchErr := identity.pipelineMatchRows(ctx, row, match)
		if matchErr != nil || len(recovered) == 0 {
			return nil, nil, false, err
		}
		return recovered, nil, false, nil
	}
	return createdRows, result.Stats, true, nil
}
