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

// mergeSourceActions projects extents produced by the existing action grammar.
// Consumers retain these views; no emitted SET text is re-admitted.
// Author: #1663 implementation lane.
type mergeSourceActions struct {
	pattern           braceView
	before, after     braceView
	onCreate, onMatch []braceView
}

func readMergeSourceActions(clause braceView) mergeSourceActions {
	clause = clause.trim()
	body := clause.clauseBody("MERGE")
	var spans mergeActionSpans
	parts := splitMergeClauseActions(body, &spans)
	project := func(clauses mergeActionClauses, positions mergeClauseSpans) []braceView {
		var result []braceView
		for i := 0; i < clauses.len(); i++ {
			result = append(result, body.spanWindow(inlineOverflowAt(positions.inline[:], positions.overflow, i)))
		}
		return result
	}
	base := len(clause.text()) - len(body.text())
	return mergeSourceActions{pattern: body.spanWindow(spans.pattern), before: clause.window(0, base+spans.pattern.Start), after: clause.window(base+spans.pattern.End, len(clause.text())), onCreate: project(parts.onCreate, spans.onCreate), onMatch: project(parts.onMatch, spans.onMatch)}
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

// viewTexts returns each view's text, for consumers whose values are strings.
func viewTexts(views []braceView) []string {
	texts := make([]string, len(views))
	for index, view := range views {
		texts[index] = view.text()
	}
	return texts
}

// Author: #1663 lane. Assignments retain the extents of each original action.
func mergeSourceActionAssignments(clauses []braceView) []braceView {
	var assignments []braceView
	for _, clause := range clauses {
		assignments = append(assignments, setAssignmentViews(clause.clauseBody("SET"))...)
	}
	return assignments
}

// splitMergeClauseActions splits the text after MERGE ("pattern [ON CREATE
// SET a] [ON MATCH SET b] ...", any number of actions in any order). It is
// the one parser of MERGE actions: the pipeline applies them
// (pipelineApplyMerge), and the statement analysers and the AST builder read
// them. It allocates only past two clauses of a kind or eight in all.
func splitMergeClauseActions(body braceView, spans *mergeActionSpans) mergeClauseActions {
	mergeBody := body.text()
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
			position := keywordIndexFromDefault(body, keyword, from)
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

// applyMergeActions applies a MERGE's ON CREATE or ON MATCH SET clauses
// to the rows the MERGE produced, through the shared SET
// applier, and adds what it wrote to stats.
func (e *StorageExecutor) applyMergeActions(ctx context.Context, rows []pipelineRow, actions braceView, stats *QueryStats) error {
	if actions.empty() {
		return nil
	}
	setStats, ok, err := e.pipelineApplySet(ctx, rows, actions)
	if err != nil {
		return err
	}
	if !ok {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherMergeInvalidAction(actions.text()))
	}
	addQueryStats(stats, setStats)
	return nil
}

// isMultiRelationshipPattern reports whether pattern (after an optional path
// assignment) is a path of more than one relationship.
func (e *StorageExecutor) isMultiRelationshipPattern(pattern braceView) bool {
	// Fewer than two relationship brackets: one relationship at most, no
	// parse needed.
	if open, _ := readRelationshipBracket(pattern, 0, requireClosedRelationship); open < 0 || strings.Count(pattern.text()[open+1:], "[") == 0 {
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
func mergePathSegments(pattern braceView) []braceView {
	_, body := parseCreatePathAssignment(pattern)
	text := body.text()
	var nodes [][2]int
	for index := 0; index < len(text); index++ {
		// cyphertext.FindMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch character := text[index]; character {
		case '(', '[':
			opener, closer := rune(character), ')'
			if character == '[' {
				closer = ']'
			}
			end := cyphertext.FindMatchingDelimiter(text, index, opener, closer, cyphertext.OpeningDelimiter)
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
	segments := make([]braceView, 0, len(nodes)-1)
	for index := 0; index+1 < len(nodes); index++ {
		segments = append(segments, body.window(nodes[index][0], nodes[index+1][1]+1))
	}
	return segments
}

// directedMergePath is pattern with each undirected relationship
// (a)-[:R]-(b) written left to right, (a)-[:R]->(b): MERGE creates an
// undirected relationship in that direction, as the single-relationship
// MERGE does (parseMergeRelationshipShape), while CREATE needs a direction.
func directedMergePath(pattern braceView) braceView {
	text := pattern.text()
	var pieces []cyphertext.SourcePiece
	last := 0
	for index := 0; index < len(text); index++ {
		// cyphertext.FindMatchingDelimiter skips quoted text and comments inside the
		// brackets; a valid pattern has none outside them.
		switch text[index] {
		case '(':
			if end := cyphertext.FindMatchingDelimiter(text, index, '(', ')', cyphertext.OpeningDelimiter); end > index {
				index = end
			}
		case '[':
			end := cyphertext.FindMatchingDelimiter(text, index, '[', ']', cyphertext.OpeningDelimiter)
			if end < 0 {
				return pattern
			}
			before := strings.TrimRightFunc(text[:index], cyphertext.IsWhitespace)
			after := strings.TrimLeftFunc(text[end+1:], cyphertext.IsWhitespace)
			if !strings.HasSuffix(before, "<-") && strings.HasPrefix(after, "-(") {
				pieces = append(pieces, pattern.window(last, end+1).piece(), generatedPiece("->"))
				last = len(text) - len(after) + 1
			}
			index = end
		}
	}
	if last == 0 {
		return pattern
	}
	pieces = append(pieces, pattern.window(last, len(text)).piece())
	return pattern.assemble(pieces...)
}

// pipelineMergePath runs, for one row, a MERGE of a path with more than one
// relationship as Neo4j 5.26 does: the whole path matches (every match is a
// row) or the whole path is created, reusing the row's bound variables
// (#907). created reports which. A null or NaN property in the pattern is a
// SemanticError, as for a single relationship (validateMergePatternProperties).
func (e *StorageExecutor) pipelineMergePath(ctx context.Context, row pipelineRow, pattern braceView, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (rows []pipelineRow, stats *QueryStats, created bool, err error) {
	// The match is the statement rewrite's MATCH: a property map reading
	// the pattern's own variables (b {k: a.k}) is a WHERE predicate there.
	wrapped := pattern.wrap("MATCH ", "")
	_, rewrite, err := desugarLabelExpressionView(wrapped, nil, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return nil, nil, false, err
	}
	desugared := wrapped.rewrite(rewrite)
	// Syntax rewriting moves maps that read this path's variables into
	// predicates/assignments. Materialize independent identity maps now; the
	// dependent assignments use the checked SET owner after the path binds.
	identityPattern := afterWord(desugared, "MATCH")
	identityTail := identityPattern.window(0, 0)
	if where := topLevelKeywordIndex(identityPattern, "WHERE"); where >= 0 {
		identityTail = identityPattern.window(where, len(identityPattern.text()))
		identityPattern = identityPattern.window(0, where).trim()
	}
	ctx, identityPattern, err = e.bindPatternProperties(ctx, identityPattern, row)
	if err != nil {
		return nil, nil, false, err
	}
	match := identityPattern.wrap("MATCH ", "")
	if identityTail.text() != "" {
		match = desugared.assemble(generatedPiece("MATCH "), identityPattern.piece(), generatedPiece(" "), identityTail.piece())
	}
	for index, segment := range mergePathSegments(identityPattern) {
		shape, shapeErr := e.parseMergeRelationshipShape(segment)
		if shapeErr != nil {
			return nil, nil, false, shapeErr
		}
		for side, content := range [...]braceView{shape.startContent, shape.endContent} {
			if index > 0 && side == 0 {
				continue // the previous segment's end node, already checked and admitted
			}
			variable, labels, properties, parseErr := e.parseMergeNodePattern(ctx, content.wrap("(", ")"), nodeContext, relContext)
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
		if !shape.relProps.empty() {
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
	createPattern, assignments := identityPattern, match.window(0, 0)
	if where := topLevelKeywordIndex(match, "WHERE"); where >= 0 {
		createPattern = match.window(len("MATCH"), where).trim()
		assignments = match.assembleCode(splitTopLevelAndConjuncts(match.window(where+len("WHERE"), len(match.text()))), ", ")
	}
	create := directedMergePath(createPattern).wrap("CREATE ", "")
	// One row: the create reports not-ok only with an error.
	createdRows, result, _, err := e.pipelineApplyCreateClauses(ctx, []pipelineRow{row}, []pipelineClause{{kind: pipelineClauseCreate, text: create.text(), view: create}})
	if err == nil && assignments.text() != "" {
		err = e.applyMergeActions(ctx, createdRows, assignments.wrap("SET ", ""), result.Stats)
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
