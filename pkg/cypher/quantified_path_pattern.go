package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Quantified path patterns (Neo4j 5.9+, GQL; #907).
//
// A parenthesised path followed by a quantifier repeats it:
//
//	MATCH p = (a:Station)((x)-[r:LINK]->(y) WHERE r.open){1,3}(b:Station)
//
// Each repetition (iteration) is a match of the inner path whose first node
// is the previous iteration's last node; the node before the group is the
// first iteration's first node and the node after it the last iteration's
// last node (with zero iterations they are one node). A variable inside the
// group binds the list of its values, one per iteration; the inner WHERE
// applies to each iteration. No relationship repeats in the clause, unless
// it is a MATCH REPEATABLE ELEMENTS (repeatable_elements.go).
//
// The pipeline's MATCH step runs a pattern part with groups here: the part
// is a sequence of pieces, plain chains and groups, matched left to right
// from the bindings so far (pipelineMatchRows for each chain and each
// iteration). A clause with other parts reaches here one part at a time
// through the pattern product, which keeps relationships distinct across
// parts.

// quantifiedPathPiece is a piece of a pattern part: a chain of node and
// relationship patterns (chain), or a quantified group.
type quantifiedPathPiece struct {
	// chain is the pattern text, starting and ending with a node pattern;
	// for a group, the inner path's.
	chain string
	// first and last are the variables of chain's first and last node
	// patterns (generated for an anonymous one).
	first, last string
	group       bool
	// where is a group's own WHERE; min and max its quantifier's bounds
	// (max < 0: none).
	where    string
	min, max int
	// variables are the variables inside a group, which bind lists.
	variables []string
	// deferred is the part of a group's WHERE that reads variables the
	// pattern binds after the group: each iteration is checked against it
	// once the pattern is matched.
	deferred string
}

// quantifiedPathMatch is a MATCH pattern part with quantified groups.
type quantifiedPathMatch struct {
	pathVariable string
	pieces       []quantifiedPathPiece
	where        string
}

// hasQuantifiedGroup reports whether pattern has a quantified group: a
// parenthesised path followed by a quantifier.
func hasQuantifiedGroup(pattern string) bool {
	if !mayUseQuantifiedGroup(pattern) {
		return false
	}
	_, _, _, ok := nextQuantifiedGroup(pattern, 0)
	return ok
}

// mayUseQuantifiedGroup reports whether text may have a quantified group,
// well formed or not: a + * or { after a ')'. It never answers false for
// one.
func mayUseQuantifiedGroup(text string) bool {
	for i := strings.IndexByte(text, ')'); i >= 0; {
		if next := skipASCIISpaces(text, i+1, len(text)); next < len(text) && strings.IndexByte("+*{", text[next]) >= 0 {
			return true
		}
		following := strings.IndexByte(text[i+1:], ')')
		if following < 0 {
			return false
		}
		i += following + 1
	}
	return false
}

// nextQuantifiedGroup finds the first quantified group at or after
// pattern[from]: its parentheses (open, close) and quantifier.
func nextQuantifiedGroup(pattern string, from int) (int, int, relationshipQuantifier, bool) {
	for i := from; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(pattern, i, c) - 1
		case '[', '{':
			closeBy := map[byte]byte{'[': ']', '{': '}'}[c]
			if close := findMatchingDelimiter(pattern, i, rune(c), rune(closeBy)); close > i {
				i = close
			}
		case '(':
			close := findMatchingParen(pattern, i)
			if close < 0 {
				return 0, 0, relationshipQuantifier{}, false
			}
			if parenthesisedPathAt(pattern, i, close) {
				if quantifier, ok := relationshipQuantifierAt(pattern, skipASCIISpaces(pattern, close+1, len(pattern)), len(pattern)); ok {
					return i, close, quantifier, true
				}
			}
			i = close
		}
	}
	return 0, 0, relationshipQuantifier{}, false
}

// parseQuantifiedPathMatch reads a MATCH body (pattern [WHERE …]) of one
// pattern part with quantified groups. ok is false for any other body.
func parseQuantifiedPathMatch(body string) (*quantifiedPathMatch, bool, error) {
	if !hasQuantifiedGroup(body) {
		return nil, false, nil
	}
	pattern, where := body, ""
	if index := topLevelKeywordIndex(body, "WHERE"); index >= 0 {
		pattern, where = strings.TrimSpace(body[:index]), strings.TrimSpace(body[index+len("WHERE"):])
	}
	if len(splitTopLevelComma(pattern)) != 1 || !hasQuantifiedGroup(pattern) {
		return nil, false, nil
	}
	m := &quantifiedPathMatch{where: where}
	if variable := extractPathAssignmentVariable(pattern); variable != "" {
		m.pathVariable = variable
		pattern = strings.TrimSpace(pattern[strings.IndexByte(pattern, '=')+1:])
	}
	generated := 0
	name := func() string {
		generated++
		return generatedVariablePrefix + "qpp" + strconv.Itoa(generated)
	}
	outside := map[string]bool{}
	for at := 0; at < len(pattern); at = skipASCIISpaces(pattern, at, len(pattern)) {
		open, close, quantifier, grouped := nextQuantifiedGroup(pattern, at)
		if grouped && open == at {
			inner := strings.TrimSpace(pattern[open+1 : close])
			piece := quantifiedPathPiece{group: true, min: quantifier.min, max: quantifier.max}
			if index := topLevelKeywordIndex(inner, "WHERE"); index >= 0 {
				inner, piece.where = strings.TrimSpace(inner[:index]), strings.TrimSpace(inner[index+len("WHERE"):])
			}
			piece.chain, piece.first, piece.last = nameChainEnds(inner, name)
			piece.variables = append(extractNodeVariables(piece.chain), extractRelationshipVariables(piece.chain)...)
			m.pieces = append(m.pieces, piece)
			at = quantifier.end
			continue
		}
		end := len(pattern)
		if grouped {
			end = open
		}
		chain := strings.TrimSpace(pattern[at:end])
		if !strings.HasPrefix(chain, "(") || !strings.HasSuffix(chain, ")") {
			return nil, true, localizedError(localization.CypherMatchingPathPatternInvalid(pattern), nil)
		}
		piece := quantifiedPathPiece{}
		piece.chain, piece.first, piece.last = nameChainEnds(chain, name)
		for _, variable := range append(extractNodeVariables(chain), extractRelationshipVariables(chain)...) {
			outside[variable] = true
		}
		m.pieces = append(m.pieces, piece)
		at = end
	}
	for _, piece := range m.pieces {
		for _, variable := range piece.variables {
			if outside[variable] || variable == m.pathVariable {
				return nil, true, labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathVariableOutside(variable))
			}
		}
	}
	for index := range m.pieces {
		splitDeferredGroupWhere(m.pieces, index)
	}
	return m, true, nil
}

// splitDeferredGroupWhere moves the conjuncts of the group pieces[index]'s
// WHERE that read a variable a later piece binds to its deferred check.
func splitDeferredGroupWhere(pieces []quantifiedPathPiece, index int) {
	piece := &pieces[index]
	if !piece.group || piece.where == "" {
		return
	}
	var later []string
	for _, next := range pieces[index+1:] {
		later = append(later, extractNodeVariables(next.chain)...)
		later = append(later, extractRelationshipVariables(next.chain)...)
	}
	var now, deferred []string
	for _, term := range splitTopLevelAndConjuncts(piece.where) {
		readsLater := false
		for _, variable := range later {
			readsLater = readsLater || referencesVariable(term, variable)
		}
		if readsLater {
			deferred = append(deferred, term)
		} else {
			now = append(now, term)
		}
	}
	piece.where, piece.deferred = strings.Join(now, " AND "), strings.Join(deferred, " AND ")
}

// nameChainEnds returns chain with a variable in its first and last node
// patterns (a generated one where it has none) and those variables.
func nameChainEnds(chain string, name func() string) (string, string, string) {
	lastOpen := lastNodePatternStart(chain)
	named := func(open int) (string, string) {
		variable, _, ok := scanSymbolicName(chain, skipASCIISpaces(chain, open+1, len(chain)))
		if ok {
			return chain, variable
		}
		variable = name()
		return chain[:open+1] + variable + chain[open+1:], variable
	}
	var last string
	if lastOpen > 0 {
		chain, last = named(lastOpen)
	}
	chain, first := named(0)
	if lastOpen <= 0 {
		last = first
	}
	return chain, first, last
}

// lastNodePatternStart is the index of the last top-level node pattern of
// chain, -1 when it has none.
func lastNodePatternStart(chain string) int {
	last := -1
	// A chain's top level holds patterns and arrows; quoted text is inside
	// brackets.
	for i := 0; i < len(chain); i++ {
		switch c := chain[i]; c {
		case '[', '{':
			closeBy := map[byte]byte{'[': ']', '{': '}'}[c]
			if close := findMatchingDelimiter(chain, i, rune(c), rune(closeBy)); close > i {
				i = close
			}
		case '(':
			last = i
			close := findMatchingParen(chain, i)
			if close < 0 {
				return last
			}
			i = close
		}
	}
	return last
}

// quantifiedPathState is the search's position: the bindings so far, the
// path so far, its end node, and the relationships it used.
type quantifiedPathState struct {
	row           pipelineRow
	nodes         []*storage.Node
	relationships []*storage.Edge
	used          map[storage.EdgeID]struct{}
	// checks are the iterations' deferred WHERE checks.
	checks []quantifiedDeferredCheck
}

// quantifiedDeferredCheck is a deferred group WHERE (where) and the
// iteration's values of the group's variables.
type quantifiedDeferredCheck struct {
	where  string
	values pipelineRow
}

// pipelineApplyQuantifiedPathMatch runs a pattern part with quantified
// groups for each row; an OPTIONAL MATCH is handled by its caller.
func (e *StorageExecutor) pipelineApplyQuantifiedPathMatch(ctx context.Context, rows []pipelineRow, m *quantifiedPathMatch) ([]pipelineRow, error) {
	var out []pipelineRow
	for _, row := range rows {
		state := quantifiedPathState{row: row, used: map[storage.EdgeID]struct{}{}}
		err := e.matchQuantifiedPieces(ctx, m, 0, state, func(done quantifiedPathState) error {
			bound := done.row
			if m.pathVariable != "" {
				bound[m.pathVariable] = e.pathToMap(PathResult{Nodes: done.nodes, Relationships: done.relationships, Length: len(done.relationships)})
			}
			for name := range bound {
				if strings.HasPrefix(name, generatedVariablePrefix+"qpp") {
					delete(bound, name)
				}
			}
			for _, check := range done.checks {
				iteration := copyPipelineRow(bound)
				for name, value := range check.values {
					iteration[name] = value
				}
				if !e.evaluateWithWhereCondition(ctx, check.where, map[string]interface{}(iteration)) {
					return getExpressionFailure(ctx)
				}
			}
			if m.where != "" && !e.evaluateWithWhereCondition(ctx, m.where, map[string]interface{}(bound)) {
				return getExpressionFailure(ctx)
			}
			out = append(out, bound)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// matchQuantifiedPieces matches m.pieces[index:] from state and calls emit
// for each complete match.
func (e *StorageExecutor) matchQuantifiedPieces(ctx context.Context, m *quantifiedPathMatch, index int, state quantifiedPathState, emit func(quantifiedPathState) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if index == len(m.pieces) {
		return emit(state)
	}
	piece := m.pieces[index]
	if !piece.group {
		return e.matchQuantifiedChain(ctx, piece.chain, piece.first, "", state, func(next quantifiedPathState, _ pipelineRow) error {
			return e.matchQuantifiedPieces(ctx, m, index+1, next, emit)
		})
	}
	lists := make(map[string][]interface{}, len(piece.variables))
	return e.matchQuantifiedIterations(ctx, m, index, piece, 0, state, lists, emit)
}

// matchQuantifiedIterations matches the group m.pieces[index] after
// iterations repetitions, with each inner variable's values so far in lists.
func (e *StorageExecutor) matchQuantifiedIterations(ctx context.Context, m *quantifiedPathMatch, index int, piece quantifiedPathPiece, iterations int, state quantifiedPathState, lists map[string][]interface{}, emit func(quantifiedPathState) error) error {
	if iterations >= piece.min {
		done := state
		done.row = copyPipelineRow(state.row)
		for _, variable := range piece.variables {
			values := lists[variable]
			if values == nil {
				values = []interface{}{}
			}
			done.row[variable] = append([]interface{}(nil), values...)
		}
		if iterations == 0 && len(state.nodes) == 0 {
			// Zero iterations before any node: the group is one node, any.
			anyNode := generatedVariablePrefix + "qpp_node"
			if err := e.matchQuantifiedChain(ctx, "("+anyNode+")", anyNode, "", done, func(next quantifiedPathState, _ pipelineRow) error {
				delete(next.row, anyNode)
				return e.matchQuantifiedPieces(ctx, m, index+1, next, emit)
			}); err != nil {
				return err
			}
		} else if err := e.matchQuantifiedPieces(ctx, m, index+1, done, emit); err != nil {
			return err
		}
	}
	if piece.max >= 0 && iterations >= piece.max {
		return nil
	}
	iteration := state
	iteration.row = copyPipelineRow(state.row)
	for _, variable := range piece.variables {
		delete(iteration.row, variable)
	}
	return e.matchQuantifiedChain(ctx, piece.chain, piece.first, piece.where, iteration, func(next quantifiedPathState, matched pipelineRow) error {
		extended := make(map[string][]interface{}, len(lists))
		for _, variable := range piece.variables {
			extended[variable] = append(append([]interface{}(nil), lists[variable]...), matched[variable])
		}
		next.row = copyPipelineRow(state.row)
		if piece.deferred != "" {
			values := make(pipelineRow, len(piece.variables))
			for _, variable := range piece.variables {
				values[variable] = matched[variable]
			}
			next.checks = append(append([]quantifiedDeferredCheck(nil), state.checks...), quantifiedDeferredCheck{where: piece.deferred, values: values})
		}
		return e.matchQuantifiedIterations(ctx, m, index, piece, iterations+1, next, extended, emit)
	})
}

// matchQuantifiedChain matches chain (with its own WHERE, where) from
// state: its first node is the path's end node, when the path has one. For
// each match, then gets the extended state and the chain's bindings.
func (e *StorageExecutor) matchQuantifiedChain(ctx context.Context, chain, first, where string, state quantifiedPathState, then func(quantifiedPathState, pipelineRow) error) error {
	row := copyPipelineRow(state.row)
	if len(state.nodes) > 0 {
		end := state.nodes[len(state.nodes)-1]
		if bound, exists := row[first]; exists {
			if node, ok := bound.(*storage.Node); !ok || node == nil || node.ID != end.ID {
				return nil
			}
		}
		row[first] = end
	}
	pathVariable := generatedVariablePrefix + "qpp_chain"
	clause := "MATCH " + pathVariable + " = " + chain
	if where != "" {
		clause += " WHERE " + where
	}
	matched, err := e.pipelineMatchRows(ctx, row, clause)
	if err != nil {
		return err
	}
	repeatable := repeatableElements(ctx)
	for _, result := range matched {
		// The chain's path value carries its PathResult (pathToMap), whose
		// nodes and relationships are storage entities.
		value, _ := result[pathVariable].(map[string]interface{})
		nodes, relationships, _, _ := pathValueParts(value)
		next := quantifiedPathState{row: result, used: state.used, checks: state.checks}
		next.nodes = append([]*storage.Node(nil), state.nodes...)
		next.relationships = append([]*storage.Edge(nil), state.relationships...)
		for i, item := range nodes {
			if i > 0 || len(state.nodes) == 0 {
				next.nodes = append(next.nodes, item.(*storage.Node))
			}
		}
		reused := false
		if !repeatable {
			next.used = make(map[storage.EdgeID]struct{}, len(state.used)+len(relationships))
			for id := range state.used {
				next.used[id] = struct{}{}
			}
		}
		for _, item := range relationships {
			edge := item.(*storage.Edge)
			if !repeatable {
				if _, exists := next.used[edge.ID]; exists {
					reused = true
					break
				}
				next.used[edge.ID] = struct{}{}
			}
			next.relationships = append(next.relationships, edge)
		}
		if reused {
			continue
		}
		delete(next.row, pathVariable)
		if err := then(next, result); err != nil {
			return err
		}
	}
	return nil
}

// copyPipelineRow returns a shallow copy of row.
func copyPipelineRow(row pipelineRow) pipelineRow {
	copied := make(pipelineRow, len(row)+4)
	for name, value := range row {
		copied[name] = value
	}
	return copied
}

// quantifiedGroupSpan is one quantified group of a pattern,
// pattern[open:close+1]: its predicates (its own WHERE and each element's
// inline WHERE, [r WHERE …] / (n WHERE …), each as the [start, end) of its
// WHERE keyword through its body) and the node and relationship variables
// it binds, each to one value per iteration.
type quantifiedGroupSpan struct {
	open, close          int
	predicates           [][2]int
	nodes, relationships []string
}

// quantifiedGroups lists pattern's quantified groups in order, including
// those inside another parenthesis (shortestPath((a)((x)-[r]-(y))+(b)), the
// form SHORTEST k is read as).
func quantifiedGroups(pattern string) []quantifiedGroupSpan {
	var groups []quantifiedGroupSpan
	for index := 0; index < len(pattern); index++ {
		switch c := pattern[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(pattern, index, c) - 1
			continue
		case '[', '{':
			closeBy := map[byte]byte{'[': ']', '{': '}'}[c]
			if close := findMatchingDelimiter(pattern, index, rune(c), rune(closeBy)); close > index {
				index = close
			}
			continue
		case '(':
		default:
			continue
		}
		open, close := index, findMatchingParen(pattern, index)
		if close < 0 {
			return groups
		}
		if !parenthesisedPathAt(pattern, open, close) {
			continue
		}
		if _, quantified := relationshipQuantifierAt(pattern, skipASCIISpaces(pattern, close+1, len(pattern)), len(pattern)); !quantified {
			continue
		}
		group := quantifiedGroupSpan{open: open, close: close}
		elementsEnd := close
		if where := topLevelKeywordIndex(pattern[open+1:close], "WHERE"); where >= 0 {
			elementsEnd = open + 1 + where
			group.predicates = append(group.predicates, [2]int{elementsEnd, close})
		}
		group.predicates = append(group.predicates, elementPredicateSpans(pattern, open+1, elementsEnd)...)
		structure := []byte(pattern[open+1 : elementsEnd])
		for _, span := range group.predicates {
			for index := max(span[0], open+1); index < min(span[1], elementsEnd); index++ {
				structure[index-open-1] = ' '
			}
		}
		group.nodes = extractNodeVariables(string(structure))
		group.relationships = extractRelationshipVariables(string(structure))
		groups = append(groups, group)
		index = close
	}
	return groups
}

// elementPredicateSpans lists the inline WHERE of each node and relationship
// element in pattern[from:to] ((n WHERE …), [r WHERE …]) as the [start, end)
// of its WHERE keyword through its body.
func elementPredicateSpans(pattern string, from, to int) [][2]int {
	var spans [][2]int
	for index := from; index < to; index++ {
		switch c := pattern[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(pattern, index, c) - 1
		case '(', '[':
			closeBy := map[byte]byte{'(': ')', '[': ']'}[c]
			close := findMatchingDelimiter(pattern, index, rune(c), rune(closeBy))
			if close < 0 || close > to {
				return spans
			}
			if where := topLevelKeywordIndex(pattern[index+1:close], "WHERE"); where >= 0 {
				spans = append(spans, [2]int{index + 1 + where, close})
			}
			index = close
		}
	}
	return spans
}

// predicateTexts are the bodies of the group's predicates in pattern.
func (group quantifiedGroupSpan) predicateTexts(pattern string) []string {
	texts := make([]string, 0, len(group.predicates))
	for _, span := range group.predicates {
		if body := strings.TrimSpace(pattern[span[0]+len("WHERE") : span[1]]); body != "" {
			texts = append(texts, body)
		}
	}
	return texts
}

// maskQuantifiedGroupPredicates blanks each quantified group's predicates
// (its own WHERE and its elements' inline WHERE) so the pattern's
// structure is scanned without them: in
// ((a)-[r]-(b) WHERE size(kinds) = 0)+ neither (kinds) nor a $param map is
// a pattern element, and r there is one relationship, not the list the
// group binds outside it.
func maskQuantifiedGroupPredicates(pattern string) string {
	var masked []byte
	for _, group := range quantifiedGroups(pattern) {
		for _, span := range group.predicates {
			if masked == nil {
				masked = []byte(pattern)
			}
			for index := span[0]; index < span[1]; index++ {
				masked[index] = ' '
			}
		}
	}
	if masked == nil {
		return pattern
	}
	return string(masked)
}

// quantifiedGroupVariables returns the node and relationship variables
// inside pattern's quantified groups: each binds a list of its values.
func quantifiedGroupVariables(pattern string) (map[string]struct{}, map[string]struct{}) {
	var nodes, relationships map[string]struct{}
	for _, group := range quantifiedGroups(pattern) {
		if nodes == nil {
			nodes, relationships = map[string]struct{}{}, map[string]struct{}{}
		}
		for _, variable := range group.nodes {
			nodes[variable] = struct{}{}
		}
		for _, variable := range group.relationships {
			relationships[variable] = struct{}{}
		}
	}
	return nodes, relationships
}

// quantifiedGroupAt reports whether a quantifier follows the parenthesised
// path that closes at query[close].
func quantifiedGroupAt(query string, close, end int) bool {
	_, ok := relationshipQuantifierAt(query, skipASCIISpaces(query, close+1, end), end)
	return ok
}

// quantifiedGroup rewrites the quantified group query[open:close+1]: its
// elements' predicates (label expressions, element WHERE) join the group's
// own WHERE, which applies to each iteration, not the clause's.
func (r *labelExpressionRewriter) quantifiedGroup(open, close int) error {
	q := r.query
	patternEnd, whereBody := close, -1
	if where := topLevelKeywordIndex(q[open+1:close], "WHERE"); where >= 0 {
		patternEnd = open + 1 + where
		whereBody = skipASCIISpaces(q, patternEnd+len("WHERE"), close)
	}
	if err := quantifiedGroupError(q, open, close, patternEnd); err != nil {
		return err
	}
	var predicates []string
	if err := r.patternElements(open+1, patternEnd, labelPatternMatch, &predicates); err != nil {
		return err
	}
	if whereBody >= 0 {
		if err := r.expression(whereBody, close); err != nil {
			return err
		}
	}
	switch {
	case len(predicates) == 0:
	case whereBody >= 0:
		r.edit(whereBody, whereBody, strings.Join(predicates, " AND ")+" AND (")
		r.edit(trimRightIndex(q, whereBody, close), trimRightIndex(q, whereBody, close), ")")
	default:
		at := trimRightIndex(q, open+1, close)
		r.edit(at, at, " WHERE "+strings.Join(predicates, " AND "))
	}
	return nil
}

// quantifiedGroupQuantifierAt reports whether the + or * at s[at]
// quantifies a parenthesised path: it follows a ')' that closes one.
func quantifiedGroupQuantifierAt(s string, at int) bool {
	close := trimRightIndex(s, 0, at) - 1
	if close < 0 || s[close] != ')' {
		return false
	}
	depth := 0
	for open := close; open >= 0; open-- {
		switch s[open] {
		case ')':
			depth++
		case '(':
			if depth--; depth == 0 {
				return parenthesisedPathAt(s, open, close)
			}
		}
	}
	return false
}

// parenthesisedPathAt reports whether s[open:close+1] is a parenthesised
// path pattern: a node pattern, then a relationship arrow.
func parenthesisedPathAt(s string, open, close int) bool {
	inner := skipASCIISpaces(s, open+1, close)
	if inner >= close || s[inner] != '(' {
		return false
	}
	nodeEnd := findMatchingParen(s[:close], inner)
	if nodeEnd < 0 {
		return false
	}
	arrow := skipASCIISpaces(s, nodeEnd+1, close)
	return arrow+1 < close && (s[arrow] == '-' || s[arrow] == '<') && strings.IndexByte("-[>", s[arrow+1]) >= 0
}

// quantifiedGroupError is Neo4j's SyntaxError for the quantified group
// query[open:close+1] (its path ending at patternEnd), or nil: a group or
// quantifier inside it, a variable-length relationship in it, no
// relationship in it, or bounds that exclude every repetition count.
func quantifiedGroupError(q string, open, close, patternEnd int) error {
	inner := q[open+1 : patternEnd]
	if _, _, _, nested := nextQuantifiedGroup(inner, 0); nested || mayUseRelationshipQuantifier(inner) {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathNested())
	}
	if hasVariableLengthRelationship(q, open+1, patternEnd) {
		return labelExpressionSyntaxError(localization.CypherMatchingVariableLengthInQuantifiedPath())
	}
	if !patternHasRelationship(inner) && !strings.Contains(blankQuotedText(inner), "--") {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathNoRelationship())
	}
	quantifier, _ := relationshipQuantifierAt(q, skipASCIISpaces(q, close+1, len(q)), len(q))
	return quantifierBoundsError(quantifier)
}

// quantifierBoundsError is Neo4j's SyntaxError for a quantifier with an
// upper bound of 0 or below its lower bound, or nil.
func quantifierBoundsError(quantifier relationshipQuantifier) error {
	switch {
	case quantifier.max == 0:
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathZeroLimit())
	case quantifier.max > 0 && quantifier.min > quantifier.max:
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifierBoundsReversed())
	}
	return nil
}
