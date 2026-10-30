package cypher

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	chain braceView
	// first and last are the variables of chain's first and last node
	// patterns (generated for an anonymous one).
	first, last string
	group       bool
	// where is a group's own WHERE; min and max its quantifier's bounds
	// (max < 0: none).
	where    braceView
	min, max int
	// variables are the variables inside a group, which bind lists.
	variables []string
	// deferred is the part of a group's WHERE that reads variables the
	// pattern binds after the group: each iteration is checked against it
	// once the pattern is matched.
	deferred braceView
}

// quantifiedPathMatch is a MATCH pattern part with quantified groups.
type quantifiedPathMatch struct {
	pathVariable string
	pieces       []quantifiedPathPiece
	where        braceView
}

// hasQuantifiedGroup reports whether pattern has a quantified group: a
// parenthesised path followed by a quantifier.
func hasQuantifiedGroup(pattern braceView) bool {
	if !mayUseQuantifiedGroup(pattern.text()) {
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
		if next := skipASCIISpaces(text, i+1, len(text)); next < len(text) && syntaxByteInSet("+*{", text[next]) {
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
// pattern[from] at its own level: its parentheses (open, close) and
// quantifier (quantifiedGroupFrom).
func nextQuantifiedGroup(pattern braceView, from int) (int, int, relationshipQuantifier, bool) {
	return quantifiedGroupFrom(pattern, from, false)
}

// quantifiedGroupFrom is the one group finder: the first quantified group at
// or after pattern[from], skipping quoted text, lists and maps. A parenthesis
// that isn't a group is skipped whole, unless nested is set: then the groups
// inside it count too (shortestPath((a)((x)-[r]-(y))+(b)), the form SHORTEST
// k is read as).
func quantifiedGroupFrom(view braceView, from int, nested bool) (int, int, relationshipQuantifier, bool) {
	pattern := view.text()
	for i := from; i < len(pattern); i++ {
		switch c := pattern[i]; {
		case (cypherLexicalByte(c)) == cypherQuoteClass:
			i = skipCypherQuotedText(pattern, i, c) - 1
		case cyphertext.IsOpenBrace(cypherLexicalByte(c)):
			if close, found := view.close(i); found {
				i = close
			}
		case (cypherLexicalByte(c)) == '[':
			closeBy := cyphertext.DelimiterCloser(c)
			if close := cyphertext.FindMatchingDelimiter(pattern, i, rune(c), rune(closeBy), cyphertext.OpeningDelimiter); close > i {
				i = close
			}
		case (cypherLexicalByte(c)) == '(':
			close := findMatchingParen(pattern, i)
			if close < 0 {
				return 0, 0, relationshipQuantifier{}, false
			}
			if parenthesisedPathAt(view, i, close) {
				if quantifier, ok := relationshipQuantifierAt(view, skipASCIISpaces(pattern, close+1, len(pattern)), len(pattern)); ok {
					return i, close, quantifier, true
				}
			}
			if !nested {
				i = close
			}
		}
	}
	return 0, 0, relationshipQuantifier{}, false
}

// parseQuantifiedPathMatch reads a MATCH body (pattern [WHERE …]) of one
// pattern part with quantified groups. ok is false for any other body.
func parseQuantifiedPathMatch(body braceView) (*quantifiedPathMatch, bool, error) {
	if !hasQuantifiedGroup(body) {
		return nil, false, nil
	}
	pattern, where := body, body.window(0, 0)
	if index := topLevelKeywordIndex(body, "WHERE"); index >= 0 {
		pattern, where = body.window(0, index).trim(), body.window(index+len("WHERE"), len(body.text())).trim()
	}
	if len(pattern.commaViews()) != 1 || !hasQuantifiedGroup(pattern) {
		return nil, false, nil
	}
	m := &quantifiedPathMatch{where: where}
	if variable := extractPathAssignmentVariable(pattern.text()); variable != "" {
		m.pathVariable = variable
		pattern = pattern.window(strings.IndexByte(pattern.text(), '=')+1, len(pattern.text())).trim()
	}
	generated := 0
	name := func() string {
		generated++
		return generatedVariablePrefix + "qpp" + strconv.Itoa(generated)
	}
	outside := map[string]bool{}
	for at := 0; at < len(pattern.text()); at = skipASCIISpaces(pattern.text(), at, len(pattern.text())) {
		open, close, quantifier, grouped := nextQuantifiedGroup(pattern, at)
		if grouped && open == at {
			inner := pattern.window(open+1, close).trim()
			piece := quantifiedPathPiece{group: true, min: quantifier.min, max: quantifier.max}
			if index := topLevelKeywordIndex(inner, "WHERE"); index >= 0 {
				inner, piece.where = inner.window(0, index).trim(), inner.window(index+len("WHERE"), len(inner.text())).trim()
			}
			piece.chain, piece.first, piece.last = nameChainEnds(inner, name)
			structure := matchPatternStructure(piece.chain)
			piece.variables = append(extractNodeVariables(structure), extractRelationshipVariables(structure)...)
			m.pieces = append(m.pieces, piece)
			at = quantifier.end
			continue
		}
		end := len(pattern.text())
		if grouped {
			end = open
		}
		chain := pattern.window(at, end).trim()
		if !strings.HasPrefix(chain.text(), "(") || !strings.HasSuffix(chain.text(), ")") {
			return nil, true, localizedError(localization.CypherMatchingPathPatternInvalid(pattern.text()), nil)
		}
		piece := quantifiedPathPiece{}
		piece.chain, piece.first, piece.last = nameChainEnds(chain, name)
		structure := matchPatternStructure(chain)
		for _, variable := range append(extractNodeVariables(structure), extractRelationshipVariables(structure)...) {
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
	if !piece.group || piece.where.empty() {
		return
	}
	var later []string
	for _, next := range pieces[index+1:] {
		structure := matchPatternStructure(next.chain)
		later = append(later, extractNodeVariables(structure)...)
		later = append(later, extractRelationshipVariables(structure)...)
	}
	var now, deferred []braceView
	for _, term := range splitTopLevelAndConjuncts(piece.where) {
		readsLater := false
		for _, variable := range later {
			readsLater = readsLater || referencesVariable(term.text(), variable)
		}
		if readsLater {
			deferred = append(deferred, term)
		} else {
			now = append(now, term)
		}
	}
	piece.where, piece.deferred = piece.where.assembleCode(now, " AND "), piece.where.assembleCode(deferred, " AND ")
}

// nameChainEnds returns chain with a variable in its first and last node
// patterns (a generated one where it has none) and those variables.
func nameChainEnds(chain braceView, name func() string) (braceView, string, string) {
	lastOpen := lastNodePatternStart(chain)
	named := func(open int) (braceView, string) {
		variable, _, ok := scanSymbolicName(chain.text(), skipASCIISpaces(chain.text(), open+1, len(chain.text())))
		if ok {
			return chain, variable
		}
		variable = name()
		return chain.replace(open+1, open+1, variable), variable
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
func lastNodePatternStart(view braceView) int {
	chain := view.text()
	last := -1
	// A chain's top level holds patterns and arrows; quoted text is inside
	// brackets.
	for i := 0; i < len(chain); i++ {
		switch c := chain[i]; {
		case cyphertext.IsOpenBrace(c):
			if close, found := view.close(i); found {
				i = close
			}
		case c == '[':
			closeBy := cyphertext.DelimiterCloser(c)
			if close := cyphertext.FindMatchingDelimiter(chain, i, rune(c), rune(closeBy), cyphertext.OpeningDelimiter); close > i {
				i = close
			}
		case c == '(':
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
	where  braceView
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
			if !m.where.empty() && !e.evaluateWithWhereCondition(ctx, m.where, map[string]interface{}(bound)) {
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
		return e.matchQuantifiedChain(ctx, piece.chain, piece.first, piece.chain.window(0, 0), state, func(next quantifiedPathState, _ pipelineRow) error {
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
			if err := e.matchQuantifiedChain(ctx, piece.chain.generated("("+anyNode+")"), anyNode, piece.chain.window(0, 0), done, func(next quantifiedPathState, _ pipelineRow) error {
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
		if !piece.deferred.empty() {
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
func (e *StorageExecutor) matchQuantifiedChain(ctx context.Context, chain braceView, first string, where braceView, state quantifiedPathState, then func(quantifiedPathState, pipelineRow) error) error {
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
	base := braceView{syntax: chain.syntax, source: chain.source.OriginalView()}
	clause := chain.wrap("MATCH "+pathVariable+" = ", "")
	if !where.empty() {
		clause = base.assembleCode([]braceView{clause, where.wrap(" WHERE ", "")}, "")
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
func quantifiedGroups(view braceView) []quantifiedGroupSpan {
	pattern := view.text()
	var groups []quantifiedGroupSpan
	for index := 0; index < len(pattern); index++ {
		open, close, _, found := quantifiedGroupFrom(view, index, true)
		if !found {
			return groups
		}
		group := quantifiedGroupSpan{open: open, close: close}
		elementsEnd := close
		if where := topLevelKeywordIndex(view.window(open+1, close), "WHERE"); where >= 0 {
			elementsEnd = open + 1 + where
			group.predicates = append(group.predicates, [2]int{elementsEnd, close})
		}
		group.predicates = append(group.predicates, elementPredicateSpans(view, open+1, elementsEnd)...)
		structure := []byte(pattern[open+1 : elementsEnd])
		for _, span := range group.predicates {
			for index := max(span[0], open+1); index < min(span[1], elementsEnd); index++ {
				structure[index-open-1] = ' '
			}
		}
		// Bound names, as the scope stores them: `x x` is x x.
		group.nodes = normalizedBindingNames(extractNodeVariables(view.window(open+1, elementsEnd).mask(string(structure))))
		group.relationships = normalizedBindingNames(extractRelationshipVariables(view.window(open+1, elementsEnd).mask(string(structure))))
		groups = append(groups, group)
		index = close
	}
	return groups
}

// elementPredicateSpans lists the inline WHERE of each node and relationship
// element in pattern[from:to] ((n WHERE …), [r WHERE …]) as the [start, end)
// of its WHERE keyword through its body.
func elementPredicateSpans(view braceView, from, to int) [][2]int {
	pattern := view.text()
	var spans [][2]int
	for index := from; index < to; index++ {
		switch c := pattern[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(pattern, index, c) - 1
		case '(', '[':
			closeBy := map[byte]byte{'(': ')', '[': ']'}[c]
			close := cyphertext.FindMatchingDelimiter(pattern, index, rune(c), rune(closeBy), cyphertext.OpeningDelimiter)
			if close < 0 || close > to {
				return spans
			}
			if where := elementWhereIndex(view, index, close); where >= 0 {
				spans = append(spans, [2]int{where, close})
			}
			index = close
		}
	}
	return spans
}

// predicateTexts are the bodies of the group's predicates in pattern.
func (group quantifiedGroupSpan) predicateTexts(pattern braceView) []braceView {
	texts := make([]braceView, 0, len(group.predicates))
	for _, span := range group.predicates {
		if body := pattern.window(span[0]+len("WHERE"), span[1]).trim(); !body.empty() {
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
func maskQuantifiedGroupPredicates(view braceView) braceView {
	pattern := view.text()
	var masked []byte
	for _, group := range quantifiedGroups(view) {
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
		return view
	}
	return view.mask(string(masked))
}

// matchPatternStructure is the pattern of a MATCH body, clause or pattern
// part (the text before its WHERE) with every predicate in it blanked: each
// quantified group's (maskQuantifiedGroupPredicates) and each element's
// inline WHERE. It is the one text the variables a pattern binds are read
// from: in ((x WHERE x.k IN (l))-->(y))+ or the (x WHERE
// __nornic_haslabels(x, (l), false)) a dynamic label $(l) becomes, (l) is
// not a node.
func matchPatternStructure(view braceView) braceView {
	if where := topLevelKeywordIndex(view, "WHERE"); where >= 0 {
		view = view.window(0, where)
	}
	view = maskQuantifiedGroupPredicates(view)
	body := view.text()
	// An element's inline WHERE outside a group ((n WHERE n.k IN (l))) is
	// an expression too.
	spans := elementPredicateSpans(view, 0, len(body))
	if len(spans) == 0 {
		return view
	}
	masked := []byte(body)
	for _, span := range spans {
		for index := span[0]; index < span[1]; index++ {
			masked[index] = ' '
		}
	}
	return view.mask(string(masked))
}

// quantifiedGroupVariables returns the node and relationship variables
// inside pattern's quantified groups: each binds a list of its values.
func quantifiedGroupVariables(pattern braceView) (map[string]struct{}, map[string]struct{}) {
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
func quantifiedGroupAt(query braceView, close, end int) bool {
	_, ok := relationshipQuantifierAt(query, skipASCIISpaces(query.text(), close+1, end), end)
	return ok
}

// quantifiedGroup rewrites the quantified group query[open:close+1]: its
// elements' predicates (label expressions, element WHERE) join the group's
// own WHERE, which applies to each iteration, not the clause's.
func (r *labelExpressionRewriter) quantifiedGroup(open, close int) error {
	q := r.query
	patternEnd, whereBody := close, -1
	if where := topLevelKeywordIndex(r.braceView.window(open+1, close), "WHERE"); where >= 0 {
		patternEnd = open + 1 + where
		whereBody = skipASCIISpaces(q, patternEnd+len("WHERE"), close)
	}
	if err := quantifiedGroupError(r.braceView, open, close, patternEnd); err != nil {
		return err
	}
	var predicates []rewritePredicate
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
		r.editPredicate(whereBody, whereBody, wrapRewritePredicate("", joinRewritePredicates(predicates, " AND "), " AND ("))
		r.edit(trimRightIndex(q, whereBody, close), trimRightIndex(q, whereBody, close), ")")
	default:
		at := trimRightIndex(q, open+1, close)
		r.editPredicate(at, at, wrapRewritePredicate(" WHERE ", joinRewritePredicates(predicates, " AND "), ""))
	}
	return nil
}

// quantifiedGroupQuantifierAt reports whether the + or * at s[at]
// quantifies a parenthesised path: it follows a ')' that closes one.
func quantifiedGroupQuantifierAt(view braceView, at int) bool {
	s := view.text()
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
				return parenthesisedPathAt(view, open, close)
			}
		}
	}
	return false
}

// parenthesisedPathAt reports whether s[open:close+1] is a parenthesised
// path pattern: a node pattern, then a relationship arrow.
func parenthesisedPathAt(view braceView, open, close int) bool {
	return braceGroupedPath(view, open, close)
}

// quantifiedGroupError is Neo4j's SyntaxError for the quantified group
// query[open:close+1] (its path ending at patternEnd), or nil: a group or
// quantifier inside it, a variable-length relationship in it, no
// relationship in it, or bounds that exclude every repetition count.
func quantifiedGroupError(view braceView, open, close, patternEnd int) error {
	q := view.text()
	inner := view.window(open+1, patternEnd)
	if _, _, _, nested := nextQuantifiedGroup(inner, 0); nested || mayUseRelationshipQuantifier(inner.text()) {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathNested())
	}
	if hasVariableLengthRelationship(view, open+1, patternEnd) {
		return labelExpressionSyntaxError(localization.CypherMatchingVariableLengthInQuantifiedPath())
	}
	if !patternHasRelationship(inner) && !strings.Contains(blankQuotedText(inner.text()), "--") {
		return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathNoRelationship())
	}
	quantifier, _ := relationshipQuantifierAt(view, skipASCIISpaces(q, close+1, len(q)), len(q))
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

// normalizedBindingNames rewrites each name as written (`x x`) to the name
// it binds (x x, normalizeProjectionColumnName), in place.
func normalizedBindingNames(names []string) []string {
	for index, name := range names {
		names[index] = normalizeProjectionColumnName(name)
	}
	return names
}
