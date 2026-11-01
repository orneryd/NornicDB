// Author: Codex R17 implementation agent, 30 October 2026.
package cypher

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// pipelineClause carries an admitted grammar occurrence. The embedded value
// keeps copies independent, while preventing callers from constructing a
// clause with a kind/text struct literal. Physical operators keep their plans
// separately; a physical grouping or creation policy is not another clause.
type pipelineClause struct {
	pipelineClauseSource
	embeddingCandidate string
}

// pipelineClauseSource is constructed only here, from the retained parser
// owner's node. Source spelling, coordinates, kind and optionality belong to
// that occurrence and cannot be supplied independently by a caller.
type pipelineClauseSource struct {
	start, end  int
	kind        pipelineClauseKind
	syntax      cypherantlr.SyntaxNode
	programRoot cypherantlr.SyntaxNode // physical query body; nodes keep their canonical coordinate frame
	text        string
	optional    bool
}

func pipelineClauseFromSyntax(node cypherantlr.SyntaxNode) (pipelineClause, bool) {
	if !node.Valid() {
		return pipelineClause{}, false
	}
	kind, ok := querySyntaxClauseKind(node)
	if node.Rule() == cypherantlr.CypherParserRULE_useClause {
		kind, ok = pipelineClauseUse, true
	}
	start, end := node.Span()
	text := node.Text()
	_, optional := node.DirectToken(cypherantlr.CypherLexerOPTIONAL)
	if optional && (kind == pipelineClauseCall || kind == pipelineClauseCallSubquery) {
		if call, found := node.DirectToken(cypherantlr.CypherLexerCALL); found {
			callStart, _ := call.Span()
			text, start = text[callStart-start:], callStart
		}
	}
	if !ok {
		// EXISTS/COUNT's pattern form is an implicit MATCH in the grammar.
		// Retain the actual container and its pattern/WHERE window, rather
		// than manufacturing a MATCH statement to obtain that meaning.
		pattern, last, found := implicitMatchExtent(node)
		if !found {
			return pipelineClause{}, false
		}
		kind = pipelineClauseMatch
		start, _ = pattern.Span()
		_, end = last.Span()
		text = node.Source()[start:end]
	}
	return pipelineClause{pipelineClauseSource: pipelineClauseSource{
		start: start, end: end, kind: kind, syntax: node, text: text, optional: optional,
	}}, true
}

// implicitMatchExtent is the one reader of an EXISTS / COUNT pattern form's
// implicit MATCH: its pattern node, and the node that ends it (the container's
// WHERE when present, else the pattern). ok is false without a pattern.
// Author: Claude R17 implementation agent, 30 October 2026.
func implicitMatchExtent(container cypherantlr.SyntaxNode) (pattern, last cypherantlr.SyntaxNode, ok bool) {
	if container.Rule() == cypherantlr.CypherParserRULE_patternWhere || container.Rule() == cypherantlr.CypherParserRULE_pattern {
		return container, container, true
	}
	pattern = container.ChildRule(cypherantlr.CypherParserRULE_patternWhere)
	if !pattern.Valid() {
		pattern = container.ChildRule(cypherantlr.CypherParserRULE_pattern)
	}
	if !pattern.Valid() {
		return cypherantlr.SyntaxNode{}, cypherantlr.SyntaxNode{}, false
	}
	last = pattern
	if where := container.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
		last = where
	}
	return pattern, last, true
}

// requiredPipelineClause is for compiler paths that already established the
// clause's grammar role. A missing role is an owned invariant violation, not a
// reason to create a text-only clause or invoke another parser.
func requiredPipelineClause(node cypherantlr.SyntaxNode) pipelineClause {
	clause, ok := pipelineClauseFromSyntax(node)
	if !ok {
		panic("retained syntax has no pipeline clause role")
	}
	return clause
}

// pipelineCreateInput is a physical creation input, not another grammar clause.
// MERGE may materialize identity properties and choose a creation direction;
// its admitted MERGE occurrence remains the source of the operation.
type pipelineCreateInput struct {
	syntax              cypherantlr.SyntaxNode
	materializedPattern string
}

func createInputFromClause(clause pipelineClause) pipelineCreateInput {
	if clause.syntax.Rule() != cypherantlr.CypherParserRULE_createSt {
		panic("physical CREATE requires its admitted CREATE occurrence")
	}
	return pipelineCreateInput{syntax: clause.syntax}
}

func createInputsFromClauses(clauses []pipelineClause) []pipelineCreateInput {
	inputs := make([]pipelineCreateInput, len(clauses))
	for i, clause := range clauses {
		inputs[i] = createInputFromClause(clause)
	}
	return inputs
}

func createInputFromMerge(node cypherantlr.SyntaxNode, materializedPattern string) pipelineCreateInput {
	// Direction selection is a physical MERGE creation policy; do not build
	// a CREATE statement or give the MERGE node a different grammar kind.
	if node.Rule() != cypherantlr.CypherParserRULE_mergeSt {
		panic("physical MERGE creation requires its admitted MERGE occurrence")
	}
	return pipelineCreateInput{syntax: node, materializedPattern: materializedPattern}
}

func (input pipelineCreateInput) pattern() string {
	if input.syntax.Rule() == cypherantlr.CypherParserRULE_mergeSt {
		return directedMergePath(input.materializedPattern)
	}
	return input.syntax.ChildRule(cypherantlr.CypherParserRULE_pattern).Code()
}

type topLevelUnwindPlan struct {
	variable      string
	expression    string
	parameterName string
	items         []interface{}
	body          queryBody
}

// unwindPlanFromSyntax reads the leading UNWIND through the one iteration
// reader (parsePipelineIteration); it selects no UNWIND child itself.
func unwindPlanFromSyntax(body queryBody) topLevelUnwindPlan {
	expression, variable, _ := parsePipelineIteration(body.branches[0].clauses[0].pipelineClause)
	return topLevelUnwindPlan{
		variable:   variable,
		expression: expression,
		body:       body.clauseWindow(1, len(body.branches[0].clauses)),
	}
}
