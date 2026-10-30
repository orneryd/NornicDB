// Author of retained effect-inspection consumption: Codex R17 implementation agent.
package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// pipelineClauseEffects is the chain planner's entire operator contract.
// Its observer consumes the canonical syntax product alongside scope binding.
type pipelineClauseEffects struct {
	reads, writes    stretchTokens
	chainWrites      stretchTokens
	batch            *transactionBatchSpec
	barrier, expands bool
	materializes     bool
	opaqueReads      bool
	predicates       []string
	skip, limit      string
	pagination       []paginationExpression
}

var pipelineClauseEffectCache = newBoundedCache[string, pipelineClauseEffects](4096, boundedCacheStringShardIndex)

// pipelineStatementEffectsFor reads a statement's effects through the query
// owner; the former clause-entry text path is gone (Claude R17 agent).
func pipelineStatementEffectsFor(ctx context.Context, text string) (pipelineClauseEffects, error) {
	return pipelineSyntaxEffects(ctx, text)
}

// pipelineSyntaxEffects is an effect-only compatibility consumer. The bound
// statement compiler composes its own binding observer with this same observer;
// an effect-cache hit can never stand in for scope completeness.
func pipelineSyntaxEffects(ctx context.Context, text string) (pipelineClauseEffects, error) {
	key := "\x00" + text
	if effects, found := pipelineClauseEffectCache.get(key); found {
		return effects, ctx.Err()
	}
	syntax, err := cypherantlr.QuerySyntax(ctx, text, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return pipelineClauseEffects{}, err
	}
	var effects pipelineClauseEffects
	syntax.Walk(pipelineEffectVisitor(&effects))
	if err := ctx.Err(); err != nil {
		return effects, err
	}
	pipelineClauseEffectCache.put(key, effects)
	return effects, nil
}

// pipelineEffectVisitor preserves graph/stream effects without owning lexical
// structure, variable identities, or a second traversal of query text.
func pipelineEffectVisitor(effects *pipelineClauseEffects) cypherantlr.SyntaxVisitor {
	depth := 0
	reading, writing, creating := true, false, false
	recordPattern := func(pattern string) {
		if reading {
			addStretchPattern(pattern, &effects.reads, false)
		}
		if writing {
			var writes stretchTokens
			addStretchPattern(pattern, &writes, true)
			effects.writes.include(&writes)
			if creating {
				writes.keys, writes.anyKey = nil, false
			}
			effects.chainWrites.include(&writes)
		}
	}
	return func(node cypherantlr.SyntaxNode) func() {
		oldDepth, oldRead, oldWrite, oldCreate := depth, reading, writing, creating
		restore := func() { depth, reading, writing, creating = oldDepth, oldRead, oldWrite, oldCreate }
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_useClause, cypherantlr.CypherParserRULE_withSt, cypherantlr.CypherParserRULE_letSt:
			// These operators inherit the effects of their children.
		case cypherantlr.CypherParserRULE_expression:
			reading, writing, creating = true, false, false
		case cypherantlr.CypherParserRULE_callSubquery:
			if depth == 0 {
				call := cypherantlr.DescribeCall(node)
				effects.batch = call.Batch
				body := node.ChildRule(cypherantlr.CypherParserRULE_subqueryBody)
				effects.expands = call.Batch == nil || body.ChildRule(cypherantlr.CypherParserRULE_returnSt).Valid()
			}
			depth++
		case cypherantlr.CypherParserRULE_existsSubquery, cypherantlr.CypherParserRULE_subqueryExist,
			cypherantlr.CypherParserRULE_countSubquery, cypherantlr.CypherParserRULE_collectSubquery:
			depth++
			reading, writing, creating = true, false, false
		case cypherantlr.CypherParserRULE_createSt:
			reading, writing, creating = false, true, true
			if depth == 0 {
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_mergeSt:
			reading, writing, creating = true, true, false
			if depth == 0 {
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_matchSt:
			reading, writing, creating = true, false, false
			if depth == 0 {
				effects.expands = true
			}
		case cypherantlr.CypherParserRULE_nodePattern:
			recordPattern(node.Code())
			if creating && !node.ChildRule(cypherantlr.CypherParserRULE_nodeLabels).Valid() && !node.ChildRule(cypherantlr.CypherParserRULE_properties).Valid() {
				if start, ok := node.Parent(); ok && start.Rule() == cypherantlr.CypherParserRULE_patternElemStart {
					if pattern, ok := start.Parent(); ok && pattern.Rule() == cypherantlr.CypherParserRULE_patternElem && !pattern.ChildRule(cypherantlr.CypherParserRULE_patternElemPart).Valid() {
						effects.writes.anyNode = true
						effects.chainWrites.anyNode = true
					}
				}
			}
		case cypherantlr.CypherParserRULE_relationshipPattern:
			recordPattern(node.Code())
			if !node.ChildRule(cypherantlr.CypherParserRULE_relationDetail).Valid() {
				if reading {
					effects.reads.anyRelationship = true
				}
				if writing {
					effects.writes.anyRelationship = true
					effects.chainWrites.anyRelationship = true
				}
			}
		case cypherantlr.CypherParserRULE_propertyExpression:
			atom := node.ChildRule(cypherantlr.CypherParserRULE_atom)
			if reading && !atom.ChildRule(cypherantlr.CypherParserRULE_parameter).Valid() && !atom.ChildRule(cypherantlr.CypherParserRULE_literal).Valid() {
				// Property names belong to this grammar node. Source adjacency
				// preserves the existing conservative treatment of spaced or
				// quoted dereferences without scanning expression text again.
				_, previousEnd := atom.Span()
				gapBeforeDot := false
				for index := 0; index < node.ChildCount(); index++ {
					child := node.Child(index)
					start, end := child.Span()
					if child.IsToken(cypherantlr.CypherLexerDOT) {
						gapBeforeDot = start != previousEnd
					} else if child.Rule() == cypherantlr.CypherParserRULE_name {
						name := child.Code()
						if gapBeforeDot || start != previousEnd || strings.HasPrefix(name, "`") {
							effects.reads.anyKey = true
						} else {
							effects.reads.add(&effects.reads.keys, symbolicNameValue(name))
						}
					}
					previousEnd = end
				}
			}
		case cypherantlr.CypherParserRULE_propertyOrLabelExpression:
			for index := 0; index < node.ChildCount(); index++ {
				label := node.Child(index)
				if label.Rule() == cypherantlr.CypherParserRULE_labelExpression {
					addStretchLabelChain(":"+label.Code(), &effects.reads)
				}
			}
		case cypherantlr.CypherParserRULE_listExpression:
			if _, subscript := node.DirectToken(cypherantlr.CypherLexerLBRACK); reading && subscript {
				immutable := false
				if parent, exists := node.Parent(); exists && parent.Rule() == cypherantlr.CypherParserRULE_atomicExpression {
					atom := parent.ChildRule(cypherantlr.CypherParserRULE_propertyOrLabelExpression).ChildRule(cypherantlr.CypherParserRULE_propertyExpression).ChildRule(cypherantlr.CypherParserRULE_atom)
					immutable = atom.ChildRule(cypherantlr.CypherParserRULE_parameter).Valid() || atom.ChildRule(cypherantlr.CypherParserRULE_literal).Valid()
				}
				if !immutable {
					effects.reads.anyKey = true
				}
			}
		case cypherantlr.CypherParserRULE_mapProjection:
			if reading {
				effects.reads.anyKey = true
			}
		case cypherantlr.CypherParserRULE_labelPredicate:
			if reading {
				addStretchLabelChain(":"+node.ChildRule(cypherantlr.CypherParserRULE_labelExpression).Code(), &effects.reads)
			}
		case cypherantlr.CypherParserRULE_setSt, cypherantlr.CypherParserRULE_removeSt:
			reading, writing, creating = false, false, false
			var changed stretchTokens
			if node.Rule() == cypherantlr.CypherParserRULE_setSt {
				addStretchSetWrites(pipelineClauseBody(node.Code(), "SET"), &changed)
			} else {
				addStretchRemoveWrites(pipelineClauseBody(node.Code(), "REMOVE"), &changed)
			}
			effects.writes.include(&changed)
			effects.chainWrites.include(&changed)
			if depth == 0 {
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_deleteSt:
			effects.writes.everything = true
			effects.chainWrites.everything = true
			if depth == 0 {
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_foreachSt:
			if depth == 0 {
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt, cypherantlr.CypherParserRULE_returnSt:
			if depth == 0 {
				effects.barrier, effects.expands = true, true
			}
		case cypherantlr.CypherParserRULE_queryCallSt, cypherantlr.CypherParserRULE_standaloneCall:
			effects.opaqueReads = true
			if depth == 0 {
				effects.expands = true
			}
			name := node.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
			if procedure, found := globalProcedureRegistry.Get(name); !found || procedure.Spec.writes() {
				effects.writes.everything = true
				effects.chainWrites.everything = true
			}
		case cypherantlr.CypherParserRULE_orderSt:
			if depth == 0 {
				effects.materializes = true
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
			value := node.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
			keyword := "SKIP"
			if node.Rule() == cypherantlr.CypherParserRULE_limitSt {
				keyword = "LIMIT"
			}
			effects.pagination = append(effects.pagination, paginationExpression{keyword: keyword, value: value})
			if depth == 0 {
				if keyword == "SKIP" {
					effects.skip = value
				} else {
					effects.limit = value
				}
			}
		case cypherantlr.CypherParserRULE_where, cypherantlr.CypherParserRULE_filterSt:
			if depth == 0 {
				effects.predicates = append(effects.predicates, node.ChildRule(cypherantlr.CypherParserRULE_expression).Code())
			}
		case cypherantlr.CypherParserRULE_countAll:
			if depth == 0 {
				effects.materializes = true
				effects.barrier = true
			}
		case cypherantlr.CypherParserRULE_functionInvocation:
			if invocation := node.ChildRule(cypherantlr.CypherParserRULE_invocationName); reading && invocation.Valid() {
				switch lowerASCII(invocation.QualifiedName()) {
				case "labels":
					effects.reads.anyNode = true
				case "properties", "keys":
					effects.reads.anyKey = true
				}
			}
			if depth == 0 && pipelineExpressionContainsAggregate(node.Code()) {
				effects.materializes = true
				effects.barrier = true
			}
		}
		return restore
	}
}

// simplifiedPipelinePredicate delegates boolean identities to the existing
// logical owner. Nonlogical leaves remain unknown: a runtime comparison such
// as 1=0 is not a planning-time empty result on Neo4j.
func simplifiedPipelinePredicate(expression string) (interface{}, bool, error) {
	expression = logicalOperandKey(expression)
	if strings.EqualFold(expression, "true") {
		return true, true, nil
	}
	if strings.EqualFold(expression, "false") {
		return false, true, nil
	}
	value, logical, resolved, err := evaluateLogicalExpression(expression, simplifiedPipelinePredicate)
	return value, logical && resolved, err
}

func (e *StorageExecutor) pipelineClauseBoundary(ctx context.Context, effects pipelineClauseEffects, writes *stretchTokens, maximumRows int) (bool, int) {
	boundary := effects.barrier || effects.reads.observes(writes)
	for _, predicate := range effects.predicates {
		if value, resolved, err := simplifiedPipelinePredicate(predicate); err == nil && resolved && value == false {
			boundary = true
		}
	}
	if effects.expands {
		maximumRows = -1
	}
	if effects.skip != "" {
		if skip, ok := e.evaluatePipelinePagination(ctx, effects.skip, nil); ok && maximumRows >= 0 {
			boundary = boundary || (skip > 0 && skip >= maximumRows)
			maximumRows = max(0, maximumRows-skip)
		}
	}
	if effects.limit != "" {
		if limit, ok := e.evaluatePipelinePagination(ctx, effects.limit, nil); ok {
			boundary = boundary || limit == 0
			if maximumRows < 0 || limit < maximumRows {
				maximumRows = limit
			}
		}
	}
	return boundary, maximumRows
}
