package cypher

import (
	"context"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// pipelineClauseEffects is the chain planner's entire operator contract.
// Graph dependencies are collected by one grammar-tree walk, irrespective of
// whether an expression appears in a projection, predicate, pattern or tail.
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

// walkCypherSyntax always descends through every representation. The visitor
// changes semantic scope on entry and restores it on exit; it cannot prune a
// projection, function argument or subquery's expression children.
func walkCypherSyntax(node antlr4.Tree, enter func(antlr4.Tree) func()) {
	leave := enter(node)
	for _, child := range node.GetChildren() {
		walkCypherSyntax(child, enter)
	}
	if leave != nil {
		leave()
	}
}

func pipelineClauseEffectsFor(ctx context.Context, text string) (pipelineClauseEffects, error) {
	return pipelineSyntaxEffects(ctx, text, false)
}

func pipelineStatementEffectsFor(ctx context.Context, text string) (pipelineClauseEffects, error) {
	return pipelineSyntaxEffects(ctx, text, true)
}

// pipelineSyntaxEffects is the one semantic walk for both clause planning and
// statement admission. The reader choice changes only the grammar entry rule.
func pipelineSyntaxEffects(ctx context.Context, text string, statement bool) (pipelineClauseEffects, error) {
	cacheKey := text
	readSyntax := cypherantlr.ReadClauseSyntax
	if statement {
		cacheKey = "\x00" + text
		readSyntax = cypherantlr.ReadStatementSyntax
	}

	if effects, ok := pipelineClauseEffectCache.get(cacheKey); ok {
		return effects, ctx.Err()
	}
	var effects pipelineClauseEffects
	var semanticErr error
	err := readSyntax(ctx, text, func(tree antlr4.Tree, source []rune) {
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
				// Creating an entity cannot change an existing row entity's
				// property. Chain dependencies still retain all label/type writes.
				if creating {
					writes.keys, writes.anyKey = nil, false
				}
				effects.chainWrites.include(&writes)
			}
		}
		walkCypherSyntax(tree, func(node antlr4.Tree) func() {
			oldDepth, oldRead, oldWrite, oldCreate := depth, reading, writing, creating
			restore := func() { depth, reading, writing, creating = oldDepth, oldRead, oldWrite, oldCreate }
			textOf := func(n antlr4.ParserRuleContext) string { return cypherantlr.SyntaxText(n, source) }
			switch n := node.(type) {
			case *cypherantlr.WithStContext, *cypherantlr.LetStContext:
				// Row-preserving operators inherit all effects from their children.
			case *cypherantlr.ExpressionContext:
				reading, writing, creating = true, false, false
			case *cypherantlr.CallSubqueryContext:
				if depth == 0 {
					call, callErr := readCallClause(ctx, textOf(n))
					if callErr != nil {
						semanticErr = callErr
					}
					if callErr == nil {
						effects.batch = call.Batch
						effects.expands = call.Batch == nil || n.SubqueryBody().(*cypherantlr.SubqueryBodyContext).ReturnSt() != nil
					}
				}
				depth++
			case *cypherantlr.ExistsSubqueryContext, *cypherantlr.SubqueryExistContext,
				*cypherantlr.CountSubqueryContext, *cypherantlr.CollectSubqueryContext:
				depth++
				reading, writing, creating = true, false, false
			case *cypherantlr.CreateStContext:
				reading, writing, creating = false, true, true
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.MergeStContext:
				reading, writing, creating = true, true, false
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.MatchStContext:
				reading, writing, creating = true, false, false
				if depth == 0 {
					effects.expands = true
				}
			case *cypherantlr.NodePatternContext:
				recordPattern(textOf(n))
				// A bare standalone node is created, unlike a variable-only
				// endpoint of a relationship pattern that may already be bound.
				if creating && n.NodeLabels() == nil && n.Properties() == nil {
					if start, ok := n.GetParent().(*cypherantlr.PatternElemStartContext); ok {
						if pattern, ok := start.GetParent().(*cypherantlr.PatternElemContext); ok && len(pattern.AllPatternElemPart()) == 0 {
							effects.writes.anyNode = true
							effects.chainWrites.anyNode = true
						}
					}
				}
			case *cypherantlr.RelationshipPatternContext:
				recordPattern(textOf(n))
				if n.RelationDetail() == nil {
					if reading {
						effects.reads.anyRelationship = true
					}
					if writing {
						effects.writes.anyRelationship = true
						effects.chainWrites.anyRelationship = true
					}
				}
			case *cypherantlr.PropertyExpressionContext:
				if reading && n.Atom().Parameter() == nil && n.Atom().Literal() == nil {
					addStretchPropertyReads(textOf(n), &effects.reads)
				}
			case *cypherantlr.PropertyOrLabelExpressionContext:
				for _, label := range n.AllLabelExpression() {
					addStretchLabelChain(":"+textOf(label), &effects.reads)
				}
			case *cypherantlr.ListExpressionContext:
				if reading && n.LBRACK() != nil {
					// A postfix subscript reads an entity property unless its
					// receiver is an immutable parameter or literal value.
					immutable := false
					if parent, ok := n.GetParent().(*cypherantlr.AtomicExpressionContext); ok {
						atom := parent.PropertyOrLabelExpression().PropertyExpression().Atom()
						immutable = atom.Parameter() != nil || atom.Literal() != nil
					}
					if !immutable {
						effects.reads.anyKey = true
					}
				}
			case *cypherantlr.MapProjectionContext:
				if reading {
					effects.reads.anyKey = true
				}
			case *cypherantlr.LabelPredicateContext:
				if reading {
					addStretchLabelChain(":"+textOf(n.LabelExpression()), &effects.reads)
				}
			case *cypherantlr.SetStContext:
				reading, writing, creating = false, false, false
				var changed stretchTokens
				addStretchSetWrites(pipelineClauseBody(textOf(n), "SET"), &changed)
				effects.writes.include(&changed)
				effects.chainWrites.include(&changed)
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.RemoveStContext:
				reading, writing, creating = false, false, false
				var changed stretchTokens
				addStretchRemoveWrites(pipelineClauseBody(textOf(n), "REMOVE"), &changed)
				effects.writes.include(&changed)
				effects.chainWrites.include(&changed)
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.DeleteStContext:
				effects.writes.everything = true
				effects.chainWrites.everything = true
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.ForeachStContext:
				if depth == 0 {
					effects.barrier = true
				}
			case *cypherantlr.UnwindStContext, *cypherantlr.ForStContext, *cypherantlr.ReturnStContext:
				if depth == 0 {
					effects.barrier = true
					effects.expands = true
				}
			case *cypherantlr.QueryCallStContext:
				effects.opaqueReads = true
				if depth == 0 {
					effects.expands = true
				}
				if procedure, found := globalProcedureRegistry.Get(n.InvocationName().GetText()); !found || procedure.Spec.writes() {
					effects.writes.everything = true
					effects.chainWrites.everything = true
				}
			case *cypherantlr.OrderStContext:
				if depth == 0 {
					effects.materializes = true
					effects.barrier = true
				}
			case *cypherantlr.SkipStContext:
				effects.pagination = append(effects.pagination, paginationExpression{keyword: "SKIP", value: textOf(n.Expression())})
				if depth == 0 {
					effects.skip = textOf(n.Expression())
				}
			case *cypherantlr.LimitStContext:
				effects.pagination = append(effects.pagination, paginationExpression{keyword: "LIMIT", value: textOf(n.Expression())})
				if depth == 0 {
					effects.limit = textOf(n.Expression())
				}
			case *cypherantlr.WhereContext:
				if depth == 0 {
					effects.predicates = append(effects.predicates, textOf(n.Expression()))
				}
			case *cypherantlr.FilterStContext:
				if depth == 0 {
					effects.predicates = append(effects.predicates, textOf(n.Expression()))
				}
			case *cypherantlr.CountAllContext:
				if depth == 0 {
					effects.materializes = true
					effects.barrier = true
				}
			case *cypherantlr.FunctionInvocationContext:
				if invocation := n.InvocationName(); reading && invocation != nil {
					switch lowerASCII(invocation.GetText()) {
					case "labels":
						effects.reads.anyNode = true
					case "properties", "keys":
						effects.reads.anyKey = true
					}
				}
				if depth == 0 && pipelineExpressionContainsAggregate(textOf(n)) {
					effects.materializes = true
					effects.barrier = true
				}
			}
			return restore
		})
	})
	if err == nil {
		err = semanticErr
	}
	if err == nil {
		pipelineClauseEffectCache.put(cacheKey, effects)
	}
	return effects, err
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
