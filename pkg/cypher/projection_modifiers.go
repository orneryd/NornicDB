// Author: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// projectionModifiers is the one reading of a projection's ORDER BY, SKIP,
// LIMIT and WHERE, for WITH (its projectionBody and withSt's WHERE) and for
// YIELD (the procedure call or SHOW tail node). Each is the retained
// occurrence's code, "" when absent. whereAfterWindow is WITH's grammar
// position: WITH … SKIP / LIMIT n WHERE p keeps n rows, then filters, as Neo4j
// does; YIELD's WHERE filters before ordering and paging.
type projectionModifiers struct {
	where                   string // WHERE predicate, "" when absent
	orderBy                 string // ORDER BY items, "" when absent
	skip                    string // SKIP expression, "" when absent
	limit                   string
	skipScalar, limitScalar *scalarExpressionSyntax // LIMIT expression, "" when absent
	whereAfterWindow        bool
}

// projectionModifiersFromSyntax reads the orderSt, skipSt and limitSt children
// of container and the predicate of where.
func projectionModifiersFromSyntax(container, where cypherantlr.SyntaxNode, whereAfterWindow bool) projectionModifiers {
	modifiers := projectionModifiers{whereAfterWindow: whereAfterWindow}
	if where.Valid() {
		modifiers.where = where.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
	}
	if !container.Valid() {
		return modifiers
	}
	for i := 0; i < container.ChildCount(); i++ {
		part := container.Child(i)
		switch part.Rule() {
		case cypherantlr.CypherParserRULE_orderSt:
			var terms []string
			for j := 0; j < part.ChildCount(); j++ {
				if item := part.Child(j); item.Rule() == cypherantlr.CypherParserRULE_orderItem {
					terms = append(terms, item.Code())
				}
			}
			modifiers.orderBy = strings.Join(terms, ", ")
		case cypherantlr.CypherParserRULE_skipSt:
			modifiers.skip = part.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
			modifiers.skipScalar = observeScalarSyntax(part.ChildRule(cypherantlr.CypherParserRULE_expression))
		case cypherantlr.CypherParserRULE_limitSt:
			modifiers.limit = part.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
			modifiers.limitScalar = observeScalarSyntax(part.ChildRule(cypherantlr.CypherParserRULE_expression))
		}
	}
	return modifiers
}

// withModifiers is a retained WITH clause's modifiers.
func withModifiers(clause pipelineClause) projectionModifiers {
	return projectionModifiersFromSyntax(clause.syntax.ChildRule(cypherantlr.CypherParserRULE_projectionBody), clause.syntax.ChildRule(cypherantlr.CypherParserRULE_where), true)
}

// windowed reports SKIP or LIMIT.
func (modifiers projectionModifiers) windowed() bool {
	return modifiers.skip != "" || modifiers.limit != ""
}

// orderTerms are the ORDER BY items' sort terms.
func (modifiers projectionModifiers) orderTerms() []orderByTerm {
	if modifiers.orderBy == "" {
		return nil
	}
	return parseOrderByClause(modifiers.orderBy)
}

// whereSplit places WHERE before ordering, or after the window when WITH
// pages its rows. Without a window both places give the same rows.
func (modifiers projectionModifiers) whereSplit() (before, after string) {
	if modifiers.whereAfterWindow && modifiers.windowed() {
		return "", modifiers.where
	}
	return modifiers.where, ""
}

// evaluateProjectionWindow evaluates SKIP (0 when absent) and LIMIT (-1 when
// absent); ok is false when either is not a non-negative integer.
func (e *StorageExecutor) evaluateProjectionWindow(ctx context.Context, modifiers projectionModifiers, rows []pipelineRow) (skip, limit int, ok bool) {
	skip, limit = 0, -1
	if modifiers.skip != "" {
		if skip, ok = e.evaluatePipelinePagination(ctx, modifiers.skip, rows); !ok {
			return 0, 0, false
		}
	}
	if modifiers.limit != "" {
		if limit, ok = e.evaluatePipelinePagination(ctx, modifiers.limit, rows); !ok {
			return 0, 0, false
		}
	}
	return skip, limit, true
}

// applyProjectionModifiers filters, orders and pages rows whose scope is the
// row itself: WHERE in its place (whereSplit), ORDER BY, SKIP / LIMIT.
func (e *StorageExecutor) applyProjectionModifiers(ctx context.Context, rows []pipelineRow, modifiers projectionModifiers) ([]pipelineRow, bool) {
	skip, limit, ok := e.evaluateProjectionWindow(ctx, modifiers, rows)
	if !ok {
		return nil, false
	}
	before, after := modifiers.whereSplit()
	rows = e.filterPipelineRows(ctx, rows, before)
	if !e.orderPipelineRows(ctx, rows, modifiers.orderTerms()) {
		return nil, false
	}
	return e.filterPipelineRows(ctx, applyPipelineWindow(rows, skip, limit), after), true
}
