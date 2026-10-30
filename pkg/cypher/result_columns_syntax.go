package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// canonicalQueryClause adapts standalone execution helpers to the same grammar
// product. The bound compiler supplies the retained clause node directly.
func canonicalQueryClause(ctx context.Context, source braceView) (cypherantlr.SyntaxNode, bool) {
	syntax, err := cypherantlr.ParseSyntax(withBraceView(ctx, source), source.text(), cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return cypherantlr.SyntaxNode{}, false
	}
	root := statementSyntaxQuery(syntax)
	if !root.Valid() {
		root, _ = syntax.Window(0, len(source.text()), cypherantlr.CypherParserRULE_subqueryBody)
	}
	body := queryBodyFromSyntax(source, root)
	if len(body.branches) == 0 || len(body.branches[0].clauses) == 0 {
		return cypherantlr.SyntaxNode{}, false
	}
	return body.branches[0].clauses[0].syntax, true
}

func syntaxSymbolName(node cypherantlr.SyntaxNode) string {
	if !node.Valid() {
		return ""
	}
	name, _ := node.Name()
	return name
}

func projectionPlanFromSyntax(view braceView, clause cypherantlr.SyntaxNode, expressions ...func(cypherantlr.SyntaxNode)) *returnProjectionPlan {
	plan := &returnProjectionPlan{view: view}
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	if !body.Valid() {
		return plan
	}
	_, plan.distinct = body.DirectToken(cypherantlr.CypherLexerDISTINCT)
	var modifiers []braceView
	for i := 0; i < body.ChildCount(); i++ {
		node := body.Child(i)
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
			modifiers = append(modifiers, view.code(node))
		}
	}
	plan.modifiersView = view.assembleCode(modifiers, " ")
	plan.modifiers = plan.modifiersView.text()
	items := body.ChildRule(cypherantlr.CypherParserRULE_projectionItems)
	if !items.Valid() {
		return plan
	}
	_, plan.star = items.DirectToken(cypherantlr.CypherLexerMULT)
	for i := 0; i < items.ChildCount(); i++ {
		item := items.Child(i)
		if item.Rule() != cypherantlr.CypherParserRULE_projectionItem {
			continue
		}
		expression := item.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !expression.Valid() {
			return &returnProjectionPlan{}
		}
		definition := item.ChildRule(cypherantlr.CypherParserRULE_symbol)
		alias := syntaxSymbolName(definition)
		if !definition.Valid() {
			alias = projectionNameFromSyntax(expression)
		}
		target := plan
		if plan.star {
			target = &returnProjectionPlan{view: view}
		}
		target.addProjection(view.code(expression), alias)
		projection := &target.projections[len(target.projections)-1]
		projection.variable, projection.direct = projectionVariableFromSyntax(expression)
		for _, visit := range expressions {
			visit(expression)
		}
		if !definition.Valid() {
			definition = expression
		}
		projection.definitionStart, projection.definitionEnd = definition.Span()
		if plan.star {
			plan.starItems = append(plan.starItems, item.Code())
			plan.starProjections = append(plan.starProjections, *projection)
		}
	}
	plan.valid = plan.star || len(plan.projections) > 0
	return plan
}

// A bare variable's column is its decoded identity. Other unaliased columns
// retain their original expression text, including internal comments. Code is
// the separate evaluation view; using it as a label loses source information.
func projectionNameFromSyntax(expression cypherantlr.SyntaxNode) string {
	if name, variable := projectionVariableFromSyntax(expression); variable {
		return name
	}
	return expression.Text()
}

func projectionVariableFromSyntax(expression cypherantlr.SyntaxNode) (string, bool) {
	for node := expression; node.Valid(); {
		if name, symbol := node.Name(); symbol {
			return name, true
		}
		if node.ChildCount() != 1 {
			break
		}
		node = node.Child(0)
	}
	return "", false
}

// column is the one decoded output identity used by scope, validation and
// execution. An explicitly empty alias is distinct from no AS clause.
// Existing constructed items with a nonempty alias retain their contract.
func (item yieldItem) column() string {
	if item.aliased || item.alias != "" {
		return item.alias
	}
	return item.name
}

// yieldFromSyntax owns decoded output identities for both execution and facts.
// Modifiers are existing expression views, never comma/keyword text slices.
func yieldFromSyntax(view braceView, clause cypherantlr.SyntaxNode) *yieldClause {
	if _, found := clause.DirectToken(cypherantlr.CypherLexerYIELD); !found {
		return nil
	}
	yield := &yieldClause{view: view, items: []yieldItem{}, whereView: view.window(0, 0), orderView: view.window(0, 0), skipView: view.window(0, 0), limitView: view.window(0, 0)}
	_, yield.yieldAll = clause.DirectToken(cypherantlr.CypherLexerMULT)
	items := clause.ChildRule(cypherantlr.CypherParserRULE_yieldItems)
	if items.Valid() {
		for i := 0; i < items.ChildCount(); i++ {
			item := items.Child(i)
			if item.Rule() != cypherantlr.CypherParserRULE_yieldItem {
				continue
			}
			var symbols []string
			for j := 0; j < item.ChildCount(); j++ {
				symbol := item.Child(j)
				if name, ok := symbol.Name(); ok {
					symbols = append(symbols, name)
				}
			}
			if len(symbols) == 0 {
				continue
			}
			output := yieldItem{name: symbols[0]}
			if len(symbols) > 1 {
				output.alias = symbols[1]
				output.aliased = true
			}
			yield.items = append(yield.items, output)
		}
		where := items.ChildRule(cypherantlr.CypherParserRULE_where)
		if where.Valid() {
			yield.whereView = view.code(where.ChildRule(cypherantlr.CypherParserRULE_expression))
			yield.where = yield.whereView.text()
		}
	}
	for i := 0; i < clause.ChildCount(); i++ {
		part := clause.Child(i)
		switch part.Rule() {
		case cypherantlr.CypherParserRULE_where:
			yield.whereView = view.code(part.ChildRule(cypherantlr.CypherParserRULE_expression))
			yield.where = yield.whereView.text()
			yield.misplacedWhere = yield.orderBy != "" || yield.skip != "" || yield.limit != ""
		case cypherantlr.CypherParserRULE_orderSt:
			var terms []braceView
			for j := 0; j < part.ChildCount(); j++ {
				item := part.Child(j)
				if item.Rule() == cypherantlr.CypherParserRULE_orderItem {
					terms = append(terms, view.code(item))
				}
			}
			yield.orderView = view.assembleCode(terms, ", ")
			yield.orderBy = yield.orderView.text()
		case cypherantlr.CypherParserRULE_skipSt:
			yield.skipView = view.code(part.ChildRule(cypherantlr.CypherParserRULE_expression))
			yield.skip = yield.skipView.text()
		case cypherantlr.CypherParserRULE_limitSt:
			yield.limitView = view.code(part.ChildRule(cypherantlr.CypherParserRULE_expression))
			yield.limit = yield.limitView.text()
		}
	}
	return yield
}
