package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// canonicalQueryClause adapts standalone execution helpers to the same grammar
// product. The bound compiler supplies the retained clause node directly.
func canonicalQueryClause(ctx context.Context, source string) (cypherantlr.SyntaxNode, bool) {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return cypherantlr.SyntaxNode{}, false
	}
	root := statementSyntaxQuery(syntax)
	if !root.Valid() {
		root, _ = syntax.Window(0, len(source), cypherantlr.CypherParserRULE_subqueryBody)
	}
	body := queryBodyFromSyntax(root)
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

func projectionPlanFromSyntax(clause cypherantlr.SyntaxNode, expressions ...func(cypherantlr.SyntaxNode)) *returnProjectionPlan {
	return projectionPlanFromSemantics(projectionSemanticsForSyntax(clause), expressions...)
}

func projectionPlanFromSemantics(projection projectionSemantics, expressions ...func(cypherantlr.SyntaxNode)) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	body := projection.body
	if !body.Valid() {
		return plan
	}
	_, plan.distinct = body.DirectToken(cypherantlr.CypherLexerDISTINCT)
	var modifiers []string
	for i := 0; i < body.ChildCount(); i++ {
		node := body.Child(i)
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_orderSt, cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
			modifiers = append(modifiers, node.Code())
		}
	}
	plan.modifiers = strings.Join(modifiers, " ")
	items := projection.items
	if !items.Valid() {
		return plan
	}
	_, plan.star = items.DirectToken(cypherantlr.CypherLexerMULT)
	valid := projection.eachItem(func(item projectionSemanticItem) {
		expression := item.expression
		alias, definition := item.output()
		target := plan
		if plan.star {
			target = &returnProjectionPlan{}
		}
		target.addProjection(expression.Code(), alias)
		projected := &target.projections[len(target.projections)-1]
		projected.syntax = expression
		projected.variable, projected.direct = projectionVariableFromSyntax(expression)
		for _, visit := range expressions {
			visit(expression)
		}
		projected.definitionStart, projected.definitionEnd = definition.Span()
		if plan.star {
			plan.starItems = append(plan.starItems, item.node.Code())
			plan.starProjections = append(plan.starProjections, *projected)
		}
	})
	if !valid {
		return &returnProjectionPlan{}
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
	semantic := expression.SemanticExpression()
	if semantic.Valid() {
		return semantic.Name()
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
func yieldFromSyntax(clause cypherantlr.SyntaxNode) *yieldClause {
	if _, found := clause.DirectToken(cypherantlr.CypherLexerYIELD); !found {
		return nil
	}
	yield := &yieldClause{items: []yieldItem{}}
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
			yield.where = where.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
		}
	}
	for i := 0; i < clause.ChildCount(); i++ {
		part := clause.Child(i)
		switch part.Rule() {
		case cypherantlr.CypherParserRULE_where:
			yield.where = part.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
			yield.misplacedWhere = yield.orderBy != "" || yield.skip != "" || yield.limit != ""
		case cypherantlr.CypherParserRULE_orderSt:
			var terms []string
			for j := 0; j < part.ChildCount(); j++ {
				item := part.Child(j)
				if item.Rule() == cypherantlr.CypherParserRULE_orderItem {
					terms = append(terms, item.Code())
				}
			}
			yield.orderBy = strings.Join(terms, ", ")
		case cypherantlr.CypherParserRULE_skipSt:
			yield.skip = part.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
		case cypherantlr.CypherParserRULE_limitSt:
			yield.limit = part.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
		}
	}
	return yield
}
