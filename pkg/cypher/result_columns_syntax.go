package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// canonicalQueryClause adapts standalone execution helpers to the same grammar
// product. The bound compiler supplies the retained clause node directly.
func canonicalQueryClause(source string) (cypherantlr.SyntaxNode, bool) {
	syntax, err := cypherantlr.ParseSyntax(context.Background(), source, cypherantlr.FragmentEntry, false)
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

func projectionPlanFromSyntax(clause cypherantlr.SyntaxNode) *returnProjectionPlan {
	plan := &returnProjectionPlan{}
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
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
	if where := clause.ChildRule(cypherantlr.CypherParserRULE_where); where.Valid() {
		plan.predicate = where.ChildRule(cypherantlr.CypherParserRULE_expression).Code()
	}
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
		alias := syntaxSymbolName(item.ChildRule(cypherantlr.CypherParserRULE_symbol))
		if alias == "" {
			alias = expression.Code()
			if name := simpleSemanticIdentifier(alias); name != "" {
				alias = name
			}
		}
		target := plan
		if plan.star {
			target = &returnProjectionPlan{}
		}
		target.addProjection(expression.Code(), alias)
		projection := &target.projections[len(target.projections)-1]
		// A direct variable follows a chain of single-child grammar rules to
		// an atom's symbol. Literals and compound expressions have other roles;
		// their spelling must never make them identity projections.
		variable := expression
		for variable.ChildCount() == 1 && variable.Rule() != cypherantlr.CypherParserRULE_symbol {
			variable = variable.Child(0)
		}
		if variableSyntaxRead(variable) {
			projection.variable = syntaxSymbolName(variable)
		}
		definition := item.ChildRule(cypherantlr.CypherParserRULE_symbol)
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
