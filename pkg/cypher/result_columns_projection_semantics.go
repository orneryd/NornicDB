package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// projectionSemantics is the one projection transfer rule. Every item reads
// the incoming scope; all outputs bind together after projectionItems; only
// then do predicates, ordering and windows observe the projected scope. The
// views retain the same grammar nodes used by the executable projection plan.
type projectionSemantics struct {
	clause, body, items cypherantlr.SyntaxNode
}

type projectionSemanticItem struct {
	node, expression, alias cypherantlr.SyntaxNode
}

func projectionSemanticsForSyntax(clause cypherantlr.SyntaxNode) projectionSemantics {
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	return projectionSemantics{clause: clause, body: body, items: body.ChildRule(cypherantlr.CypherParserRULE_projectionItems)}
}

func (projection projectionSemantics) eachItem(visit func(projectionSemanticItem)) bool {
	if !projection.items.Valid() {
		return false
	}
	for index := 0; index < projection.items.ChildCount(); index++ {
		node := projection.items.Child(index)
		if node.Rule() != cypherantlr.CypherParserRULE_projectionItem {
			continue
		}
		expression := node.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !expression.Valid() {
			return false
		}
		visit(projectionSemanticItem{node: node, expression: expression, alias: node.ChildRule(cypherantlr.CypherParserRULE_symbol)})
	}
	return true
}

func (item projectionSemanticItem) output() (string, cypherantlr.SyntaxNode) {
	if item.alias.Valid() {
		return syntaxSymbolName(item.alias), item.alias
	}
	return projectionNameFromSyntax(item.expression), item.expression
}

func projectionOutputSymbol(node cypherantlr.SyntaxNode) bool {
	parent, exists := node.Parent()
	return exists && parent.Rule() == cypherantlr.CypherParserRULE_projectionItem && parent.ChildRule(cypherantlr.CypherParserRULE_symbol).ID() == node.ID()
}

// The existing generic traversal calls the leave action after ALL item inputs.
// Observers never publish an alias while a sibling expression still reads the
// incoming scope. Nested projections use the same boundary automatically.
func projectionOutputsOnLeave(node cypherantlr.SyntaxNode, bind func(string, cypherantlr.SyntaxNode)) func() {
	if node.Rule() != cypherantlr.CypherParserRULE_projectionItems {
		return nil
	}
	body, exists := node.Parent()
	if !exists {
		return nil
	}
	clause, exists := body.Parent()
	if !exists {
		return nil
	}
	projection := projectionSemanticsForSyntax(clause)
	if !projection.items.Valid() || projection.items.ID() != node.ID() {
		return nil
	}
	return func() {
		projection.eachItem(func(item projectionSemanticItem) { name, definition := item.output(); bind(name, definition) })
	}
}

type projectionReadPhase uint8

const (
	projectionIncoming projectionReadPhase = iota
	projectionProjected
	projectionOrdered
	projectionWindow
)

func (projection projectionSemantics) readPhase(node cypherantlr.SyntaxNode) projectionReadPhase {
	for parent, exists := node.Parent(); exists && parent.ID() != projection.clause.ID(); parent, exists = parent.Parent() {
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_projectionItem:
			return projectionIncoming
		case cypherantlr.CypherParserRULE_where:
			if owner, ok := parent.Parent(); ok && owner.ID() == projection.clause.ID() {
				return projectionProjected
			}
		case cypherantlr.CypherParserRULE_orderSt:
			return projectionOrdered
		case cypherantlr.CypherParserRULE_skipSt, cypherantlr.CypherParserRULE_limitSt:
			return projectionWindow
		}
	}
	return projectionIncoming
}

// writtenName consumes the lower naming owner's source-span provenance. Binding
// identity and executable aliases remain canonical; a complex unaliased column
// keeps its written expression, so quoting cannot merge distinct public names.
func (item projectionSemanticItem) writtenName(names *quotedVariableNames) string {
	name, _ := item.output()
	if names == nil {
		return name
	}
	if item.alias.Valid() {
		if original, ok := names.internal[name]; ok {
			return original.name
		}
		return name
	}
	if _, variable := projectionVariableFromSyntax(item.expression); variable {
		if original, ok := names.internal[name]; ok {
			return original.name
		}
		return name
	}
	start, end := item.expression.Span()
	original, _ := names.OriginalText(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
	return original
}
