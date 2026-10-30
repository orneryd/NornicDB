// Author of retained projection inputs: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// projectionItemSource retains source spelling separately from the admitted
// expression/name. An explicitly empty alias still has a nonnegative AS offset.
type projectionItemSource struct {
	expression, alias       string
	rawExpression, rawAlias string
	as                      int
	syntax                  cypherantlr.SyntaxNode
	star                    bool
}

// readProjectionItemSource consumes one admitted projection occurrence. Its
// identity, alias token and source spelling come from the retained CST; a caller
// cannot replace that occurrence with a reconstructed RETURN string.
func readProjectionItemSource(item cypherantlr.SyntaxNode) projectionItemSource {
	out := projectionItemSource{as: -1, syntax: item}
	if !item.Valid() {
		return out
	}
	if item.Rule() == cypherantlr.CypherParserRULE_projectionItems {
		if token, found := item.DirectToken(cypherantlr.CypherLexerMULT); found {
			out.expression, out.alias, out.rawExpression, out.rawAlias = "*", "*", token.Text(), token.Text()
			out.star = true
		}
		return out
	}
	if item.Rule() != cypherantlr.CypherParserRULE_projectionItem {
		return out
	}
	expression := item.ChildRule(cypherantlr.CypherParserRULE_expression)
	if !expression.Valid() {
		return out
	}
	out.expression, out.rawExpression = expression.Code(), expression.Text()
	out.alias, out.rawAlias = projectionNameFromSyntax(expression), out.rawExpression
	if definition := item.ChildRule(cypherantlr.CypherParserRULE_symbol); definition.Valid() {
		out.alias, out.rawAlias = syntaxSymbolName(definition), definition.Text()
	}
	if token, found := item.DirectToken(cypherantlr.CypherLexerAS); found {
		start, _ := token.Span()
		itemStart, _ := item.Span()
		out.as = start - itemStart
	}
	return out
}

// projectionItemsFromSyntax preserves projection order and each occurrence's
// original identity. The grammar owns DISTINCT, item boundaries and the star;
// neither an item nor a WITH body is framed as a new RETURN statement.
func projectionItemsFromSyntax(clause cypherantlr.SyntaxNode) []projectionItemSource {
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	items := body.ChildRule(cypherantlr.CypherParserRULE_projectionItems)
	if !items.Valid() {
		return nil
	}
	var result []projectionItemSource
	if _, star := items.DirectToken(cypherantlr.CypherLexerMULT); star {
		result = append(result, readProjectionItemSource(items))
	}
	for i := 0; i < items.ChildCount(); i++ {
		item := items.Child(i)
		if item.Rule() == cypherantlr.CypherParserRULE_projectionItem {
			result = append(result, readProjectionItemSource(item))
		}
	}
	return result
}

// projectionClauseAt is the one text-side entry to retained projections: the
// deepest RETURN or WITH occurrence of statement containing byte offset at.
// No RETURN text is manufactured for an item or a body; an offset outside
// every projection has no projection rather than a reparsed fragment.
// Author: Claude R17 implementation agent, 30 October 2026.
func projectionClauseAt(ctx context.Context, statement string, at int) (cypherantlr.SyntaxNode, bool) {
	return retainedClauseAt(ctx, statement, at, cypherantlr.CypherParserRULE_returnSt, cypherantlr.CypherParserRULE_withSt)
}

// retainedClauseAt is the clause owner for text-side readers. The statement is
// admitted once through the syntax owner (its retained root when it is the
// executing statement), and the deepest node of one of rules containing at is
// that reader's occurrence. Callers pass offsets they already hold; a slice is
// never reframed as a new statement.
func retainedClauseAt(ctx context.Context, statement string, at int, rules ...int) (cypherantlr.SyntaxNode, bool) {
	if at < 0 || at >= len(statement) || ctx.Err() != nil {
		return cypherantlr.SyntaxNode{}, false
	}
	syntax, err := cypherantlr.InspectSyntax(ctx, statement, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil || !syntax.Valid() || syntax.Source() != statement {
		return cypherantlr.SyntaxNode{}, false
	}
	var found cypherantlr.SyntaxNode
	for node := syntax.Root(); node.Valid(); {
		start, end := node.Span()
		if at < start || at >= end {
			break
		}
		for _, rule := range rules {
			if node.Rule() == rule {
				found = node
			}
		}
		next := cypherantlr.SyntaxNode{}
		for i := 0; i < node.ChildCount(); i++ {
			child := node.Child(i)
			if childStart, childEnd := child.Span(); at >= childStart && at < childEnd {
				next = child
				break
			}
		}
		node = next
	}
	return found, found.Valid()
}
