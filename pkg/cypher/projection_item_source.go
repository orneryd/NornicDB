package cypher

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type projectionSourceStage uint8

const (
	projectionAdmitted projectionSourceStage = iota
	projectionBeforeRewrite
)

// projectionItemSource retains source spelling separately from the admitted
// expression/name. An explicitly empty alias still has a nonnegative AS offset.
// syntax is the occurrence a retained caller read; star marks the * entry of a
// projection list.
type projectionItemSource struct {
	expression, alias       string
	rawExpression, rawAlias string
	as                      int
	syntax                  cypherantlr.SyntaxNode
	star                    bool
}

// readProjectionItemSource owns alias selection products at both actual stages.
// Pre-rewrite grouping retains its permissive last-word/source-spelling policy;
// admitted consumers use the retained grammar AS and execution projection names.
func readProjectionItemSource(ctx context.Context, item string, stage projectionSourceStage) projectionItemSource {
	out := projectionItemSource{as: -1}
	var clause cypherantlr.SyntaxNode
	if stage == projectionBeforeRewrite {
		trimmed := strings.TrimSpace(item)
		leading := len(item) - len(strings.TrimLeftFunc(item, cyphertext.IsWhitespace))
		for _, word := range projectionWords(trimmed) {
			if word.depth == 0 && word.upper == "AS" {
				out.as = leading + word.start
			}
		}
	} else {
		const prefix = "RETURN "
		var ok bool
		clause, ok = canonicalQueryClause(ctx, prefix+item)
		if !ok {
			return out
		}
		items := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody).ChildRule(cypherantlr.CypherParserRULE_projectionItems)
		if items.Valid() {
			projection := items.ChildRule(cypherantlr.CypherParserRULE_projectionItem)
			if projection.Valid() {
				if token, found := projection.DirectToken(cypherantlr.CypherLexerAS); found {
					start, _ := token.Span()
					out.as = start - len(prefix)
				}
			}
		}
	}
	// Both stages share original source boundaries. Semantic code/decoded names
	// below must not replace raw alias text used by AST and diagnostic consumers.
	out.rawExpression = strings.TrimSpace(item)
	out.rawAlias = out.rawExpression
	if out.as >= 0 {
		out.rawExpression = strings.TrimSpace(item[:out.as])
		out.rawAlias = strings.TrimSpace(item[out.as+len("AS"):])
	}
	if stage == projectionBeforeRewrite {
		out.expression, out.alias = out.rawExpression, out.rawAlias
		if out.as >= 0 {
			if name, ok := cyphertext.ScanIdentifier(out.rawAlias, 0); ok && name.End == len(out.rawAlias) {
				out.alias = name.Value
			}
		}
		return out
	}
	// The admitted item products keep their one owner in the retained reader;
	// this carrier contributes the AS offset measured in its own item text.
	items := projectionItemsFromSyntax(clause)
	if len(items) != 1 {
		return out
	}
	out.expression, out.alias = items[0].expression, items[0].alias
	if items[0].star {
		out.expression, out.alias = "*", "*"
	}
	out.syntax, out.star = items[0].syntax, items[0].star
	return out
}

// readProjectionItemSyntax reads one admitted projection occurrence from the
// statement's retained syntax: its evaluation expression, its output name and
// its exact source spelling. A caller holding the statement cannot replace the
// occurrence with a reconstructed RETURN item.
func readProjectionItemSyntax(item cypherantlr.SyntaxNode) projectionItemSource {
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
// original identity. The grammar keeps DISTINCT, item boundaries and the star
// as occurrences; neither an item nor a clause body is framed as a new RETURN
// statement. The body's modifier is read through projectionBodyDistinct.
func projectionItemsFromSyntax(clause cypherantlr.SyntaxNode) []projectionItemSource {
	body := clause.ChildRule(cypherantlr.CypherParserRULE_projectionBody)
	items := body.ChildRule(cypherantlr.CypherParserRULE_projectionItems)
	if !items.Valid() {
		return nil
	}
	var result []projectionItemSource
	if _, star := items.DirectToken(cypherantlr.CypherLexerMULT); star {
		result = append(result, readProjectionItemSyntax(items))
	}
	for index := 0; index < items.ChildCount(); index++ {
		if item := items.Child(index); item.Rule() == cypherantlr.CypherParserRULE_projectionItem {
			result = append(result, readProjectionItemSyntax(item))
		}
	}
	return result
}

// projectionBodyDistinct is the one reader of a WITH or RETURN projection
// body's DISTINCT modifier: it reports whether the body's item list is the
// one after the keyword, and Neo4j's SyntaxError when the statement has no
// such projection.
//
// A retained DISTINCT token is that modifier. Without the token the body's own
// source decides, through cutDistinct: the classifier every projection
// consumer shares reads a word spelled distinct as the keyword whenever what
// follows can start an expression (#894). The grammar reads the word as the
// keyword only when the rest also parses as a projection item list
// (projectionBody: DISTINCT? projectionItems), so where the rest cannot parse
// it can instead keep distinct inside the first item's expression (RETURN
// distinct STARTS WITH 'a' AS v: distinct STARTS WITH 'a' reads as an operator
// expression). Neo4j reads DISTINCT as the keyword there, which leaves an item
// beginning with a word operator: a SyntaxError at that word, never a
// projection of a variable spelled distinct.
func projectionBodyDistinct(body cypherantlr.SyntaxNode) (bool, error) {
	items := body.ChildRule(cypherantlr.CypherParserRULE_projectionItems)
	if !items.Valid() {
		return false, nil
	}
	if _, retained := body.DirectToken(cypherantlr.CypherLexerDISTINCT); retained {
		return true, nil
	}
	// A body without the modifier token starts with the word the grammar kept
	// in its first item; the item list's own source is what the classifier
	// reads.
	start, _ := body.Span()
	_, end := items.Span()
	rest, distinct := cutDistinct(body.Source()[start:end])
	if !distinct {
		return false, nil
	}
	token, _ := cyphertext.ScanIdentifier(rest, 0)
	if token.Value == "" && rest != "" {
		word, _ := utf8.DecodeRuneInString(rest)
		token.Value = string(word)
	}
	return true, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCoreInvalidInputExpectedExpression(token.Value))
}
