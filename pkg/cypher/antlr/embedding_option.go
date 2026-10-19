package antlr

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
	"strings"
)

// The probe parses every ambiguous WITH as a projection. It is independent of
// the active parser (including validators with BuildParseTrees=false), and its
// stream marker prevents recursive option prediction.
type embeddingProjectionStream struct{ *antlr4.CommonTokenStream }

// EmbeddingOptionAllowed is the single grammar decision for the native option.
// A terminal WITH cannot be a Cypher projection. Otherwise a bound variable
// always wins over the extension, including keyword-spelled and quoted names.
func (p *CypherParser) EmbeddingOptionAllowed() bool {
	stream := p.GetTokenStream()
	if _, probe := stream.(*embeddingProjectionStream); probe {
		return false
	}
	if stream.LA(1) != CypherParserWITH || stream.LA(2) != CypherParserEMBEDDING {
		return false
	}
	terminal := false
	switch stream.LA(3) {
	case antlr4.TokenEOF, CypherParserSEMI, CypherParserRBRACE, CypherParserUNION:
		terminal = true
	}
	position := stream.LT(1).GetStart()
	tree, _ := p.embeddingProjectionTree()
	allowed, found := embeddingOptionInBody(tree.FabricBody(), position, map[string]bool{}, terminal)
	return found && allowed
}

func embeddingName(symbol ISymbolContext) string {
	if symbol == nil {
		return ""
	}
	text := symbol.GetText()
	if strings.HasPrefix(text, "`") {
		return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(text, "`"), "`"), "``", "`")
	}
	return text
}

// A possible option must be exactly the single unaliased symbol, without
// modifiers or predicates. AS, expressions and multi-item projections retain
// their ordinary meaning even when the variable is not currently bound.
func embeddingProjection(with IWithStContext) (string, bool) {
	if with == nil || with.Where() != nil {
		return "", false
	}
	body := with.ProjectionBody()
	if body.DISTINCT() != nil || body.OrderSt() != nil || body.SkipSt() != nil || body.LimitSt() != nil {
		return "", false
	}
	items := body.ProjectionItems()
	if items.MULT() != nil || len(items.AllProjectionItem()) != 1 {
		return "", false
	}
	item := items.ProjectionItem(0)
	if item.Symbol() != nil {
		return "", false
	}
	var tree antlr4.Tree = item.Expression()
	for tree != nil {
		if symbol, ok := tree.(ISymbolContext); ok {
			name := embeddingName(symbol)
			return name, strings.EqualFold(name, "embedding")
		}
		if tree.GetChildCount() != 1 {
			break
		}
		tree = tree.GetChild(0)
	}
	return "", false
}

func embeddingOptionInBody(body IFabricBodyContext, position int, scope map[string]bool, terminal bool) (bool, bool) {
	updating := false
	for _, clause := range body.AllFabricClause() {
		if clause.GetStart().GetStart() > position {
			break
		}
		if with := clause.WithSt(); with != nil {
			name, possible := embeddingProjection(with)
			if with.GetStart().GetStart() == position {
				return possible && updating && (terminal || !scope[name]), true
			}
			// Earlier unbound option syntax preserves scope rather than projecting an
			// undefined variable. This is the same decision as for the target clause.
			if possible && updating && !scope[name] {
				continue
			}
			next := map[string]bool{}
			if with.ProjectionBody().ProjectionItems().MULT() != nil {
				for name := range scope {
					next[name] = true
				}
			}
			for _, item := range with.ProjectionBody().ProjectionItems().AllProjectionItem() {
				if item.Symbol() != nil {
					next[embeddingName(item.Symbol())] = true
				} else {
					next[embeddingExpressionName(item.Expression())] = true
				}
			}
			scope = next
			updating = false
			continue
		}
		if call := clause.FabricCallSubquery(); call != nil {
			if call.GetStart().GetStart() < position && call.GetStop().GetStop() >= position {
				imported := map[string]bool{}
				if call.FabricCallScope() != nil && call.FabricCallScope().MULT() != nil || call.FabricCallScope() == nil && legacyEmbeddingImports(call.FabricBody()) {
					for name := range scope {
						imported[name] = true
					}
				} else if call.FabricCallScope() != nil {
					for _, symbol := range call.FabricCallScope().AllSymbol() {
						imported[embeddingName(symbol)] = scope[embeddingName(symbol)]
					}
				}
				return embeddingOptionInBody(call.FabricBody(), position, imported, terminal)
			}
			// Only returned columns escape a subquery.
			for _, inner := range call.FabricBody().AllFabricClause() {
				if ret := inner.ReturnSt(); ret != nil {
					for _, item := range ret.ProjectionBody().ProjectionItems().AllProjectionItem() {
						if item.Symbol() != nil {
							scope[embeddingName(item.Symbol())] = true
						} else {
							scope[embeddingExpressionName(item.Expression())] = true
						}
					}
				}
			}
			updating = false
			continue
		}
		if clause.UNION() != nil || clause.SEMI() != nil {
			scope = map[string]bool{}
			updating = false
			continue
		}
		var bind func(antlr4.Tree, antlr4.Tree)
		bind = func(tree, parent antlr4.Tree) {
			switch tree.(type) {
			case IForeachStContext, IFilterExpressionContext, IReduceExpressionContext, IAllReduceExpressionContext:
				return
			}
			if symbol, ok := tree.(ISymbolContext); ok && RoleOfSymbol(symbol, parent).QueryBinding {
				scope[embeddingName(symbol)] = true
			}
			for i := 0; i < tree.GetChildCount(); i++ {
				bind(tree.GetChild(i), tree)
			}
		}
		bind(clause, nil)
		updating = clause.CreateSt() != nil || clause.FabricInsertSt() != nil || clause.MergeSt() != nil || clause.DeleteSt() != nil || clause.FabricNoDetachDeleteSt() != nil || clause.SetSt() != nil || clause.RemoveSt() != nil || clause.ForeachSt() != nil
	}
	return false, false
}

func embeddingExpressionName(expression IExpressionContext) string {
	var tree antlr4.Tree = expression
	for tree != nil {
		if symbol, ok := tree.(ISymbolContext); ok {
			return embeddingName(symbol)
		}
		if tree.GetChildCount() != 1 {
			break
		}
		tree = tree.GetChild(0)
	}
	return expression.GetText()
}

// ProjectionPartAllowed exposes the same structural decision at the native
// pipeline alternative, before updating clauses have been consumed. This keeps
// prediction from committing to a projection pipeline for an actual option.
func (p *CypherParser) ProjectionPartAllowed() bool {
	stream := p.GetTokenStream()
	if _, probe := stream.(*embeddingProjectionStream); probe {
		return true
	}
	// This is only a negative lexical fast path, never a clause decision.
	// Queries without an EMBEDDING token have no extension ambiguity.
	ambiguous := false
	for i := 1; ; i++ {
		kind := stream.LA(i)
		if kind == CypherParserEMBEDDING {
			ambiguous = true
			break
		}
		if kind == antlr4.TokenEOF {
			break
		}
	}
	if !ambiguous {
		return true
	}
	position := stream.LT(1).GetStart()
	tree, tokens := p.embeddingProjectionTree()
	with := nextProjectionPart(tree.FabricBody(), position)
	if with == nil {
		return false
	}
	// A complete ordinary projection needs no option decision.
	if _, possible := embeddingProjection(with); !possible {
		return true
	}
	tokenIndex := with.GetStop().GetTokenIndex() + 1
	for tokens.Get(tokenIndex).GetChannel() != antlr4.TokenDefaultChannel {
		tokenIndex++
	}
	next := tokens.Get(tokenIndex).GetTokenType()
	terminal := next == antlr4.TokenEOF || next == CypherParserSEMI || next == CypherParserRBRACE || next == CypherParserUNION
	option, found := embeddingOptionInBody(tree.FabricBody(), with.GetStart().GetStart(), map[string]bool{}, terminal)
	return found && !option
}

func nextProjectionPart(body IFabricBodyContext, position int) IWithStContext {
	for _, clause := range body.AllFabricClause() {
		if call := clause.FabricCallSubquery(); call != nil && call.GetStart().GetStart() < position && call.GetStop().GetStop() >= position {
			return nextProjectionPart(call.FabricBody(), position)
		}
		if clause.GetStart().GetStart() < position {
			continue
		}
		if clause.UNION() != nil || clause.SEMI() != nil {
			break
		}
		if with := clause.WithSt(); with != nil {
			return with
		}
	}
	return nil
}

func legacyEmbeddingImports(body IFabricBodyContext) bool {
	for _, clause := range body.AllFabricClause() {
		if clause.FabricUseClause() != nil {
			continue
		}
		return clause.WithSt() != nil
	}
	return false
}

// Both native prediction points use exactly the same projection-first tree.
func (p *CypherParser) embeddingProjectionTree() (IFabricScriptContext, *antlr4.CommonTokenStream) {
	input := p.GetTokenStream().GetTokenSource().GetInputStream()
	lexer := NewCypherLexer(antlr4.NewInputStream(input.GetText(0, input.Size()-1)))
	lexer.RemoveErrorListeners()
	tokens := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	probe := NewCypherParser(&embeddingProjectionStream{tokens})
	probe.RemoveErrorListeners()
	return probe.FabricScript(), tokens
}
