package fabric

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// nativeQuery adapts an already parsed regular/expression query to the one
// clause projector. Context wrappers do not change native parents or tokens;
// every declaration, role and interval remains owned by the statement tree.
func (r *structureReader) nativeQuery(tree antlr4.ParserRuleContext, inCall bool) *QueryStructure {
	q := r.body(r.nativeBody(tree), inCall)
	q.Input = ExpressionInput{r.facts, r.start(tree.GetStart()), r.end(tree.GetStop())}
	q.tree = tree
	return q
}

func (r *structureReader) nativeBody(tree antlr4.ParserRuleContext) *grammar.FabricBodyContext {
	body := grammar.NewFabricBodyContext(nil, nil, -1)
	body.SetStart(tree.GetStart())
	body.SetStop(tree.GetStop())
	add := func(node antlr4.ParserRuleContext) *grammar.FabricClauseContext {
		clause := grammar.NewFabricClauseContext(nil, nil, -1)
		clause.SetStart(node.GetStart())
		clause.SetStop(node.GetStop())
		clause.AddChild(node)
		body.AddChild(clause)
		return clause
	}
	var previous *grammar.FabricClauseContext
	var visit func(antlr4.Tree)
	visit = func(node antlr4.Tree) {
		switch syntax := node.(type) {
		case grammar.IFabricBodyContext:
			for _, clause := range syntax.AllFabricClause() {
				body.AddChild(clause)
			}
			return
		case grammar.IPatternWhereContext:
			match := grammar.NewMatchStContext(nil, nil, -1)
			match.SetStart(syntax.GetStart())
			match.SetStop(syntax.GetStop())
			match.AddChild(syntax)
			previous = add(match)
			return
		case grammar.IPatternContext:
			pattern := grammar.NewPatternWhereContext(nil, nil, -1)
			pattern.SetStart(syntax.GetStart())
			pattern.SetStop(syntax.GetStop())
			pattern.AddChild(syntax)
			visit(pattern)
			return
		case grammar.IUseClauseContext:
			use := grammar.NewFabricUseClauseContext(nil, nil, -1)
			use.SetStart(syntax.GetStart())
			use.SetStop(syntax.GetStop())
			for _, child := range syntax.GetChildren() {
				if token, ok := child.(antlr4.TerminalNode); ok {
					use.AddTokenNode(token.GetSymbol())
				} else if rule, ok := child.(antlr4.RuleContext); ok {
					use.AddChild(rule)
				}
			}
			previous = add(use)
			return
		case grammar.ICallSubqueryContext:
			call := grammar.NewFabricCallSubqueryContext(nil, nil, -1)
			call.SetStart(syntax.GetStart())
			call.SetStop(syntax.GetStop())
			var scope *grammar.FabricCallScopeContext
			if syntax.LPAREN() != nil {
				scope = grammar.NewFabricCallScopeContext(nil, nil, -1)
				scope.SetStart(syntax.LPAREN().GetSymbol())
				scope.SetStop(syntax.RPAREN().GetSymbol())
			}
			for _, child := range syntax.GetChildren() {
				if nested, ok := child.(grammar.ISubqueryBodyContext); ok {
					adapted := r.nativeBody(nested)
					call.AddChild(adapted)
				} else if token, ok := child.(antlr4.TerminalNode); ok {
					at := token.GetSymbol().GetTokenIndex()
					if scope != nil && at >= scope.GetStart().GetTokenIndex() && at <= scope.GetStop().GetTokenIndex() {
						scope.AddTokenNode(token.GetSymbol())
						if at == scope.GetStop().GetTokenIndex() {
							call.AddChild(scope)
						}
					} else {
						call.AddTokenNode(token.GetSymbol())
					}
				} else if rule, ok := child.(antlr4.RuleContext); ok {
					if _, symbol := child.(grammar.ISymbolContext); symbol && scope != nil {
						scope.AddChild(rule)
					} else {
						call.AddChild(rule)
					}
				}
			}
			previous = add(call)
			return
		case grammar.IWithStContext, grammar.IReturnStContext, grammar.IMatchStContext,
			grammar.IUnwindStContext, grammar.ICreateStContext, grammar.IMergeStContext,
			grammar.IDeleteStContext, grammar.ISetStContext, grammar.IRemoveStContext,
			grammar.IForeachStContext, grammar.ILetStContext, grammar.IFilterStContext,
			grammar.IForStContext, grammar.IQueryCallStContext, grammar.IOrderStContext,
			grammar.ISkipStContext, grammar.ILimitStContext:
			previous = add(node.(antlr4.ParserRuleContext))
			return
		case grammar.IEmbeddingStContext:
			if previous != nil {
				previous.AddChild(syntax)
				previous.SetStop(syntax.GetStop())
			}
			return
		case antlr4.TerminalNode:
			switch syntax.GetSymbol().GetTokenType() {
			case grammar.CypherParserUNION, grammar.CypherParserFINISH:
				previous = grammar.NewFabricClauseContext(nil, nil, -1)
				previous.SetStart(syntax.GetSymbol())
				previous.SetStop(syntax.GetSymbol())
				previous.AddTokenNode(syntax.GetSymbol())
				body.AddChild(previous)
			case grammar.CypherParserALL:
				if previous != nil && previous.UNION() != nil {
					previous.AddTokenNode(syntax.GetSymbol())
					previous.SetStop(syntax.GetSymbol())
				}
			}
			return
		}
		for index := 0; index < node.GetChildCount(); index++ {
			visit(node.GetChild(index))
		}
	}
	visit(tree)
	return body
}

// QueryScopes returns query descriptors contained by this operand, with the
// same native fact owner. Acquiring a nested body never invokes a parser.
func (input ExpressionInput) QueryScopes() map[ExpressionSpan]*QueryStructure {
	input = input.resolved()
	if !input.Valid() {
		return nil
	}
	facts := input.facts
	facts.queriesOnce.Do(func() {
		facts.queries = make(map[ExpressionSpan]*QueryStructure)
		positions := facts.positions
		walkSyntax(facts.tree, func(tree antlr4.Tree) {
			var body antlr4.ParserRuleContext
			switch syntax := tree.(type) {
			case grammar.ISubqueryExistContext:
				if syntax.RegularQuery() != nil {
					body = syntax.RegularQuery()
				} else if syntax.PatternWhere() != nil {
					body = syntax.PatternWhere()
				}
			case grammar.ICountSubqueryContext:
				if syntax.SubqueryBody() != nil {
					body = syntax.SubqueryBody()
				} else if syntax.MatchSt() != nil {
					body = syntax.MatchSt()
				} else if syntax.Pattern() != nil {
					pattern := grammar.NewPatternWhereContext(nil, nil, -1)
					pattern.SetStart(syntax.Pattern().GetStart())
					pattern.SetStop(syntax.GetStop())
					pattern.AddChild(syntax.Pattern())
					if syntax.Where() != nil {
						pattern.AddChild(syntax.Where())
						pattern.SetStop(syntax.Where().GetStop())
					} else {
						pattern.SetStop(syntax.Pattern().GetStop())
					}
					body = pattern
				}
			case grammar.ICollectSubqueryContext:
				body = syntax.RegularQuery()
			}
			if body != nil {
				node := tree.(antlr4.ParserRuleContext)
				span := ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}
				facts.queries[span] = positions.nativeQuery(body, true)
			}
		})
	})
	selected := make(map[ExpressionSpan]*QueryStructure)
	for span, query := range facts.queries {
		if span.Start >= input.start && span.End <= input.end {
			selected[ExpressionSpan{span.Start - input.start, span.End - input.start}] = query
		}
	}
	return selected
}
