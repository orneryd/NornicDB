package fabric

import (
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// ExpressionSpan is a native token span in the caller's source text.
type ExpressionSpan struct{ Start, End int }

// ExpressionDeclaration records one grammar-owned declaration and the source
// that types it. Pattern declarations can refer to an existing outer binding;
// the semantic type owner, rather than this syntax boundary, decides that.
type ExpressionDeclaration struct {
	Name                                            string
	At, End                                         int
	Source                                          ExpressionSpan
	Element, Path, Pattern, Query, MapKey, MapValue bool
}

type ExpressionScopeKind uint8

const (
	ExpressionLocalScope ExpressionScopeKind = iota
	ExpressionPatternScope
	ExpressionQueryScope
	ExpressionIterationScope
	ExpressionMapScope
)

// ExpressionScope carries declarations, visibility and operand spans together.
// All offsets come from native grammar tokens; no consumer scans preceding text
// to decide whether a name is local or where its lifetime starts.
type ExpressionScope struct {
	Span                                       ExpressionSpan
	Kind                                       ExpressionScopeKind
	Function                                   string
	Open, Close, Visible, Iteration            int
	QueryDepth                                 int
	Input, Predicate, Projection, Key, Pattern ExpressionSpan
	Declarations                               []ExpressionDeclaration
}

func (scope ExpressionScope) Binds(name string, at int) bool {
	for _, declaration := range scope.Declarations {
		if !declaration.Query && declaration.Name == name && (declaration.At == at || scope.Visible <= at && at < scope.Close) {
			return true
		}
	}
	return false
}

// ExpressionScopes and ExpressionReferences are views of the same immutable
// native parse and cache entry, including complete-clause and operand inputs.
func ExpressionScopes(text string) []ExpressionScope {
	query, offset := expressionFacts(text)
	if query == nil {
		return nil
	}
	if offset != 0 {
		return query.relativeScopes
	}
	return query.expressionScopes
}

// RootExpressionScope selects a binding form that owns the complete operand.
// A nested comprehension is never mistaken for its containing literal or call.
func RootExpressionScope(text string) (ExpressionScope, bool) {
	for _, scope := range ExpressionScopes(text) {
		if scope.Span.Start == 0 && scope.Span.End == len(text) {
			return scope, true
		}
	}
	return ExpressionScope{}, false
}

// ComprehensionSyntax reads the argument-body grammar entry shared by list
// evaluators and quantifiers. The offset-adjusted descriptor is cached by the
// original input, so row execution does not construct wrappers or parse again.
func ComprehensionSyntax(body string) (ExpressionScope, bool) {
	input := ReadQueryStructure(body)
	input.comprehensionOnce.Do(func() {
		for _, scope := range ExpressionScopes("[" + body + "]") {
			if scope.Open == 0 && scope.Kind == ExpressionLocalScope {
				input.comprehension = relativeExpressionScopes([]ExpressionScope{scope}, 1)[0]
				input.comprehensionParsed = true
				break
			}
		}
	})
	return input.comprehension, input.comprehensionParsed
}

// ReduceSyntax uses the native reduce/allReduce entry for every evaluator and
// checker. Declarations and the initial/list/step/predicate spans stay together.
func ReduceSyntax(all bool, body string) (ExpressionScope, bool) {
	input := ReadQueryStructure(body)
	index, name := 0, "reduce"
	if all {
		index, name = 1, "allReduce"
	}
	input.reduceOnce[index].Do(func() {
		for _, scope := range ExpressionScopes(name + "(" + body + ")") {
			if scope.Open == len(name) && scope.Function != "" {
				input.reduceScopes[index] = relativeExpressionScopes([]ExpressionScope{scope}, len(name)+1)[0]
				input.reduceParsed[index] = true
				break
			}
		}
	})
	return input.reduceScopes[index], input.reduceParsed[index]
}

func nativeExpressionScopes(tree antlr4.Tree, positions *structureReader) []ExpressionScope {
	var scopes []ExpressionScope
	span := func(node antlr4.ParserRuleContext) ExpressionSpan {
		if node == nil {
			return ExpressionSpan{}
		}
		return ExpressionSpan{positions.start(node.GetStart()), positions.end(node.GetStop())}
	}
	declare := func(scope *ExpressionScope, symbol grammar.ISymbolContext, source ExpressionSpan, element, path, pattern bool) {
		role := symbolRole(symbol, symbol.GetParent())
		if role.binding {
			scope.Declarations = append(scope.Declarations, ExpressionDeclaration{Name: unquoteSyntaxName(symbol.GetText()), At: positions.start(symbol.GetStart()), End: positions.end(symbol.GetStop()), Source: source, Element: element, Path: path, Pattern: pattern, Query: role.queryBinding && !pattern})
		}
	}
	filter := func(scope *ExpressionScope, expression grammar.IFilterExpressionContext, pipe antlr4.TerminalNode, projection grammar.IExpressionContext) {
		scope.Input = span(expression.Expression())
		declare(scope, expression.Symbol(), scope.Input, true, false, false)
		scope.Visible = scope.Close
		if expression.Where() != nil {
			scope.Visible = positions.start(expression.Where().GetStart())
			scope.Predicate = span(expression.Where().Expression())
		} else if pipe != nil {
			scope.Visible = positions.start(pipe.GetSymbol())
		}
		if projection != nil {
			scope.Projection = span(projection)
		}
	}
	walkSyntaxParents(tree, nil, func(node, parent antlr4.Tree) {
		var scope ExpressionScope
		switch syntax := node.(type) {
		case grammar.IListComprehensionContext:
			scope.Open, scope.Close = positions.start(syntax.LBRACK().GetSymbol()), positions.start(syntax.RBRACK().GetSymbol())
			filter(&scope, syntax.FilterExpression(), syntax.STICK(), syntax.Expression())
		case grammar.IFilterWithContext:
			scope.Open, scope.Close = positions.start(syntax.LPAREN().GetSymbol()), positions.start(syntax.RPAREN().GetSymbol())
			scope.Function = strings.ToLower(syntax.GetStart().GetText())
			filter(&scope, syntax.FilterExpression(), nil, nil)
		case grammar.IReduceExpressionContext:
			scope.Open, scope.Close = positions.start(syntax.LPAREN().GetSymbol()), positions.start(syntax.RPAREN().GetSymbol())
			scope.Visible, scope.Function = positions.start(syntax.STICK().GetSymbol()), strings.ToLower(syntax.GetStart().GetText())
			scope.Input, scope.Projection = span(syntax.Expression(1)), span(syntax.Expression(2))
			declare(&scope, syntax.Symbol(0), span(syntax.Expression(0)), false, false, false)
			declare(&scope, syntax.Symbol(1), scope.Input, true, false, false)
		case grammar.IAllReduceExpressionContext:
			scope.Open, scope.Close = positions.start(syntax.LPAREN().GetSymbol()), positions.start(syntax.RPAREN().GetSymbol())
			scope.Visible, scope.Function = positions.start(syntax.STICK().GetSymbol()), strings.ToLower(syntax.GetStart().GetText())
			scope.Input, scope.Projection, scope.Predicate = span(syntax.Expression(1)), span(syntax.Expression(2)), span(syntax.Expression(3))
			declare(&scope, syntax.Symbol(0), span(syntax.Expression(0)), false, false, false)
			declare(&scope, syntax.Symbol(1), scope.Input, true, false, false)
		case grammar.IMapComprehensionContext:
			scope.Kind = ExpressionMapScope
			scope.Open, scope.Close = positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
			scope.Visible, scope.Iteration = positions.start(syntax.STICK().GetSymbol()), positions.start(syntax.IN().GetSymbol())
			scope.Input, scope.Key, scope.Projection = span(syntax.Expression(0)), span(syntax.Expression(1)), span(syntax.Expression(2))
			if syntax.Where() != nil {
				scope.Visible, scope.Predicate = positions.start(syntax.Where().GetStart()), span(syntax.Where().Expression())
			}
			declare(&scope, syntax.Symbol(0), scope.Input, false, false, false)
			declare(&scope, syntax.Symbol(1), scope.Input, false, false, false)
			scope.Declarations[0].MapKey, scope.Declarations[1].MapValue = true, true
		case grammar.IPatternComprehensionContext:
			scope.Kind = ExpressionPatternScope
			scope.Open, scope.Close = positions.start(syntax.LBRACK().GetSymbol()), positions.start(syntax.RBRACK().GetSymbol())
			scope.Pattern, scope.Projection = span(syntax.RelationshipsChainPattern()), span(syntax.Expression())
			scope.Visible = scope.Pattern.Start
			if syntax.Where() != nil {
				scope.Predicate = span(syntax.Where().Expression())
			}
			if syntax.Lhs() != nil {
				declare(&scope, syntax.Lhs().Symbol(), ExpressionSpan{}, false, true, false)
			}
			walkSyntaxParents(syntax.RelationshipsChainPattern(), nil, func(node, parent antlr4.Tree) {
				if symbol, ok := node.(grammar.ISymbolContext); ok {
					switch parent.(type) {
					case grammar.INodePatternContext, grammar.IRelationDetailContext, grammar.IPatternPartContext:
						declare(&scope, symbol, scope.Pattern, false, false, true)
					}
				}
			})
		case grammar.IForeachStContext:
			scope.Kind = ExpressionIterationScope
			scope.Open, scope.Close = positions.start(syntax.LPAREN().GetSymbol()), positions.start(syntax.RPAREN().GetSymbol())
			scope.Visible, scope.Iteration = positions.start(syntax.STICK().GetSymbol()), positions.start(syntax.IN().GetSymbol())
			scope.Input = span(syntax.Expression())
			declare(&scope, syntax.Symbol(), scope.Input, true, false, false)
		case grammar.IForStContext:
			scope.Kind = ExpressionIterationScope
			scope.Open, scope.Close, scope.Iteration = positions.start(syntax.GetStart()), positions.end(syntax.GetStop()), positions.start(syntax.IN().GetSymbol())
			scope.Visible, scope.Input = scope.Close, span(syntax.Expression())
			declare(&scope, syntax.Symbol(), scope.Input, true, false, false)
		case grammar.ISubqueryExistContext:
			scope.Kind, scope.Open, scope.Close = ExpressionQueryScope, positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
		case grammar.ICountSubqueryContext:
			scope.Kind, scope.Open, scope.Close = ExpressionQueryScope, positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
		case grammar.ICollectSubqueryContext:
			scope.Kind, scope.Open, scope.Close = ExpressionQueryScope, positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
		case grammar.ICallSubqueryContext:
			scope.Kind, scope.Open, scope.Close = ExpressionQueryScope, positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
		case grammar.IFabricCallSubqueryContext:
			scope.Kind, scope.Open, scope.Close = ExpressionQueryScope, positions.start(syntax.LBRACE().GetSymbol()), positions.start(syntax.RBRACE().GetSymbol())
		default:
			return
		}
		for ancestor := node.GetParent(); ancestor != nil; ancestor = ancestor.GetParent() {
			switch ancestor.(type) {
			case grammar.ISubqueryExistContext, grammar.ICountSubqueryContext, grammar.ICollectSubqueryContext, grammar.ICallSubqueryContext, grammar.IFabricCallSubqueryContext:
				scope.QueryDepth++
			}
		}
		scope.Span = span(node.(antlr4.ParserRuleContext))
		scopes = append(scopes, scope)
	})
	return scopes
}

func relativeExpressionScopes(scopes []ExpressionScope, offset int) []ExpressionScope {
	shift := func(span ExpressionSpan) ExpressionSpan {
		if span.End > span.Start {
			span.Start -= offset
			span.End -= offset
		}
		return span
	}
	result := make([]ExpressionScope, len(scopes))
	for index, scope := range scopes {
		scope.Span = shift(scope.Span)
		scope.Open -= offset
		scope.Close -= offset
		scope.Visible -= offset
		if scope.Iteration != 0 {
			scope.Iteration -= offset
		}
		scope.Input, scope.Predicate, scope.Projection, scope.Key, scope.Pattern = shift(scope.Input), shift(scope.Predicate), shift(scope.Projection), shift(scope.Key), shift(scope.Pattern)
		scope.Declarations = append([]ExpressionDeclaration(nil), scope.Declarations...)
		for index := range scope.Declarations {
			scope.Declarations[index].At -= offset
			scope.Declarations[index].End -= offset
			scope.Declarations[index].Source = shift(scope.Declarations[index].Source)
		}
		result[index] = scope
	}
	return result
}
