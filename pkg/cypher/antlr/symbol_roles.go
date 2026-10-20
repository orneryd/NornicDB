package antlr

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// SymbolRole is the grammar-owned role used by scope and Fabric name consumers.
type SymbolRole struct {
	Variable, Binding, QueryBinding, Expression, Selector, ProcedureColumn bool
	// A pattern declaration may consume an earlier binding during execution.
	// Admission still treats the same occurrence as a declaration.
	PatternBinding bool
}

func RoleOfSymbol(symbol ISymbolContext, parent antlr4.Tree) (role SymbolRole) {
	switch p := parent.(type) {
	case IAtomContext, IMapProjectionContext:
		if symbolIsFunctionSyntax(symbol) || cyphertext.LiteralKeywordKind(symbol.GetText()) != cyphertext.LiteralKeywordNone {
			return role
		}
		role.Variable, role.Expression = true, true
	case IMapProjectionItemContext:
		role.Variable, role.Expression, role.Selector = true, true, true
	case IFabricCallScopeContext, ICallSubqueryContext, ISetItemContext, IRemoveItemContext, ISchemaCommandContext:
		role.Variable = true
	case INodePatternContext, IRelationDetailContext, IPatternPartContext:
		role.Variable, role.Binding, role.QueryBinding, role.PatternBinding = true, true, true, true
	case IUnwindStContext, IForStContext, IProjectionItemContext, ILetItemContext, IFabricLoadCsvContext:
		role.Variable, role.Binding, role.QueryBinding = true, true, true
	case IForeachStContext, IFilterExpressionContext, IReduceExpressionContext, IAllReduceExpressionContext, IMapComprehensionContext, ILhsContext:
		role.Variable, role.Binding = true, true
	case IYieldItemContext:
		symbols := p.AllSymbol()
		role.Variable = true
		role.Binding = len(symbols) == 1 || symbols[0] != symbol
		role.QueryBinding = role.Binding
		role.ProcedureColumn = len(symbols) == 1 || symbols[0] == symbol
	}
	return role
}

// A bare symbol in a declared syntax slot names the function's mode/type. Walk
// the grammar parents; compound value expressions keep their ordinary roles.
func symbolIsFunctionSyntax(symbol ISymbolContext) bool {
	var current antlr4.Tree = symbol
	for current.GetParent() != nil {
		parent := current.GetParent()
		if chain, ok := parent.(IExpressionChainContext); ok {
			call, ok := chain.GetParent().(IFunctionInvocationContext)
			if !ok || call.InvocationName() == nil {
				return false
			}
			argument, named := FunctionSymbolArgument(call.InvocationName().GetText())
			return named && argument.Position < len(chain.AllExpression()) && chain.Expression(argument.Position) == current
		}
		if parent.GetChildCount() != 1 {
			return false
		}
		current = parent
	}
	return false
}
