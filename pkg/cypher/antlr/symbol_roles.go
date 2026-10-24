package antlr

import antlr4 "github.com/antlr4-go/antlr/v4"

// SymbolRole is the grammar-owned role used by scope and Fabric name consumers.
type SymbolRole struct {
	Variable, Binding, QueryBinding, Expression, Selector, ProcedureColumn bool
}

func RoleOfSymbol(symbol ISymbolContext, parent antlr4.Tree) (role SymbolRole) {
	switch p := parent.(type) {
	case IAtomContext, IMapProjectionContext:
		role.Variable, role.Expression = true, true
	case IMapProjectionItemContext:
		role.Variable, role.Expression, role.Selector = true, true, true
	case IFabricCallScopeContext, ICallSubqueryContext, ISetItemContext, IRemoveItemContext, ISchemaCommandContext:
		role.Variable = true
	case INodePatternContext, IRelationDetailContext, IPatternPartContext, IUnwindStContext, IForStContext, IProjectionItemContext, ILetItemContext, IFabricLoadCsvContext:
		role.Variable, role.Binding, role.QueryBinding = true, true, true
	case IForeachStContext, IFilterExpressionContext, IReduceExpressionContext, IAllReduceExpressionContext, ILhsContext:
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
