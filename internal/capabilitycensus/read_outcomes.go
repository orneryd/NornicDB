package capabilitycensus

import (
	"go/ast"
	"go/types"
 "go/token"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// Author: Lane #1644 (Codex). This is a direct compiled-use query, not a
// transitive public call graph. Entity/error results assigned in an errorless
// adapter, or with the error explicitly discarded, require an individual
// recovery/terminal disposition. The query grants no semantic approval.
func errorlessEntityReadUses(inventory workcensus.Inventory, core *types.Interface) []workcensus.Member {
	errorType := types.Universe.Lookup("error").Type()
	owners := map[string]*types.Signature{}
	calls := map[*ast.CallExpr]workcensus.Member{}
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		switch node := member.Syntax.Node.(type) {
		case *ast.FuncDecl:
			if fn, ok := member.Syntax.Info.Defs[node.Name].(*types.Func); ok {
				owners[member.Owner], _ = fn.Type().(*types.Signature)
			}
		case *ast.FuncLit:
			owners[member.Owner], _ = member.Syntax.Info.TypeOf(node).(*types.Signature)
		case *ast.CallExpr:
			calls[node] = member
		}
	}
	var result []workcensus.Member
	for _, member := range inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		var values, targets []ast.Expr
		switch node := member.Syntax.Node.(type) {
		case *ast.AssignStmt:
			values, targets = node.Rhs, node.Lhs
		case *ast.ValueSpec:
			values = node.Values
			for _, identifier := range node.Names {
				targets = append(targets, identifier)
			}
		}
		if len(values) != 1 || len(targets) != 2 {
			continue
		}
		call, ok := values[0].(*ast.CallExpr)
		if !ok {
			continue
		}
		callType := member.Syntax.Info.TypeOf(call.Fun)
		if callType == nil {
			continue
		}
		signature, ok := types.Unalias(callType).Underlying().(*types.Signature)
		if !ok || signature.Results().Len() != 2 || !types.Identical(signature.Results().At(1).Type(), errorType) {
			continue
		}

        readValue := readPayloadValue(signature.Results().At(0).Type())
        if scalar, ok := signature.Results().At(0).Type().Underlying().(*types.Basic); ok && scalar.Info() & types.IsInteger != 0 {
            if function, ok := object(member.Syntax.Info, call.Fun).(*types.Func); ok {
                declared, _ := function.Type().(*types.Signature)
                if declared != nil && declared.Recv() != nil {
                    _, contract := declared.Recv().Type().Underlying().(*types.Interface)
                    readValue = readValue || engineType(declared.Recv().Type(), core) || contract && function.Pkg()!=nil && function.Pkg().Path()==storagePackage
                }
            }
        }
        if !readValue { continue }

		discarded := false
		if identifier, ok := targets[1].(*ast.Ident); ok {
			discarded = identifier.Name == "_"
		}
		owner := owners[member.Owner]
		errorChannel := false
		if owner != nil {
			for i := 0; i < owner.Results().Len(); i++ {
				errorChannel = errorChannel || types.Identical(owner.Results().At(i).Type(), errorType)
			}
		}
		if discarded || !errorChannel || skipsReadError(member, targets[1]) {
			// A missing owner signature remains a review candidate, never an
			// inferred recovery. Canonical typed source supplies the call ID.
			if selected, ok := calls[call]; ok {
				result = append(result, selected)
			}
		}
	}
	return result
}

// readPayloadValue identifies actual payload types by their compiled storage
// declarations. Function-valued binders and stored policy metadata are distinct
// protocol values, even if their signatures mention an entity.
func readPayloadValue(value types.Type) bool {
    value=types.Unalias(value)
    switch value:=value.(type) {
    case *types.Pointer: return readPayloadValue(value.Elem())
    case *types.Slice: return readPayloadValue(value.Elem())
    case *types.Array: return readPayloadValue(value.Elem())
    case *types.Map: return readPayloadValue(value.Elem())
    case *types.Named:
        pkg:=value.Obj().Pkg()
        if pkg!=nil && pkg.Path()==storagePackage {
            for _,name:=range []string{"Node","Edge","StoredReadResult","StoredNodeFact","StoredEdgeFact"} {
                if entity:=pkg.Scope().Lookup(name);entity!=nil && types.Identical(value,entity.Type()) {return true}
            }
        }
    }
    return false
}

// skipsReadError selects direct catch-and-continue handlers in an adapter that
// otherwise has an error channel. The common census provides typed syntax and
// ancestry; this domain query examines the error variable of this exact read.
// It deliberately emits candidates, including any explicit recovery body.
func skipsReadError(member workcensus.Member, target ast.Expr) bool {
    identifier,ok:=target.(*ast.Ident)
    if !ok {return false}
    errorObject:=member.Syntax.Info.ObjectOf(identifier)
    if errorObject==nil {return false}
    usesError:=func(value ast.Expr) bool {
        id,ok:=value.(*ast.Ident)
        return ok && member.Syntax.Info.ObjectOf(id)==errorObject
    }
    isNil:=func(value ast.Expr) bool {
        id,ok:=value.(*ast.Ident)
        return ok && member.Syntax.Info.ObjectOf(id)==types.Universe.Lookup("nil")
    }
    var anyError func(ast.Expr) bool
    anyError=func(expression ast.Expr)bool{
        if paren,ok:=expression.(*ast.ParenExpr);ok{return anyError(paren.X)}
        binary,ok:=expression.(*ast.BinaryExpr)
        if !ok {return false}
        if binary.Op==token.LOR {return anyError(binary.X)||anyError(binary.Y)}
        return binary.Op==token.NEQ && (usesError(binary.X)&&isNil(binary.Y)||usesError(binary.Y)&&isNil(binary.X))
    }
    check:=func(branch *ast.IfStmt)bool{
        if !anyError(branch.Cond){return false}
        for _,statement:=range branch.Body.List{
            if transfer,ok:=statement.(*ast.BranchStmt);ok && transfer.Tok==token.CONTINUE{return true}
        }
        return false
    }
    for i:=len(member.Syntax.Ancestors)-1;i>=0;i--{
        switch parent:=member.Syntax.Ancestors[i].(type){
        case *ast.IfStmt:
            if parent.Init==member.Syntax.Node && check(parent){return true}
        case *ast.BlockStmt:
            for index,statement:=range parent.List{
                if statement.Pos()<=member.Syntax.Node.Pos() && member.Syntax.Node.End()<=statement.End() && index+1<len(parent.List){
                    if branch,ok:=parent.List[index+1].(*ast.IfStmt);ok && check(branch){return true}
                }
            }
            return false
        }
    }
    return false
}
