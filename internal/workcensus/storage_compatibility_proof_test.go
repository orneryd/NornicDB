package workcensus

// Author: #1667 implementation lane (Codex). Proof of a source adapter's
// responsibility only. Contextual callers have independent boundary members.

import (
	"go/ast"
	"go/types"
)

func storageCompatibilitySource(m Member, inv Inventory) []string {
	if m.Kind != "context-boundary" || m.Scope != "" || m.Syntax == nil || m.Syntax.Info == nil || len(m.Callees) != 1 || m.Callees[0] != "context.Background" && m.Callees[0] != "context.TODO" {
		return nil
	}
	s := m.Syntax
	owner, ok := s.Owner.(*ast.FuncDecl)
	if !ok || !owner.Name.IsExported() || owner.Body == nil || len(owner.Body.List) != 1 {
		return nil
	}
	object, ok := s.Info.Defs[owner.Name].(*types.Func)
	if !ok {
		return nil
	}
	signature, ok := object.Type().(*types.Signature)
	if !ok {
		return nil
	}
	for i := 0; i < signature.Params().Len(); i++ {
		if typeString(signature.Params().At(i).Type()) == "context.Context" {
			return nil
		}
	}
	ret, ok := owner.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return nil
	}
	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok || len(call.Args) != signature.Params().Len()+1 || call.Args[0] != s.Node {
		return nil
	}
	var target *types.Func
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if signature.Recv() != nil {
			return nil
		}
		target, _ = s.Info.ObjectOf(fun).(*types.Func)
	case *ast.SelectorExpr:
		selection := s.Info.Selections[fun]
		if selection == nil || selection.Kind() != types.MethodVal || signature.Recv() == nil {
			return nil
		}
		receiver, ok := fun.X.(*ast.Ident)
		if !ok || s.Info.ObjectOf(receiver) != signature.Recv() {
			return nil
		}
		target, _ = selection.Obj().(*types.Func)
	default:
		return nil
	}
	if target == nil || target.Pkg() != object.Pkg() || target.Name() != object.Name()+"WithContext" {
		return nil
	}
	targetSignature, ok := target.Type().(*types.Signature)
	if !ok || targetSignature.Params().Len() != signature.Params().Len()+1 || typeString(targetSignature.Params().At(0).Type()) != "context.Context" || !types.Identical(signature.Results(), targetSignature.Results()) || signature.Variadic() != targetSignature.Variadic() {
		return nil
	}
	if (call.Ellipsis.IsValid()) != signature.Variadic() {
		return nil
	}
	for i := 0; i < signature.Params().Len(); i++ {
		argument, ok := call.Args[i+1].(*ast.Ident)
		if !ok || s.Info.ObjectOf(argument) != signature.Params().At(i) || !types.Identical(signature.Params().At(i).Type(), targetSignature.Params().At(i+1).Type()) {
			return nil
		}
	}
	// Shared extraction supplies the exact resolved target and visible body.
	// Missing, mixed or synthetic unresolved targets cannot prove this relay.
	resolved := false
	for _, member := range inv.Members {
		if member.Kind == "owner-call" && member.Syntax != nil && member.Syntax.Node == call {
			if member.UnresolvedTargets || len(member.Callees) != 1 || member.Callees[0] != target.FullName() {
				return nil
			}
			resolved = true
			break
		}
	}
	if !resolved {
		return nil
	}
	for _, d := range inv.Definitions {
		if d.Symbol == target.FullName() {
			return []string{target.FullName(), "callee body " + d.Digest, "same receiver, every typed input, full result/error tuple; no incoming context"}
		}
	}
	return nil
}
