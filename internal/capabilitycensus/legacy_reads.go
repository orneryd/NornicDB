// Author: Lane #1644 (Codex). Enumerate compiled public errorless entity
// read-shaped methods. This query supplies candidates, never a compatibility
// disposition: optional work polling and mutation/control methods need reading.
package capabilitycensus

import (
    "go/types"
 "go/ast"
    "sort"
)

type ErrorlessReadAPI struct {
    Method string `json:"method"`
    Signature string `json:"signature"`
    Object *types.Func `json:"-"`
}

func ErrorlessReadAPIs(result Result) []ErrorlessReadAPI {
    seen := map[*types.Func]bool{}
    var found []ErrorlessReadAPI
    add := func(method *types.Func) {
        method = method.Origin()
        if !method.Exported() || seen[method] { return }
        signature, ok := method.Type().(*types.Signature)
        if !ok || signature.Results().Len() != 1 { return }
        output := signature.Results().At(0).Type()
        candidate := readPayloadValue(output)
        if primitive, ok := types.Unalias(output).(*types.Basic); ok && primitive.Info() & (types.IsInteger|types.IsString|types.IsBoolean) != 0 && signature.Params().Len()==1 {
            candidate = candidate || entityValue(signature.Params().At(0).Type())
        }
        if !candidate { return }
        seen[method]=true
        found=append(found,ErrorlessReadAPI{method.FullName(),signature.String(),method})
    }
    addType := func(typ types.Type) {
        for _, value := range []types.Type{typ,types.NewPointer(typ)} {
            methods:=types.NewMethodSet(value)
            for i:=0;i<methods.Len();i++ { if method,ok:=methods.At(i).Obj().(*types.Func);ok {add(method)} }
        }
    }
    for _, object := range result.Types { addType(object.Type()) }
    for _, object := range result.StoredViews { addType(object.Type()) }
    packages := map[*types.Package]bool{}
    for _, member := range result.Inventory.Members {
        if member.Syntax==nil || member.Syntax.Package==nil {continue}
        packages[member.Syntax.Package]=true
        if declaration,ok:=member.Syntax.Node.(*ast.FuncDecl);ok && declaration.Recv==nil && member.Syntax.Info!=nil {
            if function,ok:=member.Syntax.Info.Defs[declaration.Name].(*types.Func);ok {add(function)}
        }
    }
    for pkg := range packages {
        if pkg.Path()==storagePackage {
            if object:=pkg.Scope().Lookup("Engine");object!=nil {addType(object.Type())}
        }
        if pkg.Path()=="github.com/orneryd/nornicdb/pkg/cypher" {
            if object:=pkg.Scope().Lookup("StorageExecutor");object!=nil {addType(object.Type())}
        }
    }
    sort.Slice(found,func(i,j int)bool{return found[i].Method<found[j].Method})
    return found
}
