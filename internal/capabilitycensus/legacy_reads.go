// Author: Lane #1644 (Codex). Enumerate compiled public errorless entity
// read-shaped methods. This query supplies candidates, never a compatibility
// disposition: optional work polling and mutation/control methods need reading.
package capabilitycensus

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"go/ast"
	"go/types"
	"sort"
)

type ErrorlessReadAPI struct {
	Method      string            `json:"method"`
	Signature   string            `json:"signature"`
	Object      *types.Func       `json:"-"`
	Declaration workcensus.Member `json:"declaration"`
}

func ErrorlessReadAPIs(result Result) []ErrorlessReadAPI {
	seen := map[*types.Func]bool{}
	var found []ErrorlessReadAPI
	add := func(method *types.Func) {
		method = method.Origin()
		if !method.Exported() || seen[method] {
			return
		}
		signature, ok := method.Type().(*types.Signature)
		if !ok || signature.Results().Len() != 1 {
			return
		}
		output := signature.Results().At(0).Type()
		candidate := readPayloadValue(output)
		if primitive, ok := types.Unalias(output).(*types.Basic); ok && primitive.Info()&(types.IsInteger|types.IsString|types.IsBoolean) != 0 && signature.Params().Len() == 1 {
			candidate = candidate || entityValue(signature.Params().At(0).Type())
		}
		if !candidate {
			return
		}
		seen[method] = true
		found = append(found, ErrorlessReadAPI{Method: method.FullName(), Signature: signature.String(), Object: method})
	}
	addType := func(typ types.Type) {
		for _, value := range []types.Type{typ, types.NewPointer(typ)} {
			methods := types.NewMethodSet(value)
			for i := 0; i < methods.Len(); i++ {
				if method, ok := methods.At(i).Obj().(*types.Func); ok {
					add(method)
				}
			}
		}
	}
	for _, object := range result.Types {
		addType(object.Type())
	}
	for _, object := range result.StoredViews {
		addType(object.Type())
	}
	packages := map[*types.Package]bool{}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Package == nil {
			continue
		}
		packages[member.Syntax.Package] = true
		if declaration, ok := member.Syntax.Node.(*ast.FuncDecl); ok && declaration.Recv == nil && member.Syntax.Info != nil {
			if function, ok := member.Syntax.Info.Defs[declaration.Name].(*types.Func); ok {
				add(function)
			}
		}
	}
	for pkg := range packages {
		if pkg.Path() == storagePackage {
			if object := pkg.Scope().Lookup("Engine"); object != nil {
				addType(object.Type())
			}
		}
		if pkg.Path() == "github.com/orneryd/nornicdb/pkg/cypher" {
			if object := pkg.Scope().Lookup("StorageExecutor"); object != nil {
				addType(object.Type())
			}
		}
	}
	// Join the selected typed object to the engine's canonical definition member.
	// No client creates a source identity, digest, parser or loader.
	byObject := map[*types.Func]int{}
	for i, api := range found {
		byObject[api.Object] = i
	}
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		id, ok := member.Syntax.Node.(*ast.Ident)
		if !ok {
			continue
		}
		fn, ok := member.Syntax.Info.Defs[id].(*types.Func)
		if !ok {
			continue
		}
		if index, ok := byObject[fn.Origin()]; ok {
			found[index].Declaration = member
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].Method < found[j].Method })
	return found
}

// ErrorlessReadReference is a direct typed use of a candidate declaration,
// including a bound method value. The containing declaration is engine syntax;
// classification is supplied only by the individually reviewed registry.
type ErrorlessReadReference struct {
	API    string            `json:"api"`
	Owner  string            `json:"owner"`
	Member workcensus.Member `json:"member"`
}

func ErrorlessReadReferences(result Result) []ErrorlessReadReference {
	targets := map[*types.Func]string{}
	for _, api := range result.ErrorlessReadAPIs {
		targets[api.Object] = api.Method
	}
	var uses []ErrorlessReadReference
	for _, member := range result.Inventory.Members {
		if member.Syntax == nil || member.Syntax.Info == nil {
			continue
		}
		id, ok := member.Syntax.Node.(*ast.Ident)
		if !ok {
			continue
		}
		fn, ok := member.Syntax.Info.Uses[id].(*types.Func)
		if !ok {
			continue
		}
		api, ok := targets[fn.Origin()]
		if !ok {
			continue
		}
		owner := ""
		for _, ancestor := range member.Syntax.Ancestors {
			if declaration, ok := ancestor.(*ast.FuncDecl); ok {
				if fn, ok := member.Syntax.Info.Defs[declaration.Name].(*types.Func); ok {
					owner = fn.Origin().FullName()
				}
			}
		}
		uses = append(uses, ErrorlessReadReference{API: api, Owner: owner, Member: member})
	}
	sort.Slice(uses, func(i, j int) bool { return uses[i].Member.ID < uses[j].Member.ID })
	return uses
}
