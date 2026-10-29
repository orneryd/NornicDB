package capabilitycensus

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

// EngineDeletionViews includes promoted methods: a declaration-only census
// cannot see a view whose point operation is owned but whose batch operation
// comes from an embedded Engine. Contracts are compiler method-set facts;
// their semantic dispositions still require an individual view/body review.
// Author: Lane #1644 (Codex).
func EngineDeletionViews(result Result) []Site {
	declarations := map[*types.TypeName]workcensus.Member{}
	var core *types.Interface
	for _, m := range result.Inventory.Members {
		if m.Syntax == nil || m.Syntax.Info == nil {
			continue
		}
		if core == nil && m.Syntax.Package != nil && m.Syntax.Package.Path() == storagePackage {
			if anchor := m.Syntax.Package.Scope().Lookup("Engine"); anchor != nil {
				core, _ = anchor.Type().Underlying().(*types.Interface)
			}
		}
		if spec, ok := m.Syntax.Node.(*ast.TypeSpec); ok {
			if obj, ok := m.Syntax.Info.Defs[spec.Name].(*types.TypeName); ok {
				declarations[obj] = m
			}
		}
	}
	var out []Site
	for _, obj := range result.Types {
		m, ok := declarations[obj]
		if !ok {
			continue
		}
		if core == nil {
			continue
		}
		typ := types.Unalias(obj.Type())
		if _, pointer := typ.(*types.Pointer); !pointer {
			typ = types.NewPointer(typ)
		}
		set := types.NewMethodSet(typ)
		var targets []string
		errorType := types.Universe.Lookup("error").Type()
		for i := 0; i < core.NumMethods(); i++ {
			anchor := core.Method(i)
			signature := anchor.Type().(*types.Signature)
			if signature.Results().Len() != 1 || !types.Identical(signature.Results().At(0).Type(), errorType) {
				continue
			}
			selected := false
			for j := 0; j < signature.Params().Len(); j++ {
				parameter := types.Unalias(signature.Params().At(j).Type())
				if slice, ok := parameter.(*types.Slice); ok {
					parameter = types.Unalias(slice.Elem())
				}
				if id, ok := parameter.(*types.Named); ok && id.Obj().Pkg() != nil && id.Obj().Pkg().Path() == storagePackage {
					for _, name := range []string{"NodeID", "EdgeID"} {
						if actual := id.Obj().Pkg().Scope().Lookup(name); actual != nil && types.Identical(parameter, actual.Type()) {
							selected = true
						}
					}
				}
			}
			if !selected {
				continue
			}
			selection := set.Lookup(anchor.Pkg(), anchor.Name())
			if selection == nil {
				continue
			}
			method, ok := selection.Obj().(*types.Func)
			if !ok {
				continue
			}
			targets = append(targets, fmt.Sprintf("%s -> %s %s via %v", anchor.FullName(), method.Origin().FullName(), types.TypeString(selection.Type(), func(pkg *types.Package) string { return pkg.Path() }), selection.Index()))
		}
		if len(targets) == 0 {
			continue
		}
		sort.Strings(targets)
		out = append(out, Site{Population: "engine-deletion-views", Key: m.ID, Path: memberPath(m), Function: obj.Pkg().Path() + "." + obj.Name(), Line: m.Line, Kind: m.Kind, Digest: m.Digest, Contract: strings.Join(targets, "; "), Proof: "bulk-source-authority", Detail: m.Code, Evidence: SiteEvidence{OwnerDigest: m.Digest, State: m.State, Callees: targets}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
