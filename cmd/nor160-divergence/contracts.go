// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"sort"
)

type Contract struct {
	Name            string `json:"name"`
	Owner           Site   `json:"owner"`
	Implementations []Site `json:"implementations"`
	Declarations    []Site `json:"declarations,omitempty"`
}

func (a *analysis) contracts() []Finding {
	type named struct {
		obj *types.TypeName
		typ *types.Named
		pkg string
	}
	typesList := []named{}
	type contract struct {
		name         string
		iface        *types.Interface
		pkg          string
		pos          token.Pos
		declarations []Site
	}
	interfaces := []contract{}
	seenInterfaces := map[*types.Interface]bool{}
	anonymousByShape := map[string]int{}
	for _, p := range a.pkgs {
		objects := []*types.TypeName{}
		for _, definition := range p.TypesInfo.Defs {
			if obj, ok := definition.(*types.TypeName); ok {
				objects = append(objects, obj)
			}
		}
		sort.Slice(objects, func(i, j int) bool {
			x, y := a.prog.Fset.Position(objects[i].Pos()), a.prog.Fset.Position(objects[j].Pos())
			if x.Filename != y.Filename {
				return x.Filename < y.Filename
			}
			return x.Offset < y.Offset
		})
		for _, obj := range objects {
			_, ok := obj.Type().(*types.Named)
			if !ok {
				continue
			}
			t, ok := types.Unalias(obj.Type()).(*types.Named)
			if !ok || obj.IsAlias() {
				continue
			}
			n := named{obj, t, p.PkgPath}
			if iface, ok := t.Underlying().(*types.Interface); ok {
				interfaces = append(interfaces, contract{name: typeKey(t), iface: iface, pkg: p.PkgPath, pos: obj.Pos()})
				seenInterfaces[iface] = true
			} else {
				typesList = append(typesList, n)
			}
		}
		// Include anonymous interface literals (assertion capabilities, fields, parameters)
		// and local named declarations as well as package-scope contracts.
		anonymous := []ast.Expr{}
		for expr := range p.TypesInfo.Types {
			if _, ok := expr.(*ast.InterfaceType); ok {
				anonymous = append(anonymous, expr)
			}
		}
		sort.Slice(anonymous, func(i, j int) bool {
			x, y := a.prog.Fset.Position(anonymous[i].Pos()), a.prog.Fset.Position(anonymous[j].Pos())
			if x.Filename != y.Filename {
				return x.Filename < y.Filename
			}
			return x.Offset < y.Offset
		})
		for _, expr := range anonymous {
			tv := p.TypesInfo.Types[expr]
			if _, ok := expr.(*ast.InterfaceType); !ok {
				continue
			}
			iface, ok := tv.Type.Underlying().(*types.Interface)
			if !ok || seenInterfaces[iface] {
				continue
			}
			seenInterfaces[iface] = true
			site := a.site(p.PkgPath, "", expr.Pos(), "")
			site.Role = "interface_declaration"
			shape := typeKey(iface)
			site.Function = shape
			if index, ok := anonymousByShape[shape]; ok {
				interfaces[index].declarations = append(interfaces[index].declarations, site)
				continue
			}
			anonymousByShape[shape] = len(interfaces)
			interfaces = append(interfaces, contract{name: "anonymous:" + shape, iface: iface, pkg: p.PkgPath, pos: expr.Pos(), declarations: []Site{site}})
		}
	}
	out := []Finding{}
	for _, iface := range interfaces {
		i := iface.iface.Complete()
		if !i.IsMethodSet() {
			continue
		}
		name := iface.name
		inventory := Contract{Name: name, Owner: a.site(iface.pkg, name, iface.pos, "interface_owner"), Implementations: []Site{}, Declarations: iface.declarations}
		for _, concrete := range typesList {
			for _, t := range []types.Type{concrete.typ, types.NewPointer(concrete.typ)} {
				if types.Implements(t, i) {
					inventory.Implementations = append(inventory.Implementations, a.site(concrete.pkg, typeKey(t), concrete.obj.Pos(), "implementation"))
				}
			}
		}
		sort.Slice(inventory.Implementations, func(i, j int) bool {
			return siteKey(inventory.Implementations[i]) < siteKey(inventory.Implementations[j])
		})
		a.contractInventory = append(a.contractInventory, inventory)
		// T and *T are one implementation owner for the divergence threshold.
		owners := map[token.Pos]bool{}
		for _, concrete := range typesList {
			if types.Implements(concrete.typ, i) || types.Implements(types.NewPointer(concrete.typ), i) {
				owners[concrete.obj.Pos()] = true
			}
		}
		if len(owners) >= 2 {
			sites := append([]Site{inventory.Owner}, inventory.Implementations...)
			sites = append(sites, inventory.Declarations...)
			out = append(out, Finding{Kind: "multiple_implementations", Subject: name, Why: "Two or more named implementation owners satisfy the same compiler-checked interface; inspect whether they independently implement one policy.", Sites: sites, Evidence: map[string]any{"named_owners": len(owners), "interface_methods": i.NumMethods()}})
		}
	}
	for _, fn := range a.functions {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			x, ok := n.(*ast.TypeAssertExpr)
			if !ok || x.Type == nil {
				return true
			}
			asserted := fn.Pkg.TypesInfo.TypeOf(x.Type)
			source := fn.Pkg.TypesInfo.TypeOf(x.X)
			if asserted == nil || source == nil {
				return true
			}
			base, ok := source.Underlying().(*types.Interface)
			if !ok {
				return true
			}
			base.Complete()
			contract := typeKey(source)
			owner := fn.Site.Package
			if named, ok := types.Unalias(source).(*types.Named); ok && named.Obj().Pkg() != nil {
				owner = named.Obj().Pkg().Path()
			}
			if owner == fn.Site.Package || a.owners.owns("contracts", fn.Site.Package, fn.Site.Function, contract) {
				return true
			}
			// Method selections on assertion results are capability discovery. Method sets
			// also expose promoted capabilities even if a branch does not call them locally.
			methods := types.NewMethodSet(asserted)
			extra := []string{}
			for j := 0; j < methods.Len(); j++ {
				m := methods.At(j).Obj()
				found := false
				for k := 0; k < base.NumMethods(); k++ {
					if base.Method(k).Id() == m.Id() {
						found = true
						break
					}
				}
				if !found {
					extra = append(extra, m.Name())
				}
			}
			if len(extra) == 0 {
				return true
			}
			sort.Strings(extra)
			out = append(out, Finding{Kind: "outside_capability_assertion", Subject: contract + " -> " + typeKey(asserted), Why: "An assertion or type-switch case outside the interface owner discovers methods absent from the declared contract, including promoted methods.", Sites: []Site{a.site(fn.Site.Package, fn.Site.Function, x.Pos(), "capability_assertion")}, Evidence: map[string]any{"interface_owner_package": owner, "extra_methods": extra}})
			return true
		})
		// In a type switch x.(type) has no asserted type; every case is typed separately.
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			sw, ok := n.(*ast.TypeSwitchStmt)
			if !ok {
				return true
			}
			var expr ast.Expr
			switch x := sw.Assign.(type) {
			case *ast.AssignStmt:
				expr = x.Rhs[0]
			case *ast.ExprStmt:
				expr = x.X
			}
			ta, ok := expr.(*ast.TypeAssertExpr)
			if !ok {
				return true
			}
			source := fn.Pkg.TypesInfo.TypeOf(ta.X)
			if source == nil {
				return true
			}
			base, ok := source.Underlying().(*types.Interface)
			if !ok {
				return true
			}
			owner := fn.Site.Package
			if t, ok := types.Unalias(source).(*types.Named); ok && t.Obj().Pkg() != nil {
				owner = t.Obj().Pkg().Path()
			}
			if owner == fn.Site.Package || a.owners.owns("contracts", fn.Site.Package, fn.Site.Function, typeKey(source)) {
				return true
			}
			for _, stmt := range sw.Body.List {
				cl := stmt.(*ast.CaseClause)
				for _, expr := range cl.List {
					t := fn.Pkg.TypesInfo.TypeOf(expr)
					if t == nil {
						continue
					}
					extra := []string{}
					ms := types.NewMethodSet(t)
					for j := 0; j < ms.Len(); j++ {
						m := ms.At(j).Obj()
						found := false
						for k := 0; k < base.NumMethods(); k++ {
							if base.Method(k).Id() == m.Id() {
								found = true
							}
						}
						if !found {
							extra = append(extra, m.Name())
						}
					}
					if len(extra) > 0 {
						out = append(out, Finding{Kind: "outside_capability_switch", Subject: typeKey(source) + " -> " + typeKey(t), Why: "A type-switch outside the interface owner selects capabilities absent from its interface.", Sites: []Site{a.site(fn.Site.Package, fn.Site.Function, expr.Pos(), "capability_switch")}, Evidence: map[string]any{"extra_methods": extra, "interface_owner_package": owner}})
					}
				}
			}
			return true
		})
	}
	sort.Slice(a.contractInventory, func(i, j int) bool { return a.contractInventory[i].Name < a.contractInventory[j].Name })
	return out
}
