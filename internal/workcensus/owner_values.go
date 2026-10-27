package workcensus

import (
	"fmt"
	"go/ast"
	"go/types"
)

// ValueDefinition is an engine-owned binding in one source owner. Its identity
// uses named parameter/receiver/result roles and physical local declarations.
// Unrelated binding removals cannot reassign the retained scope identity.
type ValueDefinition struct {
	Reference string `json:"reference"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Role      string `json:"role"`
}

func ownerValues(base sourceSite, n ast.Node) (map[types.Object]string, []ValueDefinition) {
	bindings := map[types.Object]string{}
	for obj, ref := range base.bindings {
		bindings[obj] = ref
	}
	var values []ValueDefinition
	if base.info == nil {
		return bindings, values
	}
	add := func(fields *ast.FieldList, role string) {
		if fields == nil {
			return
		}
		for _, field := range fields.List {
			if len(field.Names) == 0 {
				continue
			}
			for _, name := range field.Names {
				obj, ok := base.info.Defs[name].(*types.Var)
				if !ok || name.Name == "_" {
					continue
				}
				ref := fmt.Sprintf("%s/%s:%s", base.ownerID, role, name.Name)
				bindings[obj] = ref
				values = append(values, ValueDefinition{Reference: ref, Name: name.Name, Type: typeString(obj.Type()), Role: role})
			}
		}
	}
	switch n := n.(type) {
	case *ast.FuncDecl:
		add(n.Recv, "receiver")
		add(n.Type.Params, "parameter")
		add(n.Type.Results, "result")
	case *ast.FuncLit:
		add(n.Type.Params, "parameter")
		add(n.Type.Results, "result")
	}
	return bindings, values
}

// valueReference intentionally recognizes direct bindings and field paths.
// Computed expressions and alias provenance need separate client domain facts;
// matching names/types alone must never impersonate the required scope value.
func valueReference(expr ast.Expr, site sourceSite) string {
	if site.info == nil {
		return ""
	}
	switch n := expr.(type) {
	case *ast.ParenExpr:
		return valueReference(n.X, site)
	case *ast.Ident:
		return site.bindings[site.info.ObjectOf(n)]
	case *ast.SelectorExpr:
		selection := site.info.Selections[n]
		if selection == nil || selection.Kind() != types.FieldVal {
			return ""
		}
		if base := valueReference(n.X, site); base != "" {
			return base + "." + selection.Obj().Name()
		}
	}
	return ""
}
