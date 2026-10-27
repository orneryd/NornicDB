package workcensus

import (
	"go/ast"
	"go/token"
	"go/types"
	"sort"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/packages"
)

type aliasSource struct {
	expr ast.Expr
	info *types.Info
}
type callAliases struct {
	writes map[types.Object][]aliasSource
	sites  map[token.Pos][]types.Object
}

// Resolve source assignments independently of variable spelling. Container and
// field assignments conservatively union targets; VTA adds function-return and
// interface targets. Unknown values remain an explicit approximation boundary.
func aliases(pkgs []*packages.Package, graph *callgraph.Graph) *callAliases {
	a := &callAliases{writes: map[types.Object][]aliasSource{}, sites: map[token.Pos][]types.Object{}}
	var lhs func(ast.Expr, *types.Info) types.Object
	lhs = func(e ast.Expr, info *types.Info) types.Object {
		switch e := e.(type) {
		case *ast.IndexExpr:
			return lhs(e.X, info)
		case *ast.StarExpr:
			return lhs(e.X, info)
		default:
			return callObject(e, info)
		}
	}
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.ValueSpec:
					if len(n.Names) == len(n.Values) {
						for i, name := range n.Names {
							obj := p.TypesInfo.ObjectOf(name)
							a.writes[obj] = append(a.writes[obj], aliasSource{n.Values[i], p.TypesInfo})
						}
					}
				case *ast.AssignStmt:
					if len(n.Lhs) == len(n.Rhs) {
						for i, left := range n.Lhs {
							obj := lhs(left, p.TypesInfo)
							if obj != nil {
								a.writes[obj] = append(a.writes[obj], aliasSource{n.Rhs[i], p.TypesInfo})
							}
						}
					}
				}
				return true
			})
		}
	}
	if graph == nil {
		return a
	}
	for _, node := range graph.Nodes {
		for _, edge := range node.Out {
			if edge.Site == nil {
				continue
			}
			fn := sourceFunction(edge.Callee.Func)
			if obj := fn.Object(); obj != nil {
				a.sites[edge.Site.Pos()] = append(a.sites[edge.Site.Pos()], obj)
			}
		}
	}
	return a
}

// Without a graph, direct function objects and builtins are exact targets.
// Function values retain their source-assignment aliases but may have unknown
// writers/returns; interfaces also have concrete implementations not enumerated
// by syntax. Keep these calls as candidates instead of silently excluding them.
func (a *callAliases) targetFacts(call *ast.CallExpr, info *types.Info) ([]types.Object, bool) {
	targets := a.targets(call, info)
	if info == nil {
		return targets, true
	}
	if info.Types[call.Fun].IsType() {
		return targets, false
	}
	object := callObject(call.Fun, info)
	switch object := object.(type) {
	case *types.Builtin:
		return targets, false
	case *types.Func:
		if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
			if _, iface := signature.Recv().Type().Underlying().(*types.Interface); iface {
				return targets, true
			}
		}
		return targets, false
	}
	return targets, true
}

func (a *callAliases) targets(call *ast.CallExpr, info *types.Info) []types.Object {
	if info == nil {
		return nil
	}
	seen := map[types.Object]bool{}
	found := map[types.Object]bool{}
	var object func(types.Object)
	var expr func(ast.Expr, *types.Info)
	object = func(obj types.Object) {
		if obj == nil || seen[obj] {
			return
		}
		seen[obj] = true
		switch obj.(type) {
		case *types.Func, *types.Builtin:
			found[obj] = true
		default:
			for _, source := range a.writes[obj] {
				expr(source.expr, source.info)
			}
		}
	}
	expr = func(e ast.Expr, info *types.Info) {
		switch e := e.(type) {
		case *ast.CompositeLit:
			for _, entry := range e.Elts {
				if pair, ok := entry.(*ast.KeyValueExpr); ok {
					expr(pair.Value, info)
				} else {
					expr(entry, info)
				}
			}
		case *ast.UnaryExpr:
			expr(e.X, info)
		case *ast.StarExpr:
			expr(e.X, info)
		case *ast.SliceExpr:
			expr(e.X, info)
		case *ast.ParenExpr:
			expr(e.X, info)
		default:
			object(callObject(e, info))
		}
	}
	expr(call.Fun, info)
	for _, pos := range []token.Pos{call.Pos(), call.Lparen, call.Fun.Pos()} {
		for _, target := range a.sites[pos] {
			object(target)
		}
	}
	var out []types.Object
	for target := range found {
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
