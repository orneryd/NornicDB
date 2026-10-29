package storage

import (
	"go/token"
	"go/types"

	"github.com/orneryd/nornicdb/internal/ssawalk"
	"golang.org/x/tools/go/ssa"
)

// bindingSSAReachability is a conservative, context-insensitive prefilter for
// the more precise alias traversal. An edge means that the destination may
// depend on the source. A missing path to the checked field proves that the
// precise traversal cannot find it; a path does not itself prove an alias.
type bindingSSAReachability struct {
	functions map[*ssa.Function]bool
	graph     *ssawalk.Graph
	roots     map[*types.Var][]ssa.Value
	unknown   []ssa.Value
	live      *ssawalk.Walk
}

func newBindingSSAReachability(a *bindingSSAAnalysis, functions []*ssa.Function) *bindingSSAReachability {
	g := &bindingSSAReachability{
		functions: make(map[*ssa.Function]bool, len(functions)),
		graph:     ssawalk.NewGraph(),
		roots:     make(map[*types.Var][]ssa.Value),
	}
	for _, fn := range functions {
		g.functions[fn] = true
	}
	// The prefilter only follows borrowed byte references: an edge from a
	// value of any other type is never on a path to the checked map.
	edge := func(src, dst ssa.Value) bool { return bindingSSAReference(src.Type()) }
	add := func(dst, src ssa.Value) { g.graph.Add(src, dst, edge) }
	field := func(typ types.Type, index int) *types.Var {
		if p, ok := typ.(*types.Pointer); ok {
			typ = p.Elem()
		}
		if s, ok := typ.Underlying().(*types.Struct); ok && index >= 0 && index < s.NumFields() {
			return s.Field(index)
		}
		return nil
	}
	returns := func(dst ssa.Value, call *ssa.Call, index int) {
		callee := call.Common().StaticCallee()
		if callee == nil || len(callee.Blocks) == 0 {
			return
		}
		if !g.functions[callee] {
			// Alias can enter a synthetic or instantiated body not in the
			// original population. Never prune through an unindexed body.
			g.unknown = append(g.unknown, dst)
			return
		}
		for _, block := range callee.Blocks {
			if len(block.Instrs) == 0 {
				continue
			}
			if ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); ok && index < len(ret.Results) {
				add(dst, ret.Results[index])
			}
		}
	}
	var inspect func(ssa.Value)
	inspect = func(value ssa.Value) {
		if value == nil || !bindingSSAReference(value.Type()) {
			return
		}
		g.graph.AddValue(value)
		switch v := value.(type) {
		case *ssa.Alloc:
			for _, store := range a.stores[v.Parent()] {
				if store.Addr == v {
					add(v, store.Val)
				}
			}
		case *ssa.FieldAddr:
			if f := field(v.X.Type(), v.Field); f != nil {
				g.roots[f] = append(g.roots[f], v)
			}
			add(v, v.X)
		case *ssa.Field:
			if f := field(v.X.Type(), v.Field); f != nil {
				g.roots[f] = append(g.roots[f], v)
			}
		case *ssa.Parameter:
			for _, call := range a.calls[v.Parent()] {
				for i, parameter := range v.Parent().Params {
					if parameter == v && i < len(call.Common().Args) {
						add(v, call.Common().Args[i])
					}
				}
			}
		case *ssa.FreeVar:
			for _, closure := range a.closures[v.Parent()] {
				for i, parameter := range v.Parent().FreeVars {
					if parameter == v {
						add(v, closure.Bindings[i])
					}
				}
			}
		case *ssa.UnOp:
			if v.Op != token.MUL {
				break
			}
			add(v, v.X)
			for _, store := range a.stores[v.Parent()] {
				if bindingSSASameAddress(store.Addr, v.X) {
					add(v, store.Val)
				}
			}
		case *ssa.Phi:
			for _, edge := range v.Edges {
				add(v, edge)
			}
		case *ssa.Lookup:
			add(v, v.X)
			for _, block := range v.Parent().Blocks {
				for _, instruction := range block.Instrs {
					if update, ok := instruction.(*ssa.MapUpdate); ok && update.Map == v.X {
						add(v, update.Value)
					}
				}
			}
		case *ssa.IndexAddr:
			add(v, v.X)
		case *ssa.Index:
			add(v, v.X)
		case *ssa.Slice:
			add(v, v.X)
		case *ssa.ChangeType:
			add(v, v.X)
		case *ssa.Convert:
			add(v, v.X)
		case *ssa.MakeInterface:
			add(v, v.X)
		case *ssa.ChangeInterface:
			add(v, v.X)
		case *ssa.TypeAssert:
			add(v, v.X)
		case *ssa.Extract:
			switch tuple := v.Tuple.(type) {
			case *ssa.Call:
				returns(v, tuple, v.Index)
			case *ssa.Lookup:
				if v.Index == 0 {
					add(v, tuple.X)
				}
			case *ssa.TypeAssert:
				if v.Index == 0 {
					add(v, tuple.X)
				}
			case *ssa.Next:
				if iter, ok := tuple.Iter.(*ssa.Range); ok && v.Index == 2 {
					add(v, iter.X)
				}
			}
		case *ssa.Call:
			returns(v, v, 0)
		}
	}
	for _, value := range ssawalk.Values(functions) {
		inspect(value)
	}
	return g
}

// setRoot marks the values whose alias may depend on root: the field's
// addresses, the results that entered no indexed body, and everything that
// depends on them. Root values receive an unbounded walk, so the prefilter
// never prunes a path from the checked field itself.
func (g *bindingSSAReachability) setRoot(root *types.Var) {
	g.live = g.graph.From(append(append([]ssa.Value(nil), g.roots[root]...), g.unknown...), -1)
}

func (g *bindingSSAReachability) mayReach(value ssa.Value) bool {
	if value.Parent() != nil && !g.functions[value.Parent()] {
		return true
	}
	if !g.graph.Known(value) {
		return true
	}
	return g.live.Reached(value)
}
