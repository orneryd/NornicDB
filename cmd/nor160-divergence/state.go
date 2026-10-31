// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"go/token"
	"go/types"
	"sort"

	"golang.org/x/tools/go/ssa"
)

func fieldID(t types.Type, index int) string {
	t = types.Unalias(t)
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	s, ok := t.Underlying().(*types.Struct)
	if !ok || index >= s.NumFields() {
		return ""
	}
	return typeKey(t) + "." + s.Field(index).Name()
}

func addresses(v ssa.Value, seen map[ssa.Value]bool) []string {
	if v == nil || seen[v] {
		return nil
	}
	seen[v] = true
	switch x := v.(type) {
	case *ssa.Global:
		return []string{x.Pkg.Pkg.Path() + "." + x.Name()}
	case *ssa.FieldAddr:
		return []string{fieldID(x.X.Type(), x.Field)}
	case *ssa.ChangeType:
		return addresses(x.X, seen)
	case *ssa.Convert:
		return addresses(x.X, seen)
	case *ssa.Phi:
		var out []string
		for _, e := range x.Edges {
			out = append(out, addresses(e, seen)...)
		}
		return out
	}
	return nil
}

func storeTargets(addr ssa.Value) []string {
	out := addresses(addr, map[ssa.Value]bool{})
	// A whole-struct Store writes every embedded value field, not just FieldAddr sites.
	if p, ok := types.Unalias(addr.Type()).Underlying().(*types.Pointer); ok {
		var fields func(types.Type)
		fields = func(t types.Type) {
			if s, ok := t.Underlying().(*types.Struct); ok {
				for i := 0; i < s.NumFields(); i++ {
					out = append(out, fieldID(t, i))
					fields(s.Field(i).Type())
				}
			}
		}
		fields(p.Elem())
	}
	return out
}

func receiverDerived(v, receiver ssa.Value, seen map[ssa.Value]bool) bool {
	if v == receiver {
		return true
	}
	if v == nil || seen[v] {
		return false
	}
	seen[v] = true
	switch x := v.(type) {
	case *ssa.FieldAddr:
		return receiverDerived(x.X, receiver, seen)
	case *ssa.Field:
		return receiverDerived(x.X, receiver, seen)
	case *ssa.UnOp:
		return receiverDerived(x.X, receiver, seen)
	case *ssa.ChangeType:
		return receiverDerived(x.X, receiver, seen)
	case *ssa.Phi:
		for _, e := range x.Edges {
			if receiverDerived(e, receiver, seen) {
				return true
			}
		}
	}
	return false
}

func (a *analysis) reachable(from, to *ssa.Function) bool {
	index, ok := a.graphIndex[to]
	if !ok {
		return false
	}
	if bits := a.reachCache[from]; bits != nil {
		return bits[index/64]&(1<<uint(index%64)) != 0
	}
	bits := make([]uint64, (len(a.graphIndex)+63)/64)
	queue := []*ssa.Function{from}
	if i, ok := a.graphIndex[from]; ok {
		bits[i/64] |= 1 << uint(i%64)
	}
	for len(queue) > 0 {
		f := queue[0]
		queue = queue[1:]
		n := a.graph.Nodes[f]
		if n == nil {
			continue
		}
		for _, e := range n.Out {
			g := e.Callee.Func
			i, ok := a.graphIndex[g]
			if ok && bits[i/64]&(1<<uint(i%64)) == 0 {
				bits[i/64] |= 1 << uint(i%64)
				queue = append(queue, g)
			}
		}
	}
	a.reachCache[from] = bits
	return bits[index/64]&(1<<uint(index%64)) != 0
}

func (a *analysis) stateFacts(wantWriters bool) []Finding {
	type writer struct {
		f     *ssa.Function
		sites []Site
	}
	writers := map[string]map[*ssa.Function]*writer{}
	for _, p := range a.pkgs {
		sp := a.prog.Package(p.Types)
		if sp == nil {
			continue
		}
		functions := []*ssa.Function{}
		for _, fn := range a.functions {
			if fn.Pkg == p {
				functions = append(functions, fn.SSA)
			}
		}
		if init := sp.Func("init"); init != nil {
			functions = append(functions, init)
		}
		for _, f := range functions {
			fn := a.canonical(f)
			for _, b := range f.Blocks {
				for _, ins := range b.Instrs {
					if fn != nil && f.Signature.Recv() != nil && len(f.Params) > 0 {
						switch x := ins.(type) {
						case *ssa.Field:
							if receiverDerived(x.X, f.Params[0], map[ssa.Value]bool{}) {
								fn.Reads[fieldID(x.X.Type(), x.Field)] = true
							}
						case *ssa.UnOp:
							if x.Op == token.MUL && receiverDerived(x.X, f.Params[0], map[ssa.Value]bool{}) {
								for _, key := range addresses(x.X, map[ssa.Value]bool{}) {
									fn.Reads[key] = true
								}
							}
						}
					}
					x, ok := ins.(*ssa.Store)
					if !ok || !wantWriters {
						continue
					}
					for _, key := range storeTargets(x.Addr) {
						if key == "" {
							continue
						}
						if writers[key] == nil {
							writers[key] = map[*ssa.Function]*writer{}
						}
						w := writers[key][f]
						if w == nil {
							w = &writer{f: f}
							writers[key][f] = w
						}
						pos := x.Pos()
						if pos == token.NoPos {
							pos = x.Addr.Pos()
						}
						w.sites = append(w.sites, a.site(p.PkgPath, f.String(), pos, "write"))
					}
				}
			}
		}
	}
	out := []Finding{}
	for key, group := range writers {
		if len(group) < 2 {
			continue
		}
		ws := []*writer{}
		for _, w := range group {
			ws = append(ws, w)
		}
		sort.Slice(ws, func(i, j int) bool { return ws[i].f.String() < ws[j].f.String() })
		independent := map[int]bool{}
		pairs := [][2]string{}
		for i := range ws {
			for j := i + 1; j < len(ws); j++ {
				if !a.reachable(ws[i].f, ws[j].f) && !a.reachable(ws[j].f, ws[i].f) {
					independent[i] = true
					independent[j] = true
					pairs = append(pairs, [2]string{ws[i].f.String(), ws[j].f.String()})
				}
			}
		}
		if len(pairs) == 0 {
			continue
		}
		sites := []Site{}
		for i, w := range ws {
			if independent[i] {
				sites = append(sites, w.sites...)
			}
		}
		out = append(out, Finding{Kind: "independent_writers", Subject: key, Why: "The same typed state location has writers not connected by a caller/helper path; inspect its ownership and lifecycle.", Sites: sites, Evidence: map[string]any{"independent_owner_pairs": pairs, "all_writer_functions": len(group)}})
	}
	return out
}
