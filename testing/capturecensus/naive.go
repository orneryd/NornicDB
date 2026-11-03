package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"go/token"
	"go/types"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// naiveTaint is the census's reconciliation oracle: a deliberately simple
// def-use taint that knows no nil tests, no conventions and no path
// conditions. It shares only the loaded program and call graph with the nil
// analysis. It is one forward fixpoint over a value graph built once:
// a phi's arms flow into it; a conversion's operand into its result; a stored
// value into its location (a local cell, a struct field shared by every base,
// or a global) and the location into every load of it; a call's arguments into
// every target's parameters (aligned for receivers); a target's returns into
// the call's results; a closure's bindings into its free variables. Calls to
// stop functions are not followed. Cycles need no special case.
type naiveTaint struct {
	succ map[any][]any // value or location -> what it flows into
}

func newNaiveTaint(p *workcensus.Program, stop func(*ssa.Function) bool) *naiveTaint {
	n := &naiveTaint{succ: map[any][]any{}}
	edge := func(from, to any) {
		if from != nil && to != nil {
			n.succ[from] = append(n.succ[from], to)
		}
	}
	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				switch instr := instr.(type) {
				case *ssa.Phi:
					for _, arm := range instr.Edges {
						edge(arm, instr)
					}
				case *ssa.MakeInterface:
					edge(instr.X, instr)
				case *ssa.ChangeInterface:
					edge(instr.X, instr)
				case *ssa.ChangeType:
					edge(instr.X, instr)
				case *ssa.Convert:
					edge(instr.X, instr)
				case *ssa.TypeAssert:
					edge(instr.X, instr)
				case *ssa.Store:
					if key := naiveLocation(instr.Addr); key != nil {
						edge(instr.Val, key)
					}
				case *ssa.UnOp:
					if instr.Op == token.MUL {
						if key := naiveLocation(instr.X); key != nil {
							edge(key, instr)
						}
					}
				case *ssa.MakeClosure:
					if closure, ok := instr.Fn.(*ssa.Function); ok {
						for i, binding := range instr.Bindings {
							if i >= len(closure.FreeVars) {
								continue
							}
							free := closure.FreeVars[i]
							edge(binding, free)
							// A captured variable is a cell the closure loads
							// and stores through its free variable: the
							// bound cell is that location.
							key := naiveLocation(binding)
							if key == nil || free.Referrers() == nil {
								continue
							}
							for _, ref := range *free.Referrers() {
								switch use := ref.(type) {
								case *ssa.UnOp:
									if use.Op == token.MUL {
										edge(key, use)
									}
								case *ssa.Store:
									if use.Addr == ssa.Value(free) {
										edge(use.Val, key)
									}
								}
							}
						}
					}
				}
				call, ok := instr.(ssa.CallInstruction)
				if !ok {
					continue
				}
				common := call.Common()
				// A context derived by the context package (WithCancel,
				// WithTimeout, WithValue, ...) carries its parent: the parent
				// reaches whatever the derived context reaches, which the
				// package's own bodies (a field store, a fresh allocation) do
				// not show.
				if derived, ok := call.(*ssa.Call); ok && derivesFromParent(common) && len(common.Args) > 0 {
					edge(common.Args[0], derived)
					if derived.Referrers() != nil {
						for _, ref := range *derived.Referrers() {
							if x, ok := ref.(*ssa.Extract); ok && x.Index == 0 {
								edge(common.Args[0], x)
							}
						}
					}
				}
				for _, target := range p.CallTargets(call) {
					if stop != nil && stop(target) {
						continue
					}
					offset := len(target.Params) - len(common.Args)
					if common.IsInvoke() && offset == 1 && len(target.Params) > 0 {
						edge(common.Value, target.Params[0])
					}
					for i, arg := range common.Args {
						if j := i + offset; j >= 0 && j < len(target.Params) {
							edge(arg, target.Params[j])
						}
					}
					value, isValue := call.(*ssa.Call)
					if !isValue {
						continue
					}
					for _, b := range target.Blocks {
						ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
						if !ok {
							continue
						}
						for index, result := range ret.Results {
							if len(ret.Results) == 1 {
								edge(result, value)
								continue
							}
							if value.Referrers() == nil {
								continue
							}
							for _, ref := range *value.Referrers() {
								if x, ok := ref.(*ssa.Extract); ok && x.Index == index {
									edge(result, x)
								}
							}
						}
					}
				}
			}
		}
	}
	return n
}

// naiveLocation identifies a stored location: a local cell, a struct field
// (shared by every base) or a global.
func naiveLocation(addr ssa.Value) any {
	switch addr := addr.(type) {
	case *ssa.Alloc:
		return addr
	case *ssa.Global:
		return addr
	case *ssa.FieldAddr:
		if ptr, ok := addr.X.Type().Underlying().(*types.Pointer); ok {
			if st, ok := ptr.Elem().Underlying().(*types.Struct); ok {
				return st.Field(addr.Field)
			}
		}
	}
	return nil
}

// reach returns every node a set of sources flows into, with, for each node,
// the sources that reach it (when track is set) or just the node (otherwise).
func (n *naiveTaint) reach(sources []ssa.Value, track bool) map[any][]ssa.Value {
	out := map[any][]ssa.Value{}
	if !track {
		var queue []any
		for _, source := range sources {
			if _, done := out[source]; !done {
				out[source] = nil
				queue = append(queue, source)
			}
		}
		for len(queue) > 0 {
			node := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			for _, next := range n.succ[node] {
				if _, done := out[next]; !done {
					out[next] = nil
					queue = append(queue, next)
				}
			}
		}
		return out
	}
	for _, source := range sources {
		seen := map[any]bool{source: true}
		queue := []any{source}
		for len(queue) > 0 {
			node := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			out[node] = append(out[node], source)
			for _, next := range n.succ[node] {
				if !seen[next] {
					seen[next] = true
					queue = append(queue, next)
				}
			}
		}
	}
	for node, origins := range out {
		sort.Slice(origins, func(i, j int) bool { return origins[i].Pos() < origins[j].Pos() })
		out[node] = origins
	}
	return out
}

// naiveSources returns the values of the loaded program matching source.
func naiveSources(p *workcensus.Program, source func(ssa.Value) bool) []ssa.Value {
	var out []ssa.Value
	seen := map[ssa.Value]bool{}
	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				if v, ok := instr.(ssa.Value); ok && source(v) && !seen[v] {
					seen[v] = true
					out = append(out, v)
				}
				for _, operand := range instr.Operands(nil) {
					if operand == nil || *operand == nil {
						continue
					}
					if c, ok := (*operand).(*ssa.Const); ok && source(c) && !seen[c] {
						seen[c] = true
						out = append(out, c)
					}
				}
			}
		}
	}
	return out
}

// naiveNilSource is a value that can be nil without any decision: a nil
// constant, a map lookup, a struct field value, a load (whose location starts
// zeroed) or a channel receive.
func naiveNilSource(v ssa.Value) bool {
	switch v := v.(type) {
	case *ssa.Const:
		return v.IsNil()
	case *ssa.Lookup, *ssa.Field:
		return true
	case *ssa.UnOp:
		return v.Op == token.MUL || v.Op == token.ARROW
	}
	return false
}

// derivesFromParent reports a context package function deriving a context
// from its first argument (context.WithCancel, WithTimeout, WithValue, ...).
func derivesFromParent(common *ssa.CallCommon) bool {
	callee := common.StaticCallee()
	return callee != nil && callee.Pkg != nil && callee.Pkg.Pkg.Path() == "context" && strings.HasPrefix(callee.Name(), "With") && len(common.Args) > 0 && isContext(common.Args[0].Type())
}
