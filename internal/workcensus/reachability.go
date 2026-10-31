package workcensus

import (
	"fmt"
	"sort"

	"golang.org/x/tools/go/ssa"
)

// Reachability is the shared engine's conservative call/closure population for
// selected roots in a loaded Program. Functions is a read-only process-local
// set, including generic instances and closures through opaque callbacks.
type Reachability struct {
	Roots     []string
	Functions map[*ssa.Function]bool
	parents   map[*ssa.Function]*ssa.Function
}

// ReachableFrom queries the already loaded program without loading or walking
// source again. Required roots must resolve. Empty Roots means no functions,
// rather than silently treating the whole program as statement reachable.
func (p *Program) ReachableFrom(roots []Root) (Reachability, error) {
	return p.reachableFrom(roots, p.functionSuccessors)
}

// DirectReachableFrom is the work roots perform themselves: static calls and
// function literals only, not the callbacks or interface values they may run.
// It shares ReachableFrom's walk; only the successor relation differs.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func (p *Program) DirectReachableFrom(roots []Root) (Reachability, error) {
	return p.reachableFrom(roots, directSuccessors)
}

func directSuccessors(fn *ssa.Function) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	for _, child := range fn.AnonFuncs {
		seen[child] = true
	}
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			if call, ok := ins.(ssa.CallInstruction); ok {
				if callee := call.Common().StaticCallee(); callee != nil {
					seen[callee] = true
				}
			}
		}
	}
	return sortedFunctions(seen)
}

func (p *Program) reachableFrom(roots []Root, successors func(*ssa.Function) []*ssa.Function) (Reachability, error) {
	out := Reachability{Functions: map[*ssa.Function]bool{}, parents: map[*ssa.Function]*ssa.Function{}}
	matched := make([]bool, len(roots))
	names := map[string]bool{}
	var queue []*ssa.Function
	for _, fn := range p.Functions {
		for i, root := range roots {
			if rootMatches(fn, root) {
				queue = append(queue, fn)
				out.parents[fn] = nil
				matched[i] = true
				names[sourceFunction(fn).String()] = true
			}
		}
	}
	for i, root := range roots {
		if root.Required && !matched[i] {
			return out, fmt.Errorf("required root missing: %s.%s receiver %s", root.Package, root.Name, root.Receiver)
		}
	}
	for name := range names {
		out.Roots = append(out.Roots, name)
	}
	sort.Strings(out.Roots)
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		if out.Functions[fn] {
			continue
		}
		out.Functions[fn] = true
		for _, next := range successors(fn) {
			if _, seen := out.parents[next]; !seen {
				out.parents[next] = fn
				queue = append(queue, next)
			}
		}
	}
	return out, nil
}

// PathTo returns a deterministic shortest conservative function/closure path
// from one selected root to a matching target, or nil when none is visible.
// It uses the same population as Contains, including compiler wrappers and
// lexical callbacks through opaque code. It proves possible reachability,
// never runtime invocation, receiver-instance identity or path dominance.
func (r Reachability) PathTo(root Root) []string {
	var best []string
	for _, fn := range sortedFunctions(r.Functions) {
		if !rootMatches(fn, root) {
			continue
		}
		var path []string
		for at := fn; at != nil; at = r.parents[at] {
			path = append(path, sourceFunction(at).String())
		}
		for a, b := 0, len(path)-1; a < b; a, b = a+1, b-1 {
			path[a], path[b] = path[b], path[a]
		}
		if best == nil || len(path) < len(best) {
			best = path
		}
	}
	return best
}

// Contains reports whether a matching source function is in this population.
// This is source reachability, not proof that a call dominates every path.
func (r Reachability) Contains(root Root) bool {
	for fn := range r.Functions {
		if rootMatches(fn, root) {
			return true
		}
	}
	return false
}

// OutermostFunction is the source function that lexically encloses fn: fn
// itself, or the declaration containing a function literal.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func OutermostFunction(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	return fn
}

// WalkCallers is the shared incoming walk, the reverse of ReachableFrom. From
// roots it follows SourceCallSites breadth-first; each site is attributed to its
// outermost enclosing source function. visit sees every caller once, with the
// function it calls toward, and reports whether to walk past that caller and
// whether to stop. Visibility is the call graph's, never a runtime proof.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func (p *Program) WalkCallers(roots []*ssa.Function, visit func(caller, callee *ssa.Function) (expand, stop bool)) {
	seen := map[*ssa.Function]bool{}
	for _, root := range roots {
		seen[root] = true
	}
	queue := append([]*ssa.Function(nil), roots...)
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		for _, site := range p.SourceCallSites(fn) {
			caller := OutermostFunction(site.Parent())
			if seen[caller] {
				continue
			}
			seen[caller] = true
			expand, stop := visit(caller, fn)
			if stop {
				return
			}
			if expand {
				queue = append(queue, caller)
			}
		}
	}
}
