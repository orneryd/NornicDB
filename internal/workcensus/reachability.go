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
}

// ReachableFrom queries the already loaded program without loading or walking
// source again. Required roots must resolve. Empty Roots means no functions,
// rather than silently treating the whole program as statement reachable.
func (p *Program) ReachableFrom(roots []Root) (Reachability, error) {
	out := Reachability{Functions: map[*ssa.Function]bool{}}
	matched := make([]bool, len(roots))
	names := map[string]bool{}
	var queue []*ssa.Function
	for _, fn := range p.Functions {
		for i, root := range roots {
			if rootMatches(fn, root) {
				queue = append(queue, fn)
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
		fn := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if out.Functions[fn] {
			continue
		}
		out.Functions[fn] = true
		queue = append(queue, p.functionSuccessors(fn)...)
	}
	return out, nil
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
