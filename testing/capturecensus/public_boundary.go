package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// BoundaryCrossing is one caller edge the in-package request walk does not
// follow: a caller outside the policy package, or a dynamic call into an
// exported method (the Engine interface, or any other interface or function
// value reaching it). Every such cut is recorded, so the census's exclusion
// population is listed and counted, never silent. Holder is the request holder
// found beyond the cut by the module-wide walk, or empty when none exists.
type BoundaryCrossing struct {
	Entry, Caller, Via, Holder string
}

func (c BoundaryCrossing) String() string {
	beyond := "no request holder beyond it"
	if c.Holder != "" {
		beyond = "request held at " + c.Holder
	}
	return fmt.Sprintf("%s <- %s (%s): %s", c.Entry, c.Caller, c.Via, beyond)
}

// publicBoundary records the cuts of every in-package walk and answers, once
// per cut caller, whether a request holder lies beyond it in the module.
type publicBoundary struct {
	p         *workcensus.Program
	policy    Policy
	crossings map[[2]*ssa.Function]*BoundaryCrossing
	beyond    map[[2]*ssa.Function][]string
}

func newPublicBoundary(p *workcensus.Program, policy Policy) *publicBoundary {
	return &publicBoundary{p: p, policy: policy, crossings: map[[2]*ssa.Function]*BoundaryCrossing{}, beyond: map[[2]*ssa.Function][]string{}}
}

// cross records one cut edge and returns it.
func (b *publicBoundary) cross(caller, entry *ssa.Function) *BoundaryCrossing {
	key := [2]*ssa.Function{caller, entry}
	if c, ok := b.crossings[key]; ok {
		return c
	}
	c := &BoundaryCrossing{Entry: symbolOf(entry), Caller: symbolOf(caller), Via: crossingVia(caller, entry, b.policy)}
	if chain := b.holderBeyond(caller, entry); chain != nil {
		c.Holder = strings.Join(chain, " <- ")
	}
	b.crossings[key] = c
	return c
}

// holderBeyond returns the first chain, caller first, from caller (which
// calls entry) to a function holding a request context anywhere in the
// module, by the engine's one incoming walk with no package or dispatch limit.
// Excluded packages hold no product request.
func (b *publicBoundary) holderBeyond(caller, entry *ssa.Function) []string {
	key := [2]*ssa.Function{caller, entry}
	if chain, ok := b.beyond[key]; ok {
		return chain
	}
	excluded := func(fn *ssa.Function) bool {
		if fn.Pkg == nil {
			return false
		}
		_, ok := b.policy.ExcludedPackages[fn.Pkg.Pkg.Path()]
		return ok
	}
	var chain []string
	switch {
	case excluded(caller):
	case requestToward(caller, entry) != "":
		chain = []string{symbolOf(caller)}
	default:
		next := map[*ssa.Function]*ssa.Function{}
		var holder *ssa.Function
		b.p.WalkCallers([]*ssa.Function{caller}, func(up, callee *ssa.Function) (bool, bool) {
			if excluded(up) {
				return false, false
			}
			next[up] = callee
			if requestToward(up, callee) != "" {
				holder = up
				return false, true
			}
			return true, false
		})
		for step := holder; step != nil; step = next[step] {
			chain = append([]string{symbolOf(step)}, chain...)
		}
	}
	b.beyond[key] = chain
	return chain
}

// unfulfilled returns the first of crossings with a request holder beyond it.
func (b *publicBoundary) unfulfilled(crossings []*BoundaryCrossing) *BoundaryCrossing {
	for _, c := range crossings {
		if c.Holder != "" {
			return c
		}
	}
	return nil
}

// all lists every recorded crossing, sorted.
func (b *publicBoundary) all() []BoundaryCrossing {
	out := make([]BoundaryCrossing, 0, len(b.crossings))
	for _, c := range b.crossings {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// crossingVia names how caller reaches entry across the cut: a static call
// from another package, or the interface methods (or function value) a dynamic
// call goes through.
func crossingVia(caller, entry *ssa.Function, policy Policy) string {
	pkg := "a synthetic wrapper"
	if caller.Pkg != nil {
		pkg = caller.Pkg.Pkg.Path()
	}
	if workcensus.StaticCall(caller, entry) {
		return "static call from " + pkg
	}
	seen := map[string]bool{}
	var ifaces []string
	var collect func(fn *ssa.Function)
	collect = func(fn *ssa.Function) {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				call, ok := instr.(ssa.CallInstruction)
				if !ok || !call.Common().IsInvoke() || call.Common().Method.Name() != entry.Name() {
					continue
				}
				name := call.Common().Value.Type().String() + "." + entry.Name()
				if !seen[name] {
					seen[name] = true
					ifaces = append(ifaces, name)
				}
			}
		}
		for _, anon := range fn.AnonFuncs {
			collect(anon)
		}
	}
	collect(caller)
	sort.Strings(ifaces)
	if len(ifaces) == 0 {
		return "dynamic call (function value or wrapper) from " + pkg
	}
	return "dynamic dispatch through " + strings.Join(ifaces, ", ") + " from " + pkg
}

// CrossingInterfaces counts crossings by the interface methods they dispatch
// through ("static" and "function value" otherwise), so the population beyond
// Engine is visible.
func CrossingInterfaces(list []BoundaryCrossing) map[string]int {
	counts := map[string]int{}
	for _, c := range list {
		switch {
		case strings.HasPrefix(c.Via, "static call"):
			counts["static"]++
		case strings.HasPrefix(c.Via, "dynamic dispatch through "):
			names := strings.TrimPrefix(c.Via, "dynamic dispatch through ")
			names = names[:strings.LastIndex(names, " from ")]
			for _, name := range strings.Split(names, ", ") {
				counts[name]++
			}
		default:
			counts["function value"]++
		}
	}
	return counts
}
