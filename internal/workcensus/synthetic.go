package workcensus

import (
	"fmt"
	"go/token"
	"go/types"
	"slices"
	"sort"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/ssa"
)

// DynamicCallTargets indexes a call graph's dynamic edges by their call sites.
// It is the one owner of which functions a dynamic site may call: every client
// builds its per-site view from it once, instead of scanning the graph per call.
func DynamicCallTargets(graph *callgraph.Graph) map[ssa.CallInstruction][]*ssa.Function {
	sets := map[ssa.CallInstruction]map[*ssa.Function]bool{}
	if graph != nil {
		for _, node := range graph.Nodes {
			for _, edge := range node.Out {
				if edge.Site == nil || edge.Callee.Func == nil || edge.Site.Common().StaticCallee() != nil {
					continue
				}
				if sets[edge.Site] == nil {
					sets[edge.Site] = map[*ssa.Function]bool{}
				}
				sets[edge.Site][edge.Callee.Func] = true
			}
		}
	}
	index := make(map[ssa.CallInstruction][]*ssa.Function, len(sets))
	for site, set := range sets {
		index[site] = sortedFunctions(set)
	}
	return index
}

// CallTargets returns the conservative targets of the actual SSA call site,
// including compiler wrappers and VTA-resolved interface/function values.
// An empty result is unknown, not evidence that the call has no effects.
func (p *Program) CallTargets(site ssa.CallInstruction) []*ssa.Function {
	if fn := site.Common().StaticCallee(); fn != nil {
		return []*ssa.Function{fn}
	}
	if p.dynamicTargets == nil {
		p.dynamicTargets = DynamicCallTargets(p.CallGraph)
	}
	return p.dynamicTargets[site]
}

// IncludeSyntheticFunctions retains seeds and the compiler-created bodies they
// reference or call, transitively. A wrapper's object may belong to an external
// package even though its executable body was generated for a selected caller.
// This does not load external source bodies or change a client's root policy.
func (p *Program) IncludeSyntheticFunctions(seeds []*ssa.Function) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	queue := append([]*ssa.Function(nil), seeds...)
	add := func(fn *ssa.Function) {
		if fn != nil && fn.Synthetic != "" && fn.Blocks != nil && !seen[fn] {
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		fn := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[fn] {
			continue
		}
		seen[fn] = true
		for _, target := range p.functionSuccessors(fn) {
			add(target)
		}
	}
	return sortedFunctions(seen)
}

// One generic compiled successor population for root and wrapper queries.
// Values passed to opaque code or stored for later remain conservative members.
func (p *Program) functionSuccessors(fn *ssa.Function) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	if p.CallGraph != nil {
		if node := p.CallGraph.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				if edge.Callee.Func != nil {
					seen[edge.Callee.Func] = true
				}
			}
		}
	}
	for _, child := range fn.AnonFuncs {
		seen[child] = true
	}
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			for _, op := range ins.Operands(nil) {
				if op != nil && *op != nil {
					if target, ok := (*op).(*ssa.Function); ok {
						seen[target] = true
					}
				}
			}
		}
	}
	return sortedFunctions(seen)
}

// SourceCallSites follows incoming call-graph edges through source-less
// wrappers, stopping at the nearest real source invocation on each path.
// All alternatives are retained: attribution is not a proof of execution,
// acquisition, callback lifetime or association with one receiver instance.
// No result means there is no visible invocation, not that a value cannot run.
// A source-origin query unions its visible generic instances. Querying one
// concrete instance retains that instance's attribution instead.
func (p *Program) SourceCallSites(fn *ssa.Function) []ssa.CallInstruction {
	sites := map[ssa.CallInstruction]bool{}
	for _, path := range p.SourceCallPaths(fn) {
		sites[path.Site] = true
	}
	var out []ssa.CallInstruction
	for site := range sites {
		out = append(out, site)
	}
	sort.Slice(out, func(i, j int) bool { return instructionLess(out[i], out[j]) })
	return out
}

// CallPath is one way a real source call reaches a function: the call, and the
// source-less wrappers between it and the function, nearest the call first.
type CallPath struct {
	Site ssa.CallInstruction
	Via  []*ssa.Function
}

// SourceCallPaths lists every path SourceCallSites follows, each with its
// wrappers, in stable order. A wrapper repeated on one path ends that path.
func (p *Program) SourceCallPaths(fn *ssa.Function) []CallPath {
	if p.CallGraph == nil {
		return nil
	}
	var out []CallPath
	done := map[string]bool{}
	var visit func(fn *ssa.Function, via []*ssa.Function)
	visit = func(fn *ssa.Function, via []*ssa.Function) {
		node := p.CallGraph.Nodes[fn]
		if node == nil {
			return
		}
		for _, edge := range node.In {
			if edge.Site != nil && realSourceInstruction(edge.Site) {
				key := fmt.Sprintf("%p %v", edge.Site, via)
				if !done[key] {
					done[key] = true
					out = append(out, CallPath{Site: edge.Site, Via: append([]*ssa.Function(nil), via...)})
				}
			} else if caller := edge.Caller.Func; caller != nil && caller.Synthetic != "" && !slices.Contains(via, caller) {
				visit(caller, append([]*ssa.Function{caller}, via...))
			}
		}
	}
	for _, variant := range p.sourceInstances(fn) {
		visit(variant, nil)
	}
	sort.SliceStable(out, func(i, j int) bool { return instructionLess(out[i].Site, out[j].Site) })
	return out
}

// BoundReceiver returns the receiver of the method value on this path: the
// bound variable of its $bound wrapper for a method named name, when there is
// one.
func (c CallPath) BoundReceiver(name string) (types.Type, bool) {
	for _, wrapper := range c.Via {
		if wrapper.Name() == name+"$bound" && len(wrapper.FreeVars) == 1 {
			return wrapper.FreeVars[0].Type(), true
		}
	}
	return nil, false
}

// SuppliedContexts lists the context values a call passes, in Is order: its
// context arguments, and what is stored into context fields of a struct it
// passes by value from a local variable.
func SuppliedContexts(site ssa.CallInstruction, isContext func(types.Type) bool) []ssa.Value {
	var out []ssa.Value
	for _, arg := range site.Common().Args {
		if isContext(arg.Type()) {
			out = append(out, arg)
			continue
		}
		load, ok := arg.(*ssa.UnOp)
		if !ok || load.Op != token.MUL {
			continue
		}
		cell, ok := load.X.(*ssa.Alloc)
		if !ok || cell.Referrers() == nil {
			continue
		}
		for _, ref := range *cell.Referrers() {
			field, ok := ref.(*ssa.FieldAddr)
			if !ok || field.Referrers() == nil {
				continue
			}
			for _, use := range *field.Referrers() {
				if store, ok := use.(*ssa.Store); ok && store.Addr == ssa.Value(field) && isContext(store.Val.Type()) {
					out = append(out, store.Val)
				}
			}
		}
	}
	return out
}

// SourceReferences retains the nearest real source instructions that refer to
// a function value through compiler wrappers. These are conservative carriers,
// not invocation witnesses. In particular a stored value remains an unresolved
// future effect even when no visible call-graph edge invokes it.
// Source origins include carriers of every visible generic instance; a query
// for a concrete instance remains specific to that instance.
func (p *Program) SourceReferences(fn *ssa.Function) []ssa.Instruction {
	seen := map[*ssa.Function]bool{}
	sites := map[ssa.Instruction]bool{}
	var visit func(*ssa.Function)
	visit = func(target *ssa.Function) {
		if seen[target] {
			return
		}
		seen[target] = true
		for _, owner := range p.Functions {
			for _, block := range owner.Blocks {
				for _, ins := range block.Instrs {
					for _, op := range ins.Operands(nil) {
						if op == nil || *op != target {
							continue
						}
						if realSourceInstruction(ins) {
							sites[ins] = true
						} else if owner.Synthetic != "" {
							visit(owner)
						}
					}
				}
			}
		}
	}
	for _, variant := range p.sourceInstances(fn) {
		visit(variant)
	}
	var out []ssa.Instruction
	for site := range sites {
		out = append(out, site)
	}
	sort.Slice(out, func(i, j int) bool { return instructionLess(out[i], out[j]) })
	return out
}

// Source queries use the same generic origin relation as root reachability.
// Instances are executable SSA targets, not separate source declarations.
func (p *Program) sourceInstances(fn *ssa.Function) []*ssa.Function {
	variants := map[*ssa.Function]bool{fn: true}
	if fn.Origin() == nil {
		for _, candidate := range p.Functions {
			if sourceFunction(candidate) == fn {
				variants[candidate] = true
			}
		}
	}
	return sortedFunctions(variants)
}

func realSourceInstruction(ins ssa.Instruction) bool {
	return ins != nil && ins.Pos().IsValid() && ins.Parent() != nil && (ins.Parent().Syntax() != nil || ins.Parent().Pkg != nil)
}

func instructionLess(a, b ssa.Instruction) bool {
	if a.Parent().String() != b.Parent().String() {
		return a.Parent().String() < b.Parent().String()
	}
	if a.Pos() != b.Pos() {
		return a.Pos() < b.Pos()
	}
	return a.String() < b.String()
}

func sortedFunctions(set map[*ssa.Function]bool) []*ssa.Function {
	out := make([]*ssa.Function, 0, len(set))
	for fn := range set {
		out = append(out, fn)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].String() != out[j].String() {
			return out[i].String() < out[j].String()
		}
		return out[i].Pos() < out[j].Pos()
	})
	return out
}
