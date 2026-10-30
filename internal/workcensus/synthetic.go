package workcensus

import (
	"sort"

	"golang.org/x/tools/go/ssa"
)

// CallTargets returns the conservative targets of the actual SSA call site,
// including compiler wrappers and VTA-resolved interface/function values.
// An empty result is unknown, not evidence that the call has no effects.
func (p *Program) CallTargets(site ssa.CallInstruction) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	if fn := site.Common().StaticCallee(); fn != nil {
		seen[fn] = true
	}
	if p.CallGraph != nil {
		if node := p.CallGraph.Nodes[site.Parent()]; node != nil {
			for _, edge := range node.Out {
				if edge.Site == site && edge.Callee.Func != nil {
					seen[edge.Callee.Func] = true
				}
			}
		}
	}
	return sortedFunctions(seen)
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
func (p *Program) SourceCallSites(fn *ssa.Function) []ssa.CallInstruction {
	seen := map[*ssa.Function]bool{}
	sites := map[ssa.CallInstruction]bool{}
	var visit func(*ssa.Function)
	visit = func(fn *ssa.Function) {
		if seen[fn] || p.CallGraph == nil {
			return
		}
		seen[fn] = true
		if node := p.CallGraph.Nodes[fn]; node != nil {
			for _, edge := range node.In {
				if edge.Site != nil && realSourceInstruction(edge.Site) {
					sites[edge.Site] = true
				} else if edge.Caller.Func != nil && edge.Caller.Func.Synthetic != "" {
					visit(edge.Caller.Func)
				}
			}
		}
	}
	visit(fn)
	var out []ssa.CallInstruction
	for site := range sites {
		out = append(out, site)
	}
	sort.Slice(out, func(i, j int) bool { return instructionLess(out[i], out[j]) })
	return out
}

// SourceReferences retains the nearest real source instructions that refer to
// a function value through compiler wrappers. These are conservative carriers,
// not invocation witnesses. In particular a stored value remains an unresolved
// future effect even when no visible call-graph edge invokes it.
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
	visit(fn)
	var out []ssa.Instruction
	for site := range sites {
		out = append(out, site)
	}
	sort.Slice(out, func(i, j int) bool { return instructionLess(out[i], out[j]) })
	return out
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
