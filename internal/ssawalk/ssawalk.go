// Package ssawalk enumerates the SSA bodies of the module and walks value
// dependency graphs over them. It owns no analysis: which instructions are
// sinks, which values are roots, which values are followed and in what order
// results are reported stay with each consumer.
//
// Two consumers share it: the buffered WAL binding oracle (pkg/storage), whose
// prefilter and alias traversal start from a struct field, and the stored
// entity census (testing/storagecensus), which starts from the state fields an
// address or map value designates. Both use instruction enumeration; the
// binding oracle also uses the reverse value graph.
package ssawalk

import "golang.org/x/tools/go/ssa"

// Instruction is one SSA instruction with the function and instruction that
// enclose it.
type Instruction struct {
	Func *ssa.Function
	Ins  ssa.Instruction
}

// Instructions lists every instruction of functions, in the order of the
// function list and of each body's blocks and instructions.
func Instructions(functions []*ssa.Function) []Instruction {
	var out []Instruction
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				out = append(out, Instruction{Func: fn, Ins: ins})
			}
		}
	}
	return out
}

// Values lists every value a body defines: its parameters and free variables,
// then its instructions that are values, in the same order.
func Values(functions []*ssa.Function) []ssa.Value {
	var out []ssa.Value
	for _, fn := range functions {
		for _, parameter := range fn.Params {
			out = append(out, parameter)
		}
		for _, free := range fn.FreeVars {
			out = append(out, free)
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if value, ok := ins.(ssa.Value); ok {
					out = append(out, value)
				}
			}
		}
	}
	return out
}

// Graph is a reverse value dependency graph. An edge from source to
// destination means that the destination may depend on the source, as the
// consumer's Edge classification decides.
type Graph struct {
	reverse map[ssa.Value][]ssa.Value
	values  map[ssa.Value]bool
}

// NewGraph is an empty graph that classifies each edge with edge.
func NewGraph() *Graph {
	return &Graph{reverse: map[ssa.Value][]ssa.Value{}, values: map[ssa.Value]bool{}}
}

// Add records that src may determine dst, when edge accepts the pair. Either
// value may be nil.
func (g *Graph) Add(src, dst ssa.Value, edge func(src, dst ssa.Value) bool) {
	if dst == nil || src == nil || !edge(src, dst) {
		return
	}
	g.reverse[src] = append(g.reverse[src], dst)
	g.values[src] = true
}

// AddValue records that the consumer classifies v, whether or not it has an
// edge.
func (g *Graph) AddValue(v ssa.Value) {
	if v != nil {
		g.values[v] = true
	}
}

// Known reports whether the consumer classified v.
func (g *Graph) Known(v ssa.Value) bool {
	return g.values[v]
}

// Walk is the set of values reachable backwards from starts through the
// graph, including the starts themselves. A value reached once is not
// queued again.
type Walk struct {
	live   map[ssa.Value]bool
	budget bool
}

// From marks every value reachable from starts as reached. With a budget, at
// most that many values are marked; a start or dependent the budget cannot
// mark keeps Reached false, and Exhausted reports that the walk stopped before
// the graph was exhausted.
func (g *Graph) From(starts []ssa.Value, budget int) *Walk {
	w := &Walk{live: map[ssa.Value]bool{}}
	queue := append([]ssa.Value(nil), starts...)
	limited := budget >= 0
	for len(queue) > 0 {
		value := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if value == nil || w.live[value] {
			continue
		}
		if limited && len(w.live) >= budget {
			w.budget = true
			return w
		}
		w.live[value] = true
		queue = append(queue, g.reverse[value]...)
	}
	return w
}

// Reached reports whether the walk marked v.
func (w *Walk) Reached(v ssa.Value) bool {
	return w.live[v]
}

// Exhausted reports whether the walk stopped at its budget with values left
// to mark.
func (w *Walk) Exhausted() bool {
	return w.budget
}
