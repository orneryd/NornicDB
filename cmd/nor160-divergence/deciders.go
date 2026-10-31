// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"encoding/hex"
	"go/types"
	"sort"

	"golang.org/x/tools/go/ssa"
)

// resultUses follows value users, including extracts/conversions, to returns and calls.
func resultUses(v ssa.Value) (bool, []ssa.CallInstruction) {
	seen := map[ssa.Value]bool{}
	queue := []ssa.Value{v}
	returned := false
	calls := []ssa.CallInstruction{}
	for len(queue) > 0 {
		x := queue[0]
		queue = queue[1:]
		if x == nil || seen[x] {
			continue
		}
		seen[x] = true
		refs := x.Referrers()
		if refs == nil {
			continue
		}
		for _, r := range *refs {
			if _, ok := r.(*ssa.Return); ok {
				returned = true
			}
			if c, ok := r.(ssa.CallInstruction); ok {
				calls = append(calls, c)
			}
			// Only unchanged-value relays carry a decision into another caller.
			// Following every computed value/call result makes all facts in an
			// evaluator appear to feed all of its ancestors.
			switch y := r.(type) {
			case *ssa.Extract:
				queue = append(queue, y)
			case *ssa.ChangeType:
				queue = append(queue, y)
			case *ssa.ChangeInterface:
				queue = append(queue, y)
			case *ssa.Convert:
				queue = append(queue, y)
			case *ssa.MakeInterface:
				queue = append(queue, y)
			case *ssa.Phi:
				queue = append(queue, y)
			}
		}
	}
	return returned, calls
}

func overlap(a, b map[string]bool) []string {
	out := []string{}
	for key := range a {
		if b[key] {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func (a *analysis) deciders() []Finding {
	// A consumer actually uses a call's result. Returned results can relay through
	// arbitrarily many input-package wrappers; main's unrelated subtrees do not match.
	consumers := map[*ssa.Function]map[*function]Site{}
	add := func(consumer *ssa.Function, fn *function, site Site) {
		if a.canonical(consumer) == nil {
			return
		}
		if consumers[consumer] == nil {
			consumers[consumer] = map[*function]Site{}
		}
		if old, ok := consumers[consumer][fn]; !ok || siteKey(site) < siteKey(old) {
			consumers[consumer][fn] = site
		}
	}
	for _, fn := range a.functions {
		if fn.Result == "" {
			continue
		}
		seen := map[*ssa.Function]bool{}
		queue := []*ssa.Function{fn.SSA}
		for len(queue) > 0 {
			f := queue[0]
			queue = queue[1:]
			if seen[f] {
				continue
			}
			seen[f] = true
			n := a.graph.Nodes[f]
			if n == nil {
				continue
			}
			for _, e := range n.In {
				if e.Site == nil {
					continue
				}
				v := e.Site.Value()
				if v == nil || v.Referrers() == nil || len(*v.Referrers()) == 0 {
					continue
				}
				caller := e.Caller.Func
				cf := a.canonical(caller)
				if cf == nil {
					continue
				}
				site := a.site(cf.Site.Package, cf.Site.Function, e.Pos(), "result_consumer")
				add(caller, fn, site)
				returned, calls := resultUses(v)
				for _, c := range calls {
					if node := a.graph.Nodes[caller]; node != nil {
						for _, out := range node.Out {
							if out.Site == c {
								add(out.Callee.Func, fn, site)
							}
						}
					}
				}
				if returned {
					queue = append(queue, caller)
				}
			}
		}
	}
	out := []Finding{}
	for consumer, deciders := range consumers {
		byResult := map[*types.Tuple][]*function{}
		for fn := range deciders {
			if fn.SSA != consumer {
				byResult[fn.ResultType] = append(byResult[fn.ResultType], fn)
			}
		}
		for _, fns := range byResult {
			result := fns[0].Result
			sort.Slice(fns, func(i, j int) bool { return fns[i].Site.Function < fns[j].Site.Function })
			rows := make([][]byte, len(fns))
			participates := make([]bool, len(fns))
			pairs := 0
			for i := range rows {
				rows[i] = make([]byte, (len(fns)+7)/8)
			}
			for i, x := range fns {
				for j := i + 1; j < len(fns); j++ {
					y := fns[j]
					if !parameterOverlap(x, y) && !intersects(x.Reads, y.Reads) {
						continue
					}
					if a.reachable(x.SSA, y.SSA) || a.reachable(y.SSA, x.SSA) {
						continue
					}
					rows[i][j/8] |= 1 << uint(j%8)
					participates[i] = true
					participates[j] = true
					pairs++
				}
			}
			if pairs == 0 {
				continue
			}
			type member struct {
				Function       string   `json:"function"`
				ParameterTypes []string `json:"parameter_types"`
				ReceiverReads  []string `json:"receiver_field_reads"`
				Witness        Site     `json:"result_use_witness"`
			}
			members := []member{}
			sites := []Site{}
			indices := []int{}
			for i, fn := range fns {
				if participates[i] {
					indices = append(indices, i)
					members = append(members, member{fn.Site.Function, sortedKeys(fn.Inputs), sortedKeys(fn.Reads), deciders[fn]})
					sites = append(sites, fn.Site, deciders[fn])
				}
			}
			encoded := []string{}
			for _, oldI := range indices {
				row := make([]byte, (len(indices)+7)/8)
				for newJ, oldJ := range indices {
					if rows[oldI][oldJ/8]&(1<<uint(oldJ%8)) != 0 {
						row[newJ/8] |= 1 << uint(newJ%8)
					}
				}
				encoded = append(encoded, hex.EncodeToString(row))
			}
			out = append(out, Finding{Kind: "parallel_deciders", Subject: consumer.String() + ":" + result, Why: "The marked independent pairs have equal typed results and overlapping inputs and feed a common result consumer; inspect whether one fact is decided twice.", Sites: sites, Evidence: map[string]any{"consumer": consumer.String(), "results": result, "members": members, "qualifying_pairs": pairs, "pair_rows_hex": encoded, "pair_encoding": "row i bit j (least-significant bit first per byte) denotes qualifying pair i,j; only j>i is stored"}})
		}
	}
	return out
}

func parameterOverlap(a, b *function) bool {
	x, y := a.SSA.Signature.Params(), b.SSA.Signature.Params()
	for i := 0; i < x.Len(); i++ {
		for j := 0; j < y.Len(); j++ {
			if types.Identical(x.At(i).Type(), y.At(j).Type()) {
				return true
			}
		}
	}
	return false
}

func intersects(a, b map[string]bool) bool {
	for k := range a {
		if b[k] {
			return true
		}
	}
	return false
}
