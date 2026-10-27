package storagecensus

import (
	"go/token"
	"go/types"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// visibilityPrimitives decide whether a stored entity is hidden: decay and
// manual suppression. With the stored reads (the methods of
// storage.StoredEntityReader, which see hidden entities), they are the
// sources of a hidden-policy decision.
var visibilityPrimitives = map[string]bool{
	"pkg/storage.BadgerEngine.filterEntityByDecay": true,
	"pkg/storage.BadgerEngine.manuallySuppressed":  true,
}

// Decision is a branch on hidden state (or on a read that sees hidden
// entities) that decides whether ordered state is written: a hidden-ID
// disposition, a suppression change, an accounting choice.
type Decision struct {
	Func string `json:"func"`
	Pos  string `json:"pos"`
	// Gates is the write the branch decides: the instruction in Func and
	// the path from it to the Badger write.
	Gates   string   `json:"gates"`
	Owners  []string `json:"owners,omitempty"`
	unowned bool
}

type decider struct {
	a       *analysis
	stored  map[string]bool // StoredEntityReader method names
	reader  *types.Interface
	tainted map[*ssa.Function]bool
}

func (a *analysis) decisions() []Decision {
	d := &decider{a: a, stored: map[string]bool{}, tainted: map[*ssa.Function]bool{}}
	if pkg := a.p.ssa.ImportedPackage(Module + "pkg/storage"); pkg != nil {
		if tn, ok := pkg.Pkg.Scope().Lookup("StoredEntityReader").(*types.TypeName); ok {
			if it, ok := tn.Type().Underlying().(*types.Interface); ok {
				d.reader = it
				for i := 0; i < it.NumMethods(); i++ {
					d.stored[it.Method(i).Name()] = true
				}
			}
		}
	}
	// Returns that depend on a source make a function a source, to a fixed
	// point.
	for changed := true; changed; {
		changed = false
		for _, fn := range a.p.funcs {
			if d.tainted[fn] {
				continue
			}
			t := d.taint(fn)
			for _, b := range fn.Blocks {
				if ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return); ok {
					for _, v := range ret.Results {
						if t[v] {
							d.tainted[fn] = true
							changed = true
						}
					}
				}
			}
		}
	}
	proofs := a.bodyOwnerProofs()
	var out []Decision
	for _, fn := range a.p.funcs {
		t := d.taint(fn)
		for _, b := range fn.Blocks {
			iff, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If)
			if !ok || !t[iff.Cond] || d.propagatesError(iff) {
				continue
			}
			if loopBound(b) {
				continue // iterating over what a read returned decides nothing
			}
			if gates := d.gatedWrites(fn, b); len(gates) > 0 {
				proof := d.branchOwnerProof(fn, b, proofs)
				owners := make([]string, 0, len(proof.owners))
				for owner := range proof.owners {
					owners = append(owners, owner)
				}
				sort.Strings(owners)
				out = append(out, Decision{Func: a.p.funcName(fn), Pos: a.p.pos(iff.Cond.Pos()), Gates: a.bodyPath(fn, gates[0]), Owners: owners, unowned: proof.unowned})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Func != out[j].Func {
			return out[i].Func < out[j].Func
		}
		return out[i].Pos < out[j].Pos
	})
	return out
}

// source reports whether a call returns hidden state or a read that sees
// hidden entities.
func (d *decider) source(c *ssa.CallCommon) bool {
	if c.IsInvoke() {
		return d.stored[c.Method.Name()] && d.reader != nil && types.Implements(c.Value.Type(), d.reader)
	}
	callee := c.StaticCallee()
	if callee == nil {
		return false
	}
	name := d.a.p.funcName(callee)
	if visibilityPrimitives[name] || d.tainted[callee] {
		return true
	}
	if recv := callee.Signature.Recv(); recv != nil && d.stored[callee.Name()] && d.reader != nil {
		return types.Implements(recv.Type(), d.reader)
	}
	return false
}

// taint is the values of fn that depend on a source.
func (d *decider) taint(fn *ssa.Function) map[ssa.Value]bool {
	t := map[ssa.Value]bool{}
	var work []ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok && d.source(call.Common()) {
				t[call] = true
				work = append(work, call)
			}
		}
	}
	for len(work) > 0 {
		v := work[len(work)-1]
		work = work[:len(work)-1]
		refs := v.Referrers()
		if refs == nil {
			continue
		}
		for _, ref := range *refs {
			var out ssa.Value
			switch r := ref.(type) {
			case *ssa.Extract, *ssa.BinOp, *ssa.UnOp, *ssa.Phi, *ssa.Field, *ssa.FieldAddr, *ssa.Convert,
				*ssa.ChangeType, *ssa.MakeInterface, *ssa.TypeAssert, *ssa.Slice, *ssa.Index, *ssa.IndexAddr, *ssa.Lookup:
				out = r.(ssa.Value)
			}
			if out != nil && !t[out] {
				t[out] = true
				work = append(work, out)
			}
		}
	}
	return t
}

// writes reports whether fn reaches a write of an entity body or of
// suppression state: the writes a hidden-ID disposition decides.
func (d *decider) writes(fn *ssa.Function) bool {
	return d.a.bodyWriters[fn]
}

func (d *decider) instrWrites(fn *ssa.Function, ins ssa.Instruction) bool {
	return d.a.bodyInstr(fn, ins)
}

// gatedWrites returns every entity-body or suppression write instruction
// reachable from only one successor within this iteration. The first is a
// display representative; every instruction contributes to ownership proof.
func (d *decider) gatedWrites(fn *ssa.Function, b *ssa.BasicBlock) []ssa.Instruction {
	r0, r1 := reachable(b.Succs[0]), reachable(b.Succs[1])
	var writes []ssa.Instruction
	for _, blk := range fn.Blocks {
		if r0[blk] == r1[blk] {
			continue
		}
		for _, ins := range blk.Instrs {
			if d.instrWrites(fn, ins) {
				writes = append(writes, ins)
			}
		}
	}
	return writes
}
func (d *decider) branchOwnerProof(fn *ssa.Function, b *ssa.BasicBlock, proofs map[*ssa.Function]bodyOwnerProof) bodyOwnerProof {
	proof := bodyOwnerProof{owners: map[string]bool{}}
	for _, ins := range d.gatedWrites(fn, b) {
		proof.merge(d.a.instructionOwnerProof(fn, ins, proofs))
	}
	return proof
}

// loopBound reports whether b is a loop header: the branch that ends it
// bounds an iteration (over what a read returned, say), and decides no
// write of its own.
func loopBound(b *ssa.BasicBlock) bool {
	for _, pred := range b.Preds {
		if b.Dominates(pred) {
			return true
		}
	}
	return false
}

// reachable is the set of blocks reachable from start, start included,
// within one iteration: loop back edges (to a block that dominates the edge's
// source) are not followed, so a decision inside a loop body still decides
// that iteration's write.
func reachable(start *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	stack := []*ssa.BasicBlock{start}
	for len(stack) > 0 {
		b := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[b] {
			continue
		}
		seen[b] = true
		for _, s := range b.Succs {
			if !s.Dominates(b) {
				stack = append(stack, s)
			}
		}
	}
	return seen
}

// propagatesError reports a branch on a source's error that only returns
// that error: the operation fails with the read, it decides nothing.
func (d *decider) propagatesError(iff *ssa.If) bool {
	cmp, ok := iff.Cond.(*ssa.BinOp)
	if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
		return false
	}
	errv, nilIndex, nilComparison := workcensus.NilComparison(cmp)
	if !nilComparison {
		errv = cmp.X
	}
	if !types.Identical(errv.Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	// A filtered error return is not a hidden disposition only when a
	// dominating branch has excluded the successful nil result for this exact
	// SSA error (the engine's NonNilAt).
	if !nilComparison && !workcensus.NonNilAt(errv, iff.Block()) {
		return false
	}
	switch errv.(type) {
	case *ssa.Extract, *ssa.Call: // (v, err) or a lone error result
	default:
		return false
	}
	if nilComparison {
		return returnsExactError(iff.Block().Succs[1-nilIndex], errv)
	}
	return returnsExactError(iff.Block().Succs[0], errv) || returnsExactError(iff.Block().Succs[1], errv)
}

func returnsExactError(branch *ssa.BasicBlock, errv ssa.Value) bool {
	ret, ok := branch.Instrs[len(branch.Instrs)-1].(*ssa.Return)
	if !ok {
		return false
	}
	for _, v := range ret.Results {
		if v == errv {
			// A direct return is propagation only when nothing in its branch
			// mutates state or calls another effect before returning.
			for _, ins := range branch.Instrs {
				switch ins.(type) {
				case *ssa.Return, *ssa.DebugRef:
				default:
					return false
				}
			}
			return true
		}
		// A defer lowers even an unnamed error result to a local return cell.
		// Accept only a nonescaping cell written with this exact error, then
		// loaded after RunDefers; captured/named results remain fail-closed.
		load, ok := v.(*ssa.UnOp)
		if !ok || load.Op != token.MUL {
			continue
		}
		cell, ok := load.X.(*ssa.Alloc)
		if !ok || cell.Heap {
			continue
		}
		safe := true
		stored := false
		for _, use := range *cell.Referrers() {
			switch use := use.(type) {
			case *ssa.Store:
				if use.Addr != cell {
					safe = false
				}
			case *ssa.UnOp:
				if use.Op != token.MUL {
					safe = false
				}
			case *ssa.DebugRef:
			default:
				safe = false
			}
		}
		for _, ins := range branch.Instrs {
			switch ins := ins.(type) {
			case *ssa.Store:
				if ins.Addr != cell || ins.Val != errv {
					safe = false
				} else {
					stored = true
				}
			case *ssa.UnOp:
				if ins.X != cell || ins.Op != token.MUL {
					safe = false
				}
			case *ssa.RunDefers, *ssa.Return, *ssa.DebugRef:
			default:
				safe = false
			}
		}
		if safe && stored {
			return true
		}
	}
	return false
}

// bodyOwnerProof retains every entity-body effect, not only the representative
// shortest path printed in Gates. An incidental owned path cannot cover a
// second, unowned write reached by the same decision.
type bodyOwnerProof struct {
	owners  map[string]bool
	unowned bool
}

func (p *bodyOwnerProof) merge(other bodyOwnerProof) bool {
	changed := false
	if other.unowned && !p.unowned {
		p.unowned = true
		changed = true
	}
	for owner := range other.owners {
		if !p.owners[owner] {
			p.owners[owner] = true
			changed = true
		}
	}
	return changed
}
func (a *analysis) instructionOwnerProof(fn *ssa.Function, ins ssa.Instruction, proofs map[*ssa.Function]bodyOwnerProof) bodyOwnerProof {
	p := bodyOwnerProof{owners: map[string]bool{}}
	for _, chain := range a.bodyChains[ins] {
		owner := decisionOwnerOnPath(Decision{Gates: strings.Join(chain, " -> ")})
		if owner == "" {
			p.unowned = true
		} else {
			p.owners[owner] = true
		}
	}
	switch ins := ins.(type) {
	case ssa.CallInstruction:
		for _, callee := range a.callees(fn, ins) {
			p.merge(proofs[callee])
		}
	case *ssa.MakeClosure:
		p.merge(proofs[ins.Fn.(*ssa.Function)])
	}
	return p
}
func (a *analysis) bodyOwnerProofs() map[*ssa.Function]bodyOwnerProof {
	proofs := map[*ssa.Function]bodyOwnerProof{}
	for _, fn := range a.p.funcs {
		proofs[fn] = bodyOwnerProof{owners: map[string]bool{}}
	}
	for changed := true; changed; {
		changed = false
		for _, fn := range a.p.funcs {
			if !a.bodyWriters[fn] {
				continue
			}
			p := proofs[fn]
			name := a.p.funcName(fn)
			if decisionOwners[name] != "" {
				if !p.owners[name] {
					p.owners[name] = true
					changed = true
				}
				continue
			}
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					if p.merge(a.instructionOwnerProof(fn, ins, proofs)) {
						changed = true
					}
				}
			}
			proofs[fn] = p
		}
	}
	return proofs
}
