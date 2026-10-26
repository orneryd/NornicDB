package storagecensus

import (
	"go/token"
	"go/types"
	"sort"
	"strings"

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
	a               *analysis
	stored          map[string]bool // StoredEntityReader method names
	reader          *types.Interface
	readers         []*types.Interface
	visible         *types.Interface // public Engine outcome, distinct from stored facts
	facts           []types.Type     // exact stored-result declarations from the loaded package
	errorPredicates map[*ssa.Function]bool
	readOperations  map[*types.Func]bool
	tainted         map[*ssa.Function]bool
}

func (a *analysis) decisions() []Decision {
	d := &decider{a: a, stored: map[string]bool{}, tainted: map[*ssa.Function]bool{}}
	d.errorPredicates = map[*ssa.Function]bool{}
	d.readOperations = map[*types.Func]bool{}
	if pkg := a.p.ssa.ImportedPackage("errors"); pkg != nil {
		for _, name := range []string{"Is", "As"} {
			if fn := pkg.Func(name); fn != nil {
				d.errorPredicates[fn] = true
			}
		}
	}
	if pkg := a.p.ssa.ImportedPackage(Module + "pkg/storage"); pkg != nil {
		// These are the existing operation owners, identified by their actual
		// declarations. Only an invocation whose callback has the stored-reader
		// contract is a stored source; binding/availability APIs are not reads.
		for _, name := range []string{"ReadWithCapability", "ReadValueWithCapability", "VisitWithCapability", "VisitWhileWithCapability"} {
			if declaration, ok := pkg.Pkg.Scope().Lookup(name).(*types.Func); ok {
				d.readOperations[declaration.Origin()] = true
			}
		}
		if declaration, ok := pkg.Pkg.Scope().Lookup("Engine").(*types.TypeName); ok {
			d.visible, _ = declaration.Type().Underlying().(*types.Interface)
		}
		if tn, ok := pkg.Pkg.Scope().Lookup("StoredEntityReader").(*types.TypeName); ok {
			if it, ok := tn.Type().Underlying().(*types.Interface); ok {
				d.reader = it
				d.readers = append(d.readers, it)
				for i := 0; i < it.NumMethods(); i++ {
					d.stored[it.Method(i).Name()] = true
				}
			}
		}
		if declaration, ok := pkg.Pkg.Scope().Lookup("ScopedStoredEntityReader").(*types.TypeName); ok {
			if contract, ok := declaration.Type().Underlying().(*types.Interface); ok {
				d.readers = append(d.readers, contract)
			}
		}
		// Author: Lane #1644 (Codex). Stored facts remain hidden-state sources
		// when the caller requests the payload-free operation. Resolve the
		// actual declarations, not function-name or receiver-name patterns.
		for _, name := range []string{"StoredReadResult", "StoredMetadata", "StoredNodeFact", "StoredEdgeFact"} {
			if declaration, ok := pkg.Pkg.Scope().Lookup(name).(*types.TypeName); ok {
				d.facts = append(d.facts, types.Unalias(declaration.Type()))
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
				// A callback-based probe can return constant true/false from
				// different successors of a read-dependent branch. Its result
				// still carries that dependency even without an SSA Phi.
				if branch, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); ok && t[branch.Cond] {
					for _, exit := range fn.Blocks {
						if ret, ok := exit.Instrs[len(exit.Instrs)-1].(*ssa.Return); ok && branch.Block().Succs[0].Dominates(exit) != branch.Block().Succs[1].Dominates(exit) {
							for _, value := range ret.Results {
								if types.Identical(value.Type(), types.Typ[types.Bool]) {
									d.tainted[fn] = true
									changed = true
								}
							}
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
	if d.storedOperation(c.StaticCallee(), c) {
		return true
	}
	if d.returnsFacts(c.Signature().Results()) {
		return true
	}
	if c.IsInvoke() {
		return d.stored[c.Method.Name()] && d.reader != nil && types.Implements(c.Value.Type(), d.reader)
	}
	callee := c.StaticCallee()
	if callee == nil {
		return false
	}
	name := d.a.p.funcName(callee)
	if visibilityPrimitives[name] || (d.tainted[callee] && !d.visibleOutcome(callee)) {
		return true
	}
	if recv := callee.Signature.Recv(); recv != nil && d.stored[callee.Name()] && d.reader != nil {
		return types.Implements(recv.Type(), d.reader)
	}
	return false
}

func (d *decider) storedOperation(fn *ssa.Function, call *ssa.CallCommon) bool {
	if fn == nil || d.reader == nil || len(call.Args) < 2 {
		return false
	}
	object, ok := fn.Object().(*types.Func)
	if !ok || !d.readOperations[object.Origin()] {
		return false
	}
	callback, ok := call.Args[1].Type().Underlying().(*types.Signature)
	if !ok || callback.Params().Len() == 0 {
		return false
	}
	contract, ok := callback.Params().At(0).Type().Underlying().(*types.Interface)
	// Operations deliberately bind a method-sized interface, not the full
	// StoredEntityReader. Require a nonempty actual subset of that method set.
	if !ok || contract.NumMethods() == 0 {
		return false
	}
	for _, stored := range d.readers {
		if types.Implements(stored, contract) {
			return true
		}
	}
	return false
}

// A public visible Engine outcome does not disclose the physical state behind
// its visibility decision. A consumer may legitimately branch on visible
// absence (MERGE admission, for example). Keep that policy boundary when
// propagating internal stored dependencies through the resolved call graph.
func (d *decider) visibleOutcome(fn *ssa.Function) bool {
	if d.visible == nil || fn.Signature.Recv() == nil || !types.Implements(fn.Signature.Recv().Type(), d.visible) {
		return false
	}
	for i := 0; i < d.visible.NumMethods(); i++ {
		method := d.visible.Method(i)
		if fn.Name() == method.Name() && types.Identical(fn.Signature, method.Type()) {
			return true
		}
	}
	return false
}

func (d *decider) returnsFacts(results *types.Tuple) bool {
	for i := 0; i < results.Len(); i++ {
		for _, fact := range d.facts {
			if types.Identical(types.Unalias(results.At(i).Type()), fact) {
				return true
			}
		}
	}
	return false
}

// callSource consumes the shared loader's VTA edges for bound methods and
// function values. Static-only inspection loses the stored dependency as soon
// as a capability operation invokes its selected callback indirectly.
// Author: Lane #1644 (Codex).
func (d *decider) callSource(fn *ssa.Function, call *ssa.Call) bool {
	if d.source(call.Common()) {
		return true
	}
	if d.a.p.cg != nil {
		if node := d.a.p.cg.Nodes[fn]; node != nil {
			for _, edge := range node.Out {
				if edge.Site == call && d.storedOperation(edge.Callee.Func, call.Common()) {
					return true
				}
			}
		}
	}
	return false
}

// taint is the values of fn that depend on a source.
func (d *decider) taint(fn *ssa.Function) map[ssa.Value]bool {
	t := map[ssa.Value]bool{}
	var work []ssa.Value
	for _, b := range fn.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok && d.callSource(fn, call) {
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
			case *ssa.Call:
				// Interpret only the actual standard error predicates and
				// builtin size operations here. An arbitrary effectful call
				// receiving a read value does not make its result hidden data.
				if d.errorPredicates[r.Common().StaticCallee()] {
					out = r
				} else if builtin, ok := r.Common().Value.(*ssa.Builtin); ok && (builtin.Name() == "len" || builtin.Name() == "cap") {
					out = r
				}
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
	errv := cmp.X
	nilComparison := false
	if c, ok := cmp.X.(*ssa.Const); ok && c.IsNil() {
		errv = cmp.Y
		nilComparison = true
	} else if c, ok := cmp.Y.(*ssa.Const); ok && c.IsNil() {
		nilComparison = true
	}
	if !types.Identical(errv.Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	if !nilComparison && !errorNonNilOnEntry(iff.Block(), errv) {
		return false
	}
	switch errv.(type) {
	case *ssa.Extract, *ssa.Call: // (v, err) or a lone error result
	default:
		return false
	}
	branch := iff.Block().Succs[0]
	if nilComparison {
		if cmp.Op == token.EQL {
			branch = iff.Block().Succs[1]
		}
		return returnsExactError(branch, errv)
	}
	return returnsExactError(branch, errv) || returnsExactError(iff.Block().Succs[1], errv)
}

// A filtered error return is not a hidden disposition only when a dominating
// branch has excluded the successful nil result for this exact SSA error.
func errorNonNilOnEntry(block *ssa.BasicBlock, errv ssa.Value) bool {
	for _, guard := range block.Parent().Blocks {
		if guard == block || !guard.Dominates(block) {
			continue
		}
		branch, ok := guard.Instrs[len(guard.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		cmp, ok := branch.Cond.(*ssa.BinOp)
		if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
			continue
		}
		other := cmp.Y
		if cmp.Y == errv {
			other = cmp.X
		} else if cmp.X != errv {
			continue
		}
		nilValue, ok := other.(*ssa.Const)
		if !ok || !nilValue.IsNil() {
			continue
		}
		nonnil := guard.Succs[0]
		if cmp.Op == token.EQL {
			nonnil = guard.Succs[1]
		}
		if nonnil.Dominates(block) {
			return true
		}
	}
	return false
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
