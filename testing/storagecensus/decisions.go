package storagecensus

import (
	"go/constant"
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
	r0, r1 := reachable(b, b.Succs[0]), reachable(b, b.Succs[1])
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

// reachable is the set of blocks reachable over the edge from -> start,
// within one iteration. Check that initial edge as well as every later edge:
// an arm that jumps straight to the loop header starts the next iteration.
func reachable(from, start *ssa.BasicBlock) map[*ssa.BasicBlock]bool {
	seen := map[*ssa.BasicBlock]bool{}
	if start.Dominates(from) {
		return seen
	}
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
	case *ssa.Extract, *ssa.Call, *ssa.Phi: // exact error, including a join of reader results
	default:
		return false
	}
	if nilComparison {
		return returnsExactError(iff.Block().Succs[1-nilIndex], errv)
	}
	return returnsExactError(iff.Block().Succs[0], errv) || returnsExactError(iff.Block().Succs[1], errv)
}

func returnsExactError(branch *ssa.BasicBlock, errv ssa.Value) bool {
	if catalogAbortReturnsExact(branch, errv) {
		return true
	}
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
					if !returnedAggregateTransfer(ins, ret) {
						return false
					}
				}
			}
			return true
		}
		// A contextual wrap is propagation only when it is the standard
		// library's fmt.Errorf around exactly this read error.
		if wrappedExactError(branch, ret, v, errv) {
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
		if !ok {
			continue
		}
		if !cell.Heap && deferCellReturnsExact(branch, cell, errv) {
			return true
		}
		// A constructor that registered its own cleanup before failing and
		// composes the cleanup's error with that failure still returns the
		// failure: the named result holds the wrap and the deferred composition
		// takes the cell's own error as its primary. The cell and the one
		// closure that captures it are both proven below.
		if deferredCleanupReturn(branch, cell, errv) {
			return true
		}
	}
	return false
}

// deferCellReturnsExact reports whether branch stores exactly errv into a local
// error result cell and returns it after RunDefers, holding nothing else.
func deferCellReturnsExact(branch *ssa.BasicBlock, cell *ssa.Alloc, errv ssa.Value) bool {
	if cell.Referrers() == nil {
		return false
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
	return safe && stored
}

// deferredCleanupReturn reports whether branch fails with errv through the named
// result cell of a constructor whose deferred cleanup composes the cell's own
// error with the cleanup's. The branch's only write to the cell is the trusted
// wrap of exactly errv (or errv itself); every other write in it clears a
// sibling result to nil; the deferred calls run after that write and the cell is
// loaded for the return afterwards. Any other instruction -- a call, a store
// into state, a second store of the cell -- is an effect the branch decides, not
// propagation.
func deferredCleanupReturn(branch *ssa.BasicBlock, cell *ssa.Alloc, errv ssa.Value) bool {
	closure, capture, ok := deferredCleanupClosure(cell, branch)
	if !ok {
		return false
	}
	allowed := map[ssa.Instruction]bool{}
	stores := 0
	for _, ins := range branch.Instrs {
		store, ok := ins.(*ssa.Store)
		if !ok || store.Addr != cell {
			continue
		}
		stores++
		value, ok := store.Val.(*ssa.Call)
		if !ok {
			if store.Val != errv {
				return false
			}
			continue
		}
		if allowed, ok = wrappedErrorCall(value, errv); !ok {
			return false
		}
	}
	if stores != 1 {
		return false
	}
	stored, rundefers, returned := -1, -1, -1
	for i, ins := range branch.Instrs {
		if allowed[ins] {
			continue // the wrap's own argument setup
		}
		switch ins := ins.(type) {
		case *ssa.Store:
			if ins.Addr == cell {
				stored = i
				continue
			}
			ret, ok := branch.Instrs[len(branch.Instrs)-1].(*ssa.Return)
			if !ok || !clearsSiblingResult(ins, ret, closure, cell) {
				return false
			}
		case *ssa.RunDefers:
			if rundefers >= 0 {
				return false
			}
			rundefers = i
		case *ssa.Return:
			if returned >= 0 {
				return false
			}
			returned = i
		case *ssa.UnOp:
			if ins.Op != token.MUL {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	if stored < 0 || rundefers < 0 || returned != len(branch.Instrs)-1 {
		return false
	}
	if stored > rundefers || rundefers > returned {
		return false
	}
	for ins := range allowed {
		if ins.Block() != branch {
			return false
		}
	}
	return cleanupComposes(closure, capture)
}

// clearsSiblingResult permits only the nil setup of a sibling return value.
// An arbitrary local allocation can escape into shared state; being an Alloc
// is therefore insufficient. The result address may be captured only by this
// same verified deferred cleanup, never passed or published elsewhere.
func clearsSiblingResult(store *ssa.Store, ret *ssa.Return, cleanup *ssa.Function, errorCell *ssa.Alloc) bool {
	value, ok := store.Val.(*ssa.Const)
	if !ok || !value.IsNil() {
		return false
	}
	cell, ok := store.Addr.(*ssa.Alloc)
	if !ok || cell == errorCell || cell.Referrers() == nil {
		return false
	}
	returned := false
	for _, result := range ret.Results {
		if load, ok := result.(*ssa.UnOp); ok && load.Op == token.MUL && load.X == cell {
			returned = true
		}
	}
	if !returned {
		return false
	}
	for _, ref := range *cell.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Store:
			if ref.Addr != cell {
				return false
			}
		case *ssa.UnOp:
			if ref.Op != token.MUL || ref.X != cell {
				return false
			}
		case *ssa.MakeClosure:
			if ref.Fn != cleanup {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// deferredCleanupClosure returns the one closure that captures the error result
// cell, and the index of that capture. The cell's other uses are stores and
// loads only; the capture's only use is a defer instruction in the cell's own
// function, and that defer dominates branch, so it runs on every path the
// branch returns from.
func deferredCleanupClosure(cell *ssa.Alloc, branch *ssa.BasicBlock) (*ssa.Function, int, bool) {
	if cell.Referrers() == nil || branch.Parent() == nil {
		return nil, 0, false
	}
	pointer, ok := cell.Type().Underlying().(*types.Pointer)
	if !ok || !types.Identical(pointer.Elem(), types.Universe.Lookup("error").Type()) {
		return nil, 0, false // only an error result cell holds the failure
	}
	var closure *ssa.Function
	capture := -1
	for _, ref := range *cell.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Store:
			if ref.Addr != cell {
				return nil, 0, false
			}
		case *ssa.UnOp:
			if ref.Op != token.MUL {
				return nil, 0, false
			}
		case *ssa.MakeClosure:
			if closure != nil {
				return nil, 0, false
			}
			captured, ok := ref.Fn.(*ssa.Function)
			if !ok || !deferredBefore(ref, branch) {
				return nil, 0, false
			}
			for i, binding := range ref.Bindings {
				if binding != cell {
					continue
				}
				if capture >= 0 {
					return nil, 0, false
				}
				capture = i
			}
			if capture < 0 || capture >= len(captured.FreeVars) || !types.Identical(captured.FreeVars[capture].Type(), cell.Type()) {
				return nil, 0, false
			}
			closure = captured
		case *ssa.DebugRef:
		default:
			return nil, 0, false
		}
	}
	if closure == nil {
		return nil, 0, false
	}
	return closure, capture, true
}

// deferredBefore reports whether mc is registered as a deferred call in the
// branch's function and that defer dominates branch: nothing else uses mc, the
// defer calls exactly it, and it is already registered on every path to branch.
func deferredBefore(mc *ssa.MakeClosure, branch *ssa.BasicBlock) bool {
	if mc.Referrers() == nil || mc.Block() == nil {
		return false
	}
	deferred := false
	for _, ref := range *mc.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Defer:
			if deferred || ref.Common().Value != ssa.Value(mc) || ref.Block() == nil || ref.Block().Parent() != branch.Parent() {
				return false
			}
			deferred = true
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return deferred && mc.Block().Dominates(branch)
}

// cleanupComposes proves the deferred closure's use of the captured error
// result: every load of it is the closure's own nil test or the primary failure
// of the composition owner's Outcomes, exactly one store is that composition's
// result, and every other store happens only where the closure's own nil test
// found the cell empty, so no store replaces a failure the constructor already
// returned.
func cleanupComposes(fn *ssa.Function, capture int) bool {
	if capture < 0 || capture >= len(fn.FreeVars) {
		return false
	}
	cell := fn.FreeVars[capture]
	if cell.Referrers() == nil {
		return false
	}
	composed := false
	for _, ref := range *cell.Referrers() {
		switch ref := ref.(type) {
		case *ssa.UnOp:
			if ref.Op != token.MUL || !cleanupRead(ref) {
				return false
			}
		case *ssa.Store:
			if ref.Addr != cell {
				return false
			}
			if composedValue(ref.Val, cell) {
				if composed {
					return false
				}
				composed = true
				continue
			}
			if !nilGuardedStore(fn, cell, ref.Block()) {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return composed
}

// cleanupRead reports whether every use of a load of the captured error result
// is only a nil test of it or the primary failure of a composition.
func cleanupRead(load *ssa.UnOp) bool {
	if load.Referrers() == nil {
		return false
	}
	value := ssa.Value(load)
	for _, ref := range *load.Referrers() {
		switch ref := ref.(type) {
		case *ssa.BinOp:
			tested, _, ok := workcensus.NilComparison(ref)
			if !ok || tested != value || ref.Referrers() == nil {
				return false
			}
			for _, use := range *ref.Referrers() {
				switch use.(type) {
				case *ssa.If, *ssa.DebugRef:
				default:
					return false
				}
			}
		case *ssa.Call:
			if !composedPrimary(ref, value) {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// composedPrimary reports whether call is the composition owner's Outcomes with
// primary as its primary failure.
func composedPrimary(call *ssa.Call, primary ssa.Value) bool {
	if !trustedOutcomes(call.Common()) {
		return false
	}
	args := call.Common().Args
	return len(args) > 0 && args[0] == primary
}

// composedValue reports whether v is the composition owner's Outcomes of the
// cell's own error as its primary failure.
func composedValue(v ssa.Value, cell ssa.Value) bool {
	call, ok := v.(*ssa.Call)
	if !ok || !trustedOutcomes(call.Common()) {
		return false
	}
	args := call.Common().Args
	if len(args) == 0 {
		return false
	}
	primary, ok := args[0].(*ssa.UnOp)
	return ok && primary.Op == token.MUL && primary.X == cell
}

// nilGuardedStore reports whether block is reached only through the arm of a
// dominating branch that tested this very cell against nil and found it empty.
func nilGuardedStore(fn *ssa.Function, cell ssa.Value, block *ssa.BasicBlock) bool {
	for _, dominating := range fn.Blocks {
		if dominating == block || len(dominating.Instrs) == 0 {
			continue
		}
		iff, ok := dominating.Instrs[len(dominating.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		tested, nilIndex, ok := workcensus.NilComparison(iff.Cond)
		if !ok {
			continue
		}
		load, ok := tested.(*ssa.UnOp)
		if !ok || load.Op != token.MUL || load.X != cell {
			continue
		}
		if dominating.Succs[nilIndex].Dominates(block) {
			return true
		}
	}
	return false
}

// trustedErrorf is the standard library's contextual error constructor called
// through its imported identity: package path fmt and name Errorf, with the
// format-string, variadic-interface signature fmt.Errorf declares. A
// same-shaped local wrapper is not it.
func trustedErrorf(call *ssa.CallCommon) bool {
	if call.IsInvoke() {
		return false
	}
	callee := call.StaticCallee()
	if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg == nil || callee.Pkg.Pkg.Path() != "fmt" || callee.Name() != "Errorf" {
		return false
	}
	sig := callee.Signature
	if sig == nil || !sig.Variadic() || sig.Params().Len() != 2 || sig.Results().Len() != 1 {
		return false
	}
	if format, ok := sig.Params().At(0).Type().Underlying().(*types.Basic); !ok || format.Kind() != types.String {
		return false
	}
	spread, ok := sig.Params().At(1).Type().(*types.Slice)
	if !ok {
		return false
	}
	element, ok := spread.Elem().Underlying().(*types.Interface)
	return ok && element.NumMethods() == 0 && types.Identical(sig.Results().At(0).Type(), types.Universe.Lookup("error").Type())
}

// composeOutcomesPath is the module's error-composition owner. Its Outcomes
// keeps the primary failure first: it skips a cleanup failure the primary
// already wraps, returns a lone cause unchanged and joins several, so a
// composition still carries the primary in its causal chain. The census trusts
// that owner by identity -- this module path, the name Outcomes and its
// (primary error, others ...error) error signature -- exactly as it trusts the
// standard library's fmt.Errorf, never a bare spelling; the contract itself is
// exercised against the real implementation in
// cleanup_error_propagation_test.go.
const composeOutcomesPath = Module + "pkg/errors/compose"

// trustedOutcomes reports whether call is the composition owner's Outcomes,
// called through its imported identity. A same-shaped local helper, another
// package's Outcomes or a wrapper around it is not it.
func trustedOutcomes(call *ssa.CallCommon) bool {
	if call.IsInvoke() {
		return false
	}
	callee := call.StaticCallee()
	if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg == nil {
		return false
	}
	if callee.Pkg.Pkg.Path() != composeOutcomesPath || callee.Name() != "Outcomes" {
		return false
	}
	sig := callee.Signature
	errorType := types.Universe.Lookup("error").Type()
	if sig == nil || !sig.Variadic() || sig.Params().Len() != 2 || sig.Results().Len() != 1 {
		return false
	}
	if !types.Identical(sig.Params().At(0).Type(), errorType) {
		return false
	}
	others, ok := sig.Params().At(1).Type().(*types.Slice)
	if !ok || !types.Identical(others.Elem(), errorType) {
		return false
	}
	return types.Identical(sig.Results().At(0).Type(), errorType)
}

// wrappedErrorCall reports whether call is the trusted fmt.Errorf wrapping
// exactly errv, and returns the instructions that call lowers to. The format
// holds one %w verb, errv is the sole formatted value after interface
// conversion, and the variadic array holding it is local to this call: one
// element store, the call's own slice, and no other write, read, escape or use.
func wrappedErrorCall(call *ssa.Call, errv ssa.Value) (map[ssa.Instruction]bool, bool) {
	if !trustedErrorf(call.Common()) || len(call.Common().Args) != 2 {
		return nil, false
	}
	format, ok := call.Common().Args[0].(*ssa.Const)
	if !ok || format.Value == nil || format.Value.Kind() != constant.String {
		return nil, false
	}
	// The proof accepts only a single %w format; other formatting shapes
	// remain unproven and are rejected conservatively.
	if text := constant.StringVal(format.Value); strings.Count(text, "%") != 1 || !strings.Contains(text, "%w") {
		return nil, false
	}
	spread, ok := call.Common().Args[1].(*ssa.Slice)
	if !ok || spread.Low != nil || spread.High != nil || spread.Max != nil {
		return nil, false
	}
	array, ok := spread.X.(*ssa.Alloc)
	if !ok {
		return nil, false
	}
	pointer, ok := array.Type().Underlying().(*types.Pointer)
	if !ok {
		return nil, false
	}
	argumentArray, ok := pointer.Elem().Underlying().(*types.Array)
	if !ok || argumentArray.Len() != 1 {
		return nil, false
	}
	var index *ssa.IndexAddr
	for _, ref := range *array.Referrers() {
		switch ref := ref.(type) {
		case *ssa.IndexAddr:
			if index != nil || ref.X != array {
				return nil, false
			}
			index = ref
		case *ssa.Slice:
			if ref != spread {
				return nil, false
			}
		case *ssa.DebugRef:
		default:
			return nil, false
		}
	}
	if index == nil || index.Referrers() == nil || len(*index.Referrers()) != 1 {
		return nil, false
	}
	position, ok := index.Index.(*ssa.Const)
	if !ok || position.Value == nil || position.Value.Kind() != constant.Int || constant.Sign(position.Value) != 0 {
		return nil, false
	}
	store, ok := (*index.Referrers())[0].(*ssa.Store)
	if !ok || store.Addr != index {
		return nil, false
	}
	var conversion ssa.Instruction
	var converted ssa.Value
	switch value := store.Val.(type) {
	case *ssa.ChangeInterface:
		if value.X != errv {
			return nil, false
		}
		conversion, converted = value, value
	case *ssa.MakeInterface:
		if value.X != errv {
			return nil, false
		}
		conversion, converted = value, value
	default:
		return nil, false
	}
	for _, ref := range *converted.Referrers() {
		if ref == store {
			continue
		}
		if _, ok := ref.(*ssa.DebugRef); !ok {
			return nil, false
		}
	}
	for _, ref := range *spread.Referrers() {
		if ref == call {
			continue
		}
		if _, ok := ref.(*ssa.DebugRef); !ok {
			return nil, false
		}
	}
	return map[ssa.Instruction]bool{array: true, index: true, store: true, conversion: true, spread: true, call: true}, true
}

// wrappedExactError reports whether branch returns v, the wrap wrappedErrorCall
// proves. The branch holds nothing besides that argument setup, the call, the
// return, debug references and the aggregate result transfer a return already
// allows.
func wrappedExactError(branch *ssa.BasicBlock, ret *ssa.Return, v, errv ssa.Value) bool {
	call, ok := v.(*ssa.Call)
	if !ok {
		return false
	}
	allowed, ok := wrappedErrorCall(call, errv)
	if !ok {
		return false
	}
	present := map[ssa.Instruction]bool{}
	for _, ins := range branch.Instrs {
		present[ins] = true
		if allowed[ins] {
			continue
		}
		switch ins.(type) {
		case *ssa.Return, *ssa.DebugRef:
			continue
		}
		if !returnedAggregateTransfer(ins, ret) {
			return false
		}
	}
	for ins := range allowed {
		if !present[ins] {
			return false
		}
	}
	return true
}

// returnedAggregateTransfer recognizes only the local aggregate result's
// return-time load and identity self-copy. SSA lowers `return r, err` for a
// captured named r into load/store/load even though the source returns r
// unchanged. All other stores, calls, pointer reads and defer execution remain
// effects; in particular, an error-result cell is not an aggregate.
func returnedAggregateTransfer(ins ssa.Instruction, ret *ssa.Return) bool {
	var cell ssa.Value
	switch value := ins.(type) {
	case *ssa.UnOp:
		if value.Op != token.MUL {
			return false
		}
		cell = value.X
	case *ssa.Store:
		loaded, ok := value.Val.(*ssa.UnOp)
		if !ok || loaded.Op != token.MUL || loaded.X != value.Addr || loaded.Block() != ret.Block() {
			return false
		}
		cell = value.Addr
	default:
		return false
	}
	if _, ok := cell.(*ssa.Alloc); !ok {
		return false
	}
	for _, result := range ret.Results {
		loaded, ok := result.(*ssa.UnOp)
		if !ok || loaded.Op != token.MUL || loaded.X != cell {
			continue
		}
		switch loaded.Type().Underlying().(type) {
		case *types.Struct, *types.Array:
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
