package workcensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"fmt"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// MayBeNil reports whether value may be nil where at uses it, with a witness
// path (nil when it cannot). It follows SSA def/use within functions,
// parameters to their visible call sites, closure free variables to their
// bindings, phi arms, the returns of static callees, local cells, and struct
// fields to every store and to every allocation that leaves the field zero. A
// value used only after the non-nil edge of its own nil test is non-nil there.
// Results of external or context-package code, and of calls with no visible
// target, are non-nil (Go's non-nil interface convention); a nil visible behind
// a static, interface or function-value call is never assumed away, except on
// an error return when the use is reached only after that error tested nil,
// a lazily initialized field or cell, and a Recover block no deferred call can
// enter.
func (p *Program) MayBeNil(value ssa.Value, at ssa.Instruction) []string {
	witness, _ := p.MayBeNilWhy(value, at)
	return witness
}

// MayBeNilWhy is MayBeNil with the reasons for a non-nil verdict: every proof
// rule (a nil test, a checked error, a non-nilable type, a cycle) and every
// model convention (an external or unresolved result, a parameter with no
// visible caller, an unmodelled value) that removed a path to nil. A non-nil
// verdict with no reason is one the model reached without deciding anything.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func (p *Program) MayBeNilWhy(value ssa.Value, at ssa.Instruction) (witness, why []string) {
	if p.nilFlow == nil {
		p.nilFlow = newNilFlow(p)
	}
	f := p.nilFlow
	f.trail = f.trail[:0]
	witness = f.use(value, at)
	if witness == nil {
		why = dedupe(f.trail)
	}
	return witness, why
}

// discharge records one reason a path to nil was removed.
func (f *nilFlow) discharge(rule string) { f.trail = append(f.trail, rule) }

func dedupe(rules []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, rule := range rules {
		if !seen[rule] {
			seen[rule] = true
			out = append(out, rule)
		}
	}
	sort.Strings(out)
	return out
}

func newNilFlow(p *Program) *nilFlow {
	return &nilFlow{p: p, values: map[ssa.Value][]string{}, fields: map[*types.Var][]string{}, active: map[any]bool{}, recovers: map[*ssa.Function]bool{}, why: map[ssa.Value][]string{}, fieldWhy: map[*types.Var][]string{}, external: map[string]string{}, bodies: map[string]map[string]*ssa.Function{}, sourcePackages: map[string]*ssa.Package{}, globalInitializers: map[*ssa.Global]*globalInitializer{}}
}

type nilFlow struct {
	errorPreservation map[*ssa.UnOp]bool // source-verified exact deferred error loads, never caller-conditioned values

	p                   *Program
	values              map[ssa.Value][]string
	fields              map[*types.Var][]string
	active              map[any]bool
	recovers            map[*ssa.Function]bool
	why                 map[ssa.Value][]string                // the reasons behind each cached non-nil verdict
	fieldWhy            map[*types.Var][]string               // the same for field verdicts
	trail               []string                              // reasons recorded during the current query
	external            map[string]string                     // deferred callee -> verdict from its package's source
	bodies              map[string]map[string]*ssa.Function   // package path -> its functions, loaded on demand
	sourcePackages      map[string]*ssa.Package               // same loaded source packages, including globals/init
	globalInitializers  map[*ssa.Global]*globalInitializer    // source facts only; phase stays per-load
	initializationOrder map[*ssa.Store]map[*ssa.Function]bool // proven prefix order per exact initializer/read owner
}

// use applies the guard at this use, then the value's own may-nil result.
func (f *nilFlow) use(v ssa.Value, at ssa.Instruction) []string {
	if v == nil {
		return nil
	}
	if at != nil && NonNilAt(v, at.Block()) {
		f.discharge("proof: nil-test guard")
		return nil
	}
	if f.assertionNonNilAt(v, at) {
		f.discharge("proof: assertion-call normal-return postcondition at use")
		return nil
	}
	// Go's (value, error) convention: a result used only after its sibling
	// error tested nil comes from the callee's success returns.
	if x, ok := v.(*ssa.Extract); ok && at != nil {
		if call, ok := x.Tuple.(*ssa.Call); ok && errorChecked(call, at.Block()) {
			return f.resultOf(call, x.Index, true)
		}
	}
	if phi, ok := v.(*ssa.Phi); ok && at != nil && at.Block() != nil {
		pruned := false
		for _, pred := range phi.Block().Preds {
			pruned = pruned || edgeContradictsStableUse(pred, phi.Block(), at.Block())
		}
		if pruned {
			key := struct {
				value *ssa.Phi
				use   ssa.Instruction
			}{phi, at}
			if f.active[key] {
				f.discharge("proof: use-local phi cycle adds no new source")
				return nil
			}
			f.active[key] = true
			defer delete(f.active, key)
			feasible := 0
			for i, arm := range phi.Edges {
				pred := phi.Block().Preds[i]
				if edgeContradictsStableUse(pred, phi.Block(), at.Block()) {
					continue
				}
				feasible++
				if nonNilEdge(arm, pred, phi.Block()) {
					continue
				}
				if w := f.use(arm, pred.Instrs[len(pred.Instrs)-1]); w != nil {
					return w
				}
			}
			if feasible == 0 {
				return f.value(phi)
			}
			f.discharge("proof: phi edges contradicting the same stable predicate excluded at use")
			return nil
		}
	}
	return f.value(v)
}

// callSuccess returns the If of call's trailing error test and the block that
// runs only where that result is nil, from the engine's one call-success
// analysis (SuccessfulCallEdge, which also follows the error through a cell it
// is stored in, only while that cell still holds it), or nil, nil.
func callSuccess(call *ssa.Call) (*ssa.If, *ssa.BasicBlock) {
	tuple, ok := call.Type().(*types.Tuple)
	if !ok || tuple.Len() < 2 || !types.Identical(tuple.At(tuple.Len()-1).Type(), types.Universe.Lookup("error").Type()) {
		return nil, nil
	}
	return SuccessfulCallEdge(call, tuple.Len()-1)
}

// errorChecked reports that block runs only where call's trailing error result
// is nil: the test's block and its success successor both dominate block, so
// every path there took the success edge. A success block that another
// predecessor can enter without the test proves nothing, not even at itself.
func errorChecked(call *ssa.Call, block *ssa.BasicBlock) bool {
	iff, success := callSuccess(call)
	if iff == nil || success == nil || block == nil || len(success.Preds) != 1 {
		return false
	}
	return iff.Block().Dominates(block) && success.Dominates(block)
}

// assertionNonNilAt is deliberately use-site dependent: an earlier visible
// synchronous call must dominate this use and pass this exact SSA value to a
// parameter tested non-nil on every normal return of every possible target.
// It never updates the value memo: a use before the assertion may still be nil.
func (f *nilFlow) assertionNonNilAt(value ssa.Value, use ssa.Instruction) bool {
	if value == nil || use == nil || use.Block() == nil || value.Referrers() == nil {
		return false
	}
	for _, ref := range *value.Referrers() {
		call, ok := ref.(*ssa.Call)
		if !ok || call == use || call.Block() == nil || call.Parent() != use.Parent() || !call.Block().Dominates(use.Block()) {
			continue
		}
		if call.Block() == use.Block() && instructionIndex(call) >= instructionIndex(use) {
			continue
		}
		for index, arg := range call.Common().Args {
			if arg != value {
				continue
			}
			targets := f.p.CallTargets(call)
			proven := len(targets) > 0
			for _, target := range targets {
				i := index + len(target.Params) - len(call.Common().Args)
				if len(target.Blocks) == 0 || i < 0 || i >= len(target.Params) || !types.AssignableTo(value.Type(), target.Params[i].Type()) {
					proven = false
					break
				}
				returns := 0
				for _, block := range target.Blocks {
					if _, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); !ok {
						continue
					}
					returns++
					if !NonNilAt(target.Params[i], block) {
						proven = false
						break
					}
				}
				if returns == 0 || !proven {
					proven = false
					break
				}
			}
			if proven {
				return true
			}
		}
	}
	return false
}

// sameLocation reports two addresses of one variable: the same cell, or the
// same field of the same base object (cellObject).
func sameLocation(a, b ssa.Value) bool {
	fa, okA := a.(*ssa.FieldAddr)
	fb, okB := b.(*ssa.FieldAddr)
	if okA || okB {
		return okA && okB && sameStableObject(fa.X, fb.X, 0) && fa.Field == fb.Field
	}
	return a == b
}

// sameStableObject compares SSA object identities, including a source-proven
// stable re-read. Struct projections are immutable SSA values. Pointer loads
// additionally require an acyclic, mutation/call-free interval on every path;
// sharing a field name or a type never supplies object identity.
func sameStableObject(a, b ssa.Value, depth int) bool {
	if depth > 12 {
		return false
	}
	a, b = cellObject(a), cellObject(b)
	if a == b {
		return true
	}
	if a == nil || b == nil || a.Parent() != b.Parent() {
		return false
	}
	if x, ok := a.(*ssa.Field); ok {
		y, ok := b.(*ssa.Field)
		return ok && x.Field == y.Field && sameStableObject(x.X, y.X, depth+1)
	}
	x, ok := a.(*ssa.UnOp)
	y, other := b.(*ssa.UnOp)
	if !ok || !other || x.Op != token.MUL || y.Op != token.MUL {
		return false
	}
	if !sameStableAddress(x.X, y.X, depth+1) {
		return false
	}
	return stableLoadInterval(x, y) || stableLoadInterval(y, x)
}

func sameStableAddress(a, b ssa.Value, depth int) bool {
	if a == b {
		return true
	}
	x, ok := a.(*ssa.FieldAddr)
	y, other := b.(*ssa.FieldAddr)
	return ok && other && x.Field == y.Field && sameStableObject(x.X, y.X, depth+1)
}

// stableLoadInterval rejects unknown calls, stores (including aliases/whole
// objects), escapes, deferred execution and cycles. This deliberately does not
// guess which writes are harmless; a future relaxation needs its own proof.
func stableLoadInterval(first, last *ssa.UnOp) bool {
	if first.Parent() != last.Parent() || first.Block() == nil || last.Block() == nil || !first.Block().Dominates(last.Block()) {
		return false
	}
	active := map[*ssa.BasicBlock]bool{}
	done := map[*ssa.BasicBlock]bool{}
	var walk func(*ssa.BasicBlock, int) bool
	walk = func(block *ssa.BasicBlock, end int) bool {
		if active[block] {
			return false
		}
		if done[block] {
			return true
		}
		active[block] = true
		defer delete(active, block)
		for i := end - 1; i >= 0; i-- {
			ins := block.Instrs[i]
			if ins == first {
				return true
			}
			switch ins := ins.(type) {
			case *ssa.Store, *ssa.Go, *ssa.RunDefers, *ssa.Defer, *ssa.MakeClosure, *ssa.Send, *ssa.Select, *ssa.MapUpdate:
				return false
			case *ssa.UnOp:
				if ins.Op == token.ARROW {
					return false
				}
			case *ssa.Call:
				// Builtins such as copy/clear/append can overwrite aliased storage.
				return false
			}
		}
		if len(block.Preds) == 0 {
			return false
		}
		for _, pred := range block.Preds {
			if !walk(pred, len(pred.Instrs)) {
				return false
			}
		}
		done[block] = true
		return true
	}
	for i, ins := range last.Block().Instrs {
		if ins == last {
			return walk(last.Block(), i)
		}
	}
	return false
}

// cellObject names the object a value refers to. A load of an immutable cell
// (one store that dominates the load, ImmutableCellValueAt) names the stored
// value: a variable captured by a closure lives in such a cell, and each use
// loads it again, but every load reads the same object.
func cellObject(v ssa.Value) ssa.Value {
	for depth := 0; depth < 8; depth++ {
		load, ok := v.(*ssa.UnOp)
		if !ok || load.Op != token.MUL {
			return v
		}
		stored, ok := ImmutableCellValueAt(load.X, load)
		if !ok {
			return v
		}
		v = stored
	}
	return v
}

// recoverReachable reports whether fn's Recover block can run: some deferred
// call may recover a panic and return normally. It is unreachable when every
// deferred call has a static callee which either cannot reach recover or
// cannot finish normally after recovering a non-nil panic. A deferred interface call,
// a function value without a static callee, or a call in that walk with no
// visible target counts as possibly recovering. A deferred callee from a
// package loaded without bodies is judged by externalMayRecover.
func (f *nilFlow) recoverReachable(fn *ssa.Function) bool {
	if fn.Recover == nil {
		return false
	}
	if live, done := f.recovers[fn]; done {
		return live
	}
	live := false
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			deferred, ok := ins.(*ssa.Defer)
			if !ok {
				continue
			}
			callee := deferred.Common().StaticCallee()
			if callee == nil {
				live = true
				continue
			}
			if len(callee.Blocks) == 0 {
				if f.externalMayRecover(callee) {
					live = true
				}
				continue
			}
			if !recoveryCannotReturn(callee) && f.mayCallRecover(callee, map[*ssa.Function]bool{}) {
				live = true
			}
		}
	}
	f.recovers[fn] = live
	return live
}

// recoveryCannotReturn proves that a deferred function cannot finish normally
// after directly recovering a non-nil panic. Unknown callees and cell escapes
// do not supply this proof. A captured cause may only retain the original cause
// or be replaced by a value strictly tested non-nil before its store.
func recoveryCannotReturn(fn *ssa.Function) bool {
	var recovered []*ssa.Call
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			if call, ok := ins.(*ssa.Call); ok {
				if builtin, ok := call.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "recover" {
					recovered = append(recovered, call)
				}
			}
		}
	}
	if len(recovered) == 0 {
		return false
	}
	for _, cause := range recovered {
		var cell *ssa.Alloc
		var initial *ssa.Store
		if cause.Referrers() != nil {
			for _, ref := range *cause.Referrers() {
				if store, ok := ref.(*ssa.Store); ok && store.Val == cause {
					candidate, ok := store.Addr.(*ssa.Alloc)
					if !ok || cell != nil && cell != candidate {
						return false
					}
					cell = candidate
					initial = store
				}
			}
		}
		if cell != nil {
			if !localCell(cell) {
				return false
			}
			stores := capturedStores(cell)
			for _, ref := range *cell.Referrers() {
				if store, ok := ref.(*ssa.Store); ok && store.Addr == cell {
					stores = append(stores, store)
				}
			}
			for _, store := range stores {
				if store.Val != cause && !NonNilAt(store.Val, store.Block()) {
					return false
				}
			}
		}
		for _, block := range fn.Blocks {
			if _, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); !ok {
				continue
			}
			guarded := NilAt(cause, block)
			if !guarded && cell != nil {
				for _, ref := range *cell.Referrers() {
					if load, ok := ref.(*ssa.UnOp); ok && load.Op == token.MUL && load.X == cell && initial != nil && initial.Block().Dominates(load.Block()) && (initial.Block() != load.Block() || instructionIndex(initial) < instructionIndex(load)) && NilAt(load, block) {
						guarded = true
					}
				}
			}
			if !guarded {
				return false
			}
		}
	}
	return true
}

// externalMayRecover judges a deferred callee whose package was loaded without
// bodies (a dependency such as sync): it loads that package's source on its own,
// with the program's build settings, and reports whether the callee's body calls
// the builtin recover. Go stops a panic only in a recover called directly by the
// deferred function, so that body is the one that matters. A callee that still
// has no body (assembly), or that cannot be loaded or found, may recover. Each
// verdict is recorded in ExternalDeferred.
func (f *nilFlow) externalMayRecover(callee *ssa.Function) bool {
	key := callee.String()
	if verdict, done := f.external[key]; done {
		return verdict != "no recover call in its body"
	}
	verdict := "no loadable body"
	if body := f.externalBody(callee); body != nil {
		verdict = "no recover call in its body"
		for _, block := range body.Blocks {
			for _, ins := range block.Instrs {
				if call, ok := ins.(ssa.CallInstruction); ok {
					if builtin, ok := call.Common().Value.(*ssa.Builtin); ok && builtin.Name() == "recover" {
						verdict = "calls recover"
					}
				}
			}
		}
	}
	f.external[key] = verdict
	return verdict != "no recover call in its body"
}

// externalBody returns the built body of callee from a separate source load of
// its package, or nil.
func (f *nilFlow) externalBody(callee *ssa.Function) *ssa.Function {
	obj := callee.Object()
	if obj == nil || obj.Pkg() == nil {
		return nil
	}
	path := obj.Pkg().Path()
	f.sourcePackage(path)
	if body := f.bodies[path][callee.String()]; body != nil && len(body.Blocks) > 0 {
		return body
	}
	return nil
}

// sourcePackage shares the existing on-demand source load for external bodies
// and global initializers. It uses the original loader, overlay and parsed-byte
// capture; failed loads remain unknown rather than supplying a non-nil fact.
func (f *nilFlow) sourcePackage(path string) *ssa.Package {
	if _, loaded := f.bodies[path]; !loaded {
		f.bodies[path] = map[string]*ssa.Function{}
		cfg := f.p.load
		cfg.Mode = packages.LoadSyntax
		cfg.Fset = token.NewFileSet()
		pkgs, err := packages.Load(&cfg, path)
		if err != nil || len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
			return nil
		}
		prog, built := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
		prog.Build()
		if len(built) != 1 || built[0] == nil {
			return nil
		}
		f.sourcePackages[path] = built[0]
		for fn := range sourceFunctions(prog, pkgs) {
			if fn.Pkg != nil && fn.Pkg.Pkg.Path() == path {
				f.bodies[path][fn.String()] = fn
			}
		}
	}
	return f.sourcePackages[path]
}

// ExternalDeferred lists each deferred callee judged from a separate source load
// of its package, with its verdict.
func (p *Program) ExternalDeferred() map[string]string {
	if p.nilFlow == nil {
		return nil
	}
	return p.nilFlow.external
}

func (f *nilFlow) mayCallRecover(fn *ssa.Function, seen map[*ssa.Function]bool) bool {
	if seen[fn] {
		return false
	}
	seen[fn] = true
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			call, ok := ins.(ssa.CallInstruction)
			if !ok {
				continue
			}
			if builtin, ok := call.Common().Value.(*ssa.Builtin); ok {
				if builtin.Name() == "recover" {
					return true
				}
				continue
			}
			targets := f.p.CallTargets(call)
			if len(targets) == 0 {
				return true // an unresolved call may recover
			}
			for _, target := range targets {
				if f.mayCallRecover(target, seen) {
					return true
				}
			}
		}
	}
	return false
}

// nonNilEdge reports that arm reaches succ from pred only along the non-nil
// edge of arm's own nil test, as in "if ctx == nil { ctx = ... }".
func nonNilEdge(arm ssa.Value, pred, succ *ssa.BasicBlock) bool {
	if arm.Referrers() == nil {
		return false
	}
	iff, nilSucc := NilSuccessEdge(arm)
	return iff != nil && iff.Block() == pred && succ != nilSucc
}

func (f *nilFlow) position(fn *ssa.Function, pos token.Pos) string {
	if fn == nil || !pos.IsValid() {
		return ""
	}
	at := f.p.FileSet.PositionFor(pos, false)
	return fmt.Sprintf("%s:%d", at.Filename, at.Line)
}

func (f *nilFlow) value(v ssa.Value) (witness []string) {
	if w, done := f.values[v]; done {
		if w == nil {
			f.trail = append(f.trail, f.why[v]...)
		}
		return w
	}
	if f.active[v] {
		f.discharge("proof: cycle adds no new source")
		return nil // a cycle adds no new source of nil
	}
	f.active[v] = true
	start := len(f.trail)
	defer func() {
		delete(f.active, v)
		f.values[v] = witness
		if witness == nil {
			f.why[v] = dedupe(f.trail[start:])
		}
	}()
	switch v := v.(type) {
	case *ssa.Const:
		if v.IsNil() {
			if at := f.position(v.Parent(), v.Pos()); at != "" {
				return []string{"nil " + v.Type().String() + " at " + at}
			}
			return []string{"nil " + v.Type().String()}
		}
		return nil // a non-nil constant
	case *ssa.Parameter:
		return f.parameter(v)
	case *ssa.FreeVar:
		return f.freeVar(v)
	case *ssa.Phi:
		for i, arm := range v.Edges {
			pred := v.Block().Preds[i]
			if nonNilEdge(arm, pred, v.Block()) {
				f.discharge("proof: nil-test edge")
				continue
			}
			if w := f.use(arm, pred.Instrs[len(pred.Instrs)-1]); w != nil {
				return w
			}
		}
	case *ssa.Call:
		return f.result(v, -1)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return f.result(call, v.Index)
		}
		if lookup, ok := v.Tuple.(*ssa.Lookup); ok && v.Index == 0 {
			return []string{"absent map value at " + f.position(lookup.Parent(), lookup.Pos())}
		}
	case *ssa.Lookup:
		return []string{"absent map value at " + f.position(v.Parent(), v.Pos())}
	case *ssa.ChangeInterface:
		return f.value(v.X)
	case *ssa.ChangeType:
		return f.value(v.X)
	case *ssa.MakeInterface:
		if structOf(v.X.Type()) != nil && !isPointer(v.X.Type()) {
			return f.embedded(v.X)
		}
		if !nilable(v.X.Type()) {
			f.discharge("proof: non-nilable value")
			return nil // an interface holding a non-nilable value is never nil
		}
		return f.value(v.X)
	case *ssa.Field:
		return f.projectField(v.X, structField(v.X.Type(), v.Field))
	case *ssa.UnOp:
		if v.Op == token.MUL {
			return f.load(v)
		}
		f.discharge("convention: unmodelled value " + v.Op.String())
		return nil
	case *ssa.Alloc, *ssa.MakeClosure, *ssa.MakeMap, *ssa.MakeChan, *ssa.MakeSlice, *ssa.Function, *ssa.Global, *ssa.FieldAddr, *ssa.IndexAddr:
		// Structurally non-nil (an address or a made value); recorded, so
		// every non-nil verdict carries its reason.
		f.discharge(fmt.Sprintf("proof: structurally non-nil %T", v))
		return nil
	}
	f.discharge(fmt.Sprintf("convention: unmodelled value %T", v))
	return nil
}

// sourceParameterIndex resolves a parameter once before walking its attributed
// source sites. The cached ordinal is checked by sourceArgumentAt on each use.
func sourceParameterIndex(param *ssa.Parameter) int {
	for i, candidate := range param.Parent().Params {
		if candidate == param {
			return i
		}
	}
	return -1
}

// sourceArgumentAt aligns a parameter with an already-attributed source site.
// Interface/bound-method source calls may omit a receiver; compiler wrappers
// may expose an incompatible argument. This judges binding only, not target
// membership, nil semantics, or whether the call executes.
func sourceArgumentAt(param *ssa.Parameter, index int, site ssa.CallInstruction) (ssa.Value, bool) {
	params := param.Parent().Params
	if index < 0 || index >= len(params) || params[index] != param {
		return nil, false
	}
	args := site.Common().Args
	i := index - (len(params) - len(args))
	if i < 0 || i >= len(args) || !types.AssignableTo(args[i].Type(), param.Type()) {
		return nil, false
	}
	return args[i], true
}

func (f *nilFlow) parameter(param *ssa.Parameter) []string {
	fn := param.Parent()
	index := sourceParameterIndex(param)
	sites := f.p.SourceCallSites(fn)
	if len(sites) == 0 {
		f.discharge("convention: parameter without visible call sites: " + fn.String())
	}
	for _, site := range sites {
		arg, compatible := sourceArgumentAt(param, index, site)
		if !compatible {
			continue
		}
		if w := f.use(arg, site); w != nil {
			return append(w, fmt.Sprintf("-> parameter %s of %s (call at %s)", param.Name(), fn.String(), f.position(site.Parent(), site.Pos())))
		}
	}
	return nil
}

func (f *nilFlow) freeVar(free *ssa.FreeVar) []string {
	binding := freeVarBinding(free)
	if binding == nil {
		f.discharge("convention: unbound free variable of " + free.Parent().String())
		return nil
	}
	if w := f.value(binding); w != nil {
		return append(w, "-> captured "+free.Name()+" of "+free.Parent().String())
	}
	return nil
}

// result follows the returns of every call-graph target of call (the static
// callee, or the targets of an interface or function-value call), so a nil
// produced behind a dynamic call is not assumed away. Context-package and
// bodiless (external) targets produce non-nil results.
func (f *nilFlow) result(call ssa.CallInstruction, index int) []string {
	return f.resultOf(call, index, false)
}

// resultOf follows the returns of call's targets; successOnly skips a return
// whose trailing error result is not the nil constant (an error return).
func (f *nilFlow) resultOf(call ssa.CallInstruction, index int, successOnly bool) []string {
	targets := f.p.CallTargets(call)
	resultIndex := index
	if resultIndex < 0 {
		resultIndex = 0
	}
	// A reviewed result boundary is exact in function identity and tuple
	// index. A dynamic call uses it only when every candidate is covered.
	covered := len(targets) > 0
	for _, target := range targets {
		if f.p.nonNilResults[target][resultIndex] == "" {
			covered = false
			break
		}
	}
	if covered {
		for _, target := range targets {
			f.discharge(fmt.Sprintf("reviewed-result-contract: %s result #%d: %s", target.String(), resultIndex, f.p.nonNilResults[target][resultIndex]))
		}
		return nil
	}
	if len(targets) == 0 {
		f.discharge("convention: unresolved call target at " + f.position(call.Parent(), call.Pos()))
	}
	for _, callee := range targets {
		switch {
		case callee.Pkg != nil && callee.Pkg.Pkg.Path() == "context":
			f.discharge("convention: context-package result " + callee.String())
			continue
		case callee.Blocks == nil || callee.Pkg == nil:
			f.discharge("convention: external result " + callee.String())
			continue
		}
		for _, block := range callee.Blocks {
			ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
			if !ok {
				continue
			}
			if block == callee.Recover && !f.recoverReachable(callee) {
				f.discharge("proof: unreachable Recover block of " + callee.String())
				continue
			}
			i := index
			if i < 0 {
				i = 0
			}
			if i >= len(ret.Results) {
				continue
			}
			// A checked caller sees only returns whose error can be nil; a
			// return is dropped only when its error is proven non-nil.
			if successOnly && (f.nonNilGlobalErrorAtCall(ret.Results[len(ret.Results)-1], call) || f.use(ret.Results[len(ret.Results)-1], ret) == nil) {
				f.discharge("proof: return with a non-nil error excluded by the caller's check")
				continue
			}
			if w := f.use(ret.Results[i], ret); w != nil {
				return append(w, "-> result of "+callee.String())
			}
		}
	}
	return nil
}

// nonNilGlobalErrorAtCall preserves initialization phase at this exact call
// while checking a refusal return. It must not populate global/value facts:
// another call may run in the owner's still-incomplete initializer.
func (f *nilFlow) nonNilGlobalErrorAtCall(value ssa.Value, call ssa.CallInstruction) bool {
	if value == nil || !isError(value.Type()) || call == nil || call.Parent() == nil || call.Parent().Pkg == nil {
		return false
	}
	for {
		switch converted := value.(type) {
		case *ssa.ChangeInterface:
			value = converted.X
		case *ssa.ChangeType:
			value = converted.X
		default:
			goto original
		}
	}
original:
	load, ok := value.(*ssa.UnOp)
	if !ok || load.Op != token.MUL {
		return false
	}
	global, ok := load.X.(*ssa.Global)
	if !ok {
		return false
	}
	fact := f.globalInitializer(global)
	if fact == nil || !fact.nonNil {
		return false
	}
	caller := call.Parent()
	if caller.Pkg.Pkg.Path() != fact.owner.Pkg.Path() {
		imported := false
		for _, dependency := range caller.Pkg.Pkg.Imports() {
			imported = imported || dependency.Path() == fact.owner.Pkg.Path()
		}
		if !imported {
			return false
		}
	} else {
		if caller != fact.owner.Func("init") || !fact.store.Block().Dominates(call.Block()) || (fact.store.Block() == call.Block() && instructionIndex(fact.store) >= instructionIndex(call)) {
			return false
		}
	}
	f.discharge("proof: exact checked callsite executes after source-verified single package initializer with no other writes or address escape: " + global.String() + " at " + f.position(call.Parent(), call.Pos()))
	return true
}

// embedded judges a struct value converted to an interface: the interface is
// never nil, but the methods it promotes forward to its embedded interface
// fields, so each of those is judged by projectField where the value is read.
func (f *nilFlow) embedded(x ssa.Value) []string {
	st := structOf(x.Type())
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		if !field.Anonymous() || !types.IsInterface(field.Type()) {
			continue
		}
		if w := f.projectField(x, field); w != nil {
			return append(w[:len(w):len(w)], "-> promoted through embedded field "+field.Name()+" of "+x.Type().String())
		}
	}
	f.discharge("proof: struct value, every embedded interface judged")
	return nil
}

// projectField judges the value of field inside the struct value x (field is
// x's own field or nested inline in it): zero in a zero constant; through
// reaching definitions when x is loaded from a local composite literal, or
// through the canonical inline projection of that object's whole-value stores
// when the field itself was never addressed; otherwise the field's possible
// values anywhere. Embedded interfaces and whole-value stores share this
// projection.
func (f *nilFlow) projectField(x ssa.Value, field *types.Var) []string {
	name := "field " + field.Name()
	// A by-value parameter is a snapshot of each actual argument, not every
	// allocation of its struct type. No pointer/alias assumption is made here.
	if param, ok := x.(*ssa.Parameter); ok && !isPointer(param.Type()) {
		key := struct {
			value ssa.Value
			field *types.Var
		}{x, field}
		if f.active[key] {
			return []string{"unresolved cyclic projection of " + name}
		}
		f.active[key] = true
		defer delete(f.active, key)
		index := sourceParameterIndex(param)
		sites := f.p.SourceCallSites(param.Parent())
		matched := false
		for _, site := range sites {
			arg, compatible := sourceArgumentAt(param, index, site)
			if !compatible {
				continue
			}
			matched = true
			if w := f.projectField(arg, field); w != nil {
				return w
			}
		}
		if matched {
			f.discharge("proof: by-value field follows every visible caller argument")
			return nil
		}
		return f.field(field)
	}
	if _, ok := x.(*ssa.Const); ok {
		return []string{"zero " + name + " of " + x.Type().String()}
	}
	index := fieldIndex(x.Type(), field)
	if load, ok := x.(*ssa.UnOp); ok && load.Op == token.MUL && index >= 0 {
		if cell, ok := load.X.(*ssa.Alloc); ok && cell.Referrers() != nil {
			var addr *ssa.FieldAddr
			for _, ref := range *cell.Referrers() {
				if fa, ok := ref.(*ssa.FieldAddr); ok && fa.X == cell && fa.Field == index {
					addr = fa
				}
			}
			if addr == nil {
				// A local object loaded whole holds this field from its
				// whole-value stores (a checked constructor's success return or
				// a caller's spilled copy), not from an automatic zero merely
				// because the field was never addressed. Judge it by the
				// canonical inline projection at this load: the object's own
				// start is zero, and an escaped, unproven or unknown source
				// stays possible nil.
				projected := f.projectFieldAt(x, []int{index}, load)
				if projected.Witness != nil {
					return projected.Witness
				}
				if projected.Unknown {
					return []string{"unknown " + name + " of " + x.Type().String()}
				}
				return nil
			}
			return f.definedRead(load, addr, name)
		}
	}
	return f.field(field)
}

// nilable reports a type whose values can be nil.
func nilable(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Kind() == types.UnsafePointer || u.Kind() == types.UntypedNil
	case *types.Struct, *types.Array:
		return false
	}
	return true
}

func isPointer(t types.Type) bool {
	_, ok := t.Underlying().(*types.Pointer)
	return ok
}

func (f *nilFlow) load(load *ssa.UnOp) []string {
	if f.deferredErrorNonNil(load) {
		f.discharge("proof: source-verified conditional error preservation through exact deferred cell")
		return nil
	}
	switch addr := load.X.(type) {
	case *ssa.FieldAddr:
		return f.definedRead(load, addr, "field "+structField(addr.X.Type(), addr.Field).Name())
	case *ssa.Alloc:
		if stored, ok := ImmutableCellValueAt(addr, load); ok {
			return f.use(stored, load)
		}
		if localCell(addr) {
			return f.definedRead(load, addr, "local "+addr.Comment)
		}
		return f.stores(addr, "local "+addr.Comment)
	case *ssa.Global:
		return f.globalLoad(addr, load)
	case *ssa.FreeVar:
		// A captured variable: the closure loads its enclosing function's cell.
		if cell, ok := freeVarBinding(addr).(*ssa.Alloc); ok {
			// A cell that one store defines, and no other write or use can
			// change, holds that value in every closure that captures it: the
			// capture is where the value leaves this function, so it is judged
			// there, with the nil tests and checked errors that hold there.
			if stored, capture, ok := capturedCellValue(addr, cell); ok {
				if w := f.use(stored, capture); w != nil {
					return append(w[:len(w):len(w)], "-> captured "+addr.Name()+" of "+capture.Parent().String())
				}
				f.discharge("proof: immutable captured cell " + addr.Name() + " of " + capture.Parent().String())
				return nil
			}
			if store, capture, ok := capturedCellStore(addr, cell); ok {
				if w := f.use(store.Val, capture); w != nil {
					return append(w[:len(w):len(w)], "-> captured "+addr.Name()+" of "+capture.Parent().String())
				}
				f.discharge("proof: reaching store before the capture of " + addr.Name() + " of " + capture.Parent().String())
				return nil
			}
			return f.stores(cell, "captured "+addr.Name())
		}
	}
	f.discharge(fmt.Sprintf("convention: unmodelled load through %T", load.X))
	return nil
}

// capturedCellValue names the value a closure's captured cell holds, with the
// MakeClosure that captured it, when ImmutableCellValueAt proves the cell one
// store defines before that capture and nothing else writes or passes on. A
// closure body runs only after the capture that created it, so every load
// there reads that one value: the capture is the point where the value leaves
// its function, and there the same nil tests and checked errors apply as at
// the load itself. A mutated, escaped or opaque cell has no such value.
func capturedCellValue(free *ssa.FreeVar, cell *ssa.Alloc) (ssa.Value, *ssa.MakeClosure, bool) {
	closure, _ := freeVarClosure(free)
	if closure == nil {
		return nil, nil, false
	}
	stored, ok := ImmutableCellValueAt(cell, closure)
	if !ok {
		return nil, nil, false
	}
	return stored, closure, true
}

// capturedCellStore names the one definition that reaches a closure's capture
// of cell, with that capture, when the cell can still hold only that value:
// its address escapes to no call, no capturing closure stores into it, and no
// store to it can run again after the capture. Earlier reassignments are permitted, but the cell therefore holds,
// when the closure reads it, the single value that reached its capture; and the capture, not the store, is where that value leaves the
// function, so the nil tests and checked errors that dominate the capture
// bound it. A reaching definition this rule cannot name as one store, and a
// cell any of those writes can still change, is unknown.
func capturedCellStore(free *ssa.FreeVar, cell *ssa.Alloc) (*ssa.Store, *ssa.MakeClosure, bool) {
	closure, _ := freeVarClosure(free)
	if closure == nil || !localCell(cell) || len(capturedStores(cell)) > 0 {
		return nil, nil, false
	}
	for _, store := range storesTo(cell) {
		if cellRewrittenAfter(cell, store, closure) {
			return nil, nil, false
		}
	}
	definitions := definitionsBefore(closure.Block(), instructionIndex(closure), cell, nil)
	if len(definitions) != 1 || definitions[0].store == nil {
		return nil, nil, false
	}
	return definitions[0].store, closure, true
}

// storesTo are the direct stores to cell in its own function.
func storesTo(cell *ssa.Alloc) []*ssa.Store {
	if cell.Referrers() == nil {
		return nil
	}
	var out []*ssa.Store
	for _, ref := range *cell.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == cell {
			out = append(out, store)
		}
	}
	return out
}

// instructionIndex is instruction's slot in its own block, or -1.
func instructionIndex(instruction ssa.Instruction) int {
	for index, candidate := range instruction.Block().Instrs {
		if candidate == instruction {
			return index
		}
	}
	return -1
}

func freeVarBinding(free *ssa.FreeVar) ssa.Value {
	if closure, index := freeVarClosure(free); closure != nil {
		return closure.Bindings[index]
	}
	return nil
}

// freeVarClosure returns the enclosing function's MakeClosure that binds a
// closure's free variable, with the binding's index, or nil: the one
// closure-binding resolution (freeVarBinding and DerivedOnlyFrom use it).
// FreeVarClosure is freeVarClosure for the census policy packages.
func FreeVarClosure(free *ssa.FreeVar) (*ssa.MakeClosure, int) { return freeVarClosure(free) }

func freeVarClosure(free *ssa.FreeVar) (*ssa.MakeClosure, int) {
	fn := free.Parent()
	for i, candidate := range fn.FreeVars {
		if candidate != free || fn.Parent() == nil {
			continue
		}
		for _, block := range fn.Parent().Blocks {
			for _, ins := range block.Instrs {
				if closure, ok := ins.(*ssa.MakeClosure); ok && closure.Fn == fn && i < len(closure.Bindings) {
					return closure, i
				}
			}
		}
	}
	return nil, 0
}

// localReferenceFact names which of the two facts the one reference-use
// traversal returns for an object's address.
type localReferenceFact int

const (
	// wholeCellFact is the fact the census policy packages read a cell's value
	// by: the value a whole load reads is one of the values the cell's own
	// whole-value stores put there, or its zero value. A field address is
	// refused, because a store through it changes part of that value; a capture
	// is followed into the closure, whose whole-value uses are then checked the
	// same way.
	wholeCellFact localReferenceFact = iota
	// inlineNonEscapeFact is the fact the nil model excludes calls by: no
	// reference derived from the object's address leaves its frame's own
	// memory, so no call the frame makes can write the object and the frame's
	// own inline field stores are all its writes. Addressing an inline field
	// stays inside the object and is followed; a capture is refused, because a
	// function value holding a reference into the object can run at any time and
	// its stores are then not the frame's visible stores.
	inlineNonEscapeFact
)

// localCell reports an object used only by direct loads and stores of its whole
// value, in its own function and in every closure that captures it (at any
// depth): its address escapes to no call or other value anywhere, so its loads
// and stores are all its reads and writes. It is the whole-cell fact, which
// refuses an address-derived field reference; localInlineObject asks the strict
// inline-address fact of the same traversal.
func localCell(cell *ssa.Alloc) bool {
	return localReferences(cell, wholeCellFact, map[ssa.Value]bool{})
}

// LocalCell is localCell for the census policy packages: every use of the cell
// is a load, a store of its whole value, or a capture whose uses are the same,
// so the value a load reads is one of the values its whole-cell stores put
// there, or its zero value. A field address is refused: a store through one
// changes part of a value those packages read whole, which is a different
// question from whether a reference derived from the object leaves its
// function. The stores a capturing closure makes are named by CapturedStores.
func LocalCell(cell *ssa.Alloc) bool { return localCell(cell) }

// localReferences is the one canonical traversal of every reference derived
// from an object's address -- the cell itself and, under the strict inline
// fact, the field addresses that name its inline fields -- reporting whether
// every use of those references is one this owner models under that fact. It
// returns both facts: wholeCellFact for the value a whole load reads,
// inlineNonEscapeFact for whether a callee can reach the object at all.
func localReferences(reference ssa.Value, fact localReferenceFact, seen map[ssa.Value]bool) bool {
	if seen[reference] {
		return true
	}
	seen[reference] = true
	if reference.Referrers() == nil {
		return false
	}
	for _, ref := range *reference.Referrers() {
		switch ref := ref.(type) {
		case *ssa.Store:
			if ref.Addr != reference || ref.Val == reference {
				return false
			}
		case *ssa.UnOp:
			if ref.Op != token.MUL || ref.X != reference {
				return false
			}
		case *ssa.FieldAddr:
			// An address-derived reference to an inline field. The whole-cell
			// fact refuses it; the strict inline fact follows it, so a store
			// through the field address is still this object's own write.
			if fact != inlineNonEscapeFact || ref.X != reference || !localReferences(ref, fact, seen) {
				return false
			}
		case *ssa.MakeClosure:
			// The whole-cell fact needs a known closure's whole-value uses: a
			// captured cell is the variable the closure reads and writes, and
			// CapturedStores names the stores it makes. The strict inline fact
			// refuses every capture, a field address included: the function value
			// can run at any time, so a store it makes is not one of the frame's
			// visible stores.
			if fact != wholeCellFact {
				return false
			}
			fn, ok := ref.Fn.(*ssa.Function)
			if !ok {
				return false
			}
			for i, binding := range ref.Bindings {
				if binding == reference && (i >= len(fn.FreeVars) || !localReferences(fn.FreeVars[i], fact, seen)) {
					return false
				}
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// localInlineObject reports that addr names the object a local allocation
// starts, or one of that object's inline fields, and that no reference derived
// from the object's address leaves its frame's own memory (the strict
// inline-address fact). Nothing outside the frame can then hold a pointer to
// the object, so no call the frame makes can write it, and only its own visible
// stores define its inline fields. A capture refuses the fact even when the
// closure body only writes this object: a function value runs at any time, so
// its write is invisible to the backwards walk that reads this location.
func localInlineObject(addr ssa.Value) bool {
	alloc, ok := addressRoot(addr).(*ssa.Alloc)
	return ok && localReferences(alloc, inlineNonEscapeFact, map[ssa.Value]bool{})
}

// addressRoot returns the value an address operand starts from: the object
// itself, behind any chain of inline struct projections and conversions that
// name the same memory. A root this walk does not model (a pointer load, a
// parameter, a global) is returned as it is, so only a local allocation's own
// address is ever recognised.
func addressRoot(addr ssa.Value) ssa.Value {
	for depth := 0; depth < 8; depth++ {
		switch x := addr.(type) {
		case *ssa.FieldAddr:
			addr = x.X
		case *ssa.ChangeType:
			addr = x.X
		case *ssa.Convert:
			addr = x.X
		default:
			return addr
		}
	}
	return addr
}

// CapturedStores is capturedStores for the census policy packages.
func CapturedStores(cell ssa.Value) []*ssa.Store { return capturedStores(cell) }

// capturedStores are the stores a capturing closure (or one nested in it)
// makes to cell; they may run at any time relative to the enclosing function.
func capturedStores(cell ssa.Value) []*ssa.Store {
	var out []*ssa.Store
	if cell.Referrers() == nil {
		return nil
	}
	for _, ref := range *cell.Referrers() {
		closure, ok := ref.(*ssa.MakeClosure)
		if !ok {
			continue
		}
		fn := closure.Fn.(*ssa.Function)
		for i, binding := range closure.Bindings {
			if binding != cell || i >= len(fn.FreeVars) || fn.FreeVars[i].Referrers() == nil {
				continue
			}
			free := fn.FreeVars[i]
			for _, use := range *free.Referrers() {
				if store, ok := use.(*ssa.Store); ok && store.Addr == free {
					out = append(out, store)
				}
			}
			out = append(out, capturedStores(free)...)
		}
	}
	return out
}

// definedRead judges a load of a struct field or a local cell by the
// definitions that reach it within its function: on every path backwards from
// the load, the nearest store to the location, a write that may reach it
// through another reference, the branch of a nil test of the same location
// (whose result is then known), or the location's start. A later store kills
// an earlier one, so a value initialized and then overwritten is judged by the
// overwrite. A field's start or unknown write stands for its possible values
// anywhere (every store and zero allocation); a cell's start for its zero
// value. Stores by capturing closures may run at any time and always count.
func (f *nilFlow) definedRead(load *ssa.UnOp, addr ssa.Value, name string) []string {
	var field *types.Var
	if fa, ok := addr.(*ssa.FieldAddr); ok {
		field = structField(fa.X.Type(), fa.Field)
	}
	everywhere := func() []string {
		if field != nil {
			return f.field(field)
		}
		return []string{"zero " + name}
	}
	if cell, ok := addr.(*ssa.Alloc); ok {
		for _, store := range capturedStores(cell) {
			if w := f.use(store.Val, store); w != nil {
				return append(w[:len(w):len(w)], "-> stored in "+name+" by a closure")
			}
		}
	}
	for _, d := range reachingDefinitions(load, addr, field) {
		var w []string
		switch {
		case d.store != nil:
			if w = f.storedValue(d.store, load); w != nil {
				w = append(w[:len(w):len(w)], "-> stored in "+name)
			}
		case d.whole != nil:
			// An exact whole-value store defines this field through its own
			// inline path, however many indices that path has: the stored value
			// is judged by its own source (a checked constructor's success
			// returns for a tuple), exactly as ProjectFieldAt judges it, never by
			// the field's stores anywhere. Only a store that names no inline path
			// from its target to this location keeps the whole-value projection.
			path := d.wholePath
			if len(path) == 0 {
				if index := fieldIndex(d.whole.Val.Type(), field); index >= 0 {
					path = []int{index}
				}
			}
			if len(path) > 0 {
				projected := f.projectFieldAt(d.whole.Val, path, load)
				w = projected.Witness
				f.trail = append(f.trail, projected.Why...)
			} else {
				w = f.projectField(d.whole.Val, field)
			}
			if w == nil {
				f.discharge("proof: field projected from exact whole-base store")
			}
		case d.unknown:
			if w = everywhere(); w != nil {
				w = append(w[:len(w):len(w)], "-> after a write that may reach "+name)
			}
		case d.nilBranch:
			w = []string{"nil on the nil branch of a test of " + name}
		case d.nonNilBranch:
			f.discharge("proof: non-nil branch of a test of the location")
		default: // the location's start: function entry or its creation
			w = everywhere()
		}
		if w != nil {
			return w
		}
	}
	return nil
}

// storedValue judges a stored value at the load that reads it; a stored call
// result whose error is tested nil before the load (on the load's block, or on
// every path from the store to the load) comes from the callee's success
// returns.
func (f *nilFlow) storedValue(store *ssa.Store, load *ssa.UnOp) []string {
	if x, ok := store.Val.(*ssa.Extract); ok {
		if call, ok := x.Tuple.(*ssa.Call); ok && (errorChecked(call, load.Block()) || errorCheckedBetween(call, store, load)) {
			return f.resultOf(call, x.Index, true)
		}
	}
	return f.use(store.Val, store)
}

type definition struct {
	store                   *ssa.Store // a store to this very location
	whole                   *ssa.Store // whole value stored to this field's exact base
	wholePath               []int      // exact inline path when the store targets an ancestor
	unknown                 bool       // a write that may change it to any of its possible values
	nilBranch, nonNilBranch bool       // the known result of a nil test of it
	tested                  *ssa.UnOp  // the tested load, for a nil-test result
	// none of these: the location's start (function entry, or where its
	// cell or base object is created)
}

// locationBase returns the value naming addr's object: the cell itself, or the
// base of a field address. The walk stops at its definition, so every match
// "same location" found before it concerns the same object (a loop iteration
// that redefines the base or reallocates the cell is never crossed).
func locationBase(addr ssa.Value) ssa.Value {
	if fa, ok := addr.(*ssa.FieldAddr); ok {
		return cellObject(fa.X)
	}
	return addr
}

// writesLocation reports an instruction that defines addr's location: the
// creation of its cell or base object, a store to the same location, or, for a
// field, a write that may reach it through another reference (a store to the
// same field of another base, a store of a whole struct that contains the
// field, a store of the object's own address as a value, or a non-builtin call,
// which may do either). A call is a writer only when a reference to the object
// could reach it: a local object whose address-derived references never leave
// its function (localInlineObject) is written by its visible stores alone.
// Concurrent writes are out of scope: a read racing a write is a data race, and
// the census assumes the race-free program the -race test runs check.
func writesLocation(ins ssa.Instruction, addr ssa.Value, field *types.Var) (definition, bool) {
	if v, ok := ins.(ssa.Value); ok && v == locationBase(addr) {
		return definition{}, true
	}
	switch ins := ins.(type) {
	case *ssa.Store:
		if sameLocation(ins.Addr, addr) {
			return definition{store: ins}, true
		}
		if path, ok := inlineAddressPath(addr, ins.Addr); ok && len(path) > 0 {
			return definition{whole: ins, wholePath: path}, true
		}
		if field == nil {
			return definition{}, false
		}
		// The object's own address, stored as a value into a package variable,
		// another object or the object itself, puts a pointer to it where this
		// walk does not follow it: a later write through that reference may reach
		// the field, however its value was last set here.
		if alloc, ok := addressRoot(addr).(*ssa.Alloc); ok && addressRoot(ins.Val) == alloc {
			return definition{unknown: true}, true
		}
		if fa, ok := ins.Addr.(*ssa.FieldAddr); ok && structField(fa.X.Type(), fa.Field) == field {
			return definition{unknown: true}, true
		}
		// An exact whole-base assignment defines this field by projection of
		// the assigned value. Another object's store remains unknown below.
		if fa, ok := addr.(*ssa.FieldAddr); ok && cellObject(ins.Addr) == cellObject(fa.X) && types.Identical(ins.Val.Type(), fa.X.Type().Underlying().(*types.Pointer).Elem()) {
			return definition{whole: ins}, true
		}
		if ptr, ok := ins.Addr.Type().Underlying().(*types.Pointer); ok && containsField(ptr.Elem(), field, 0) {
			return definition{unknown: true}, true
		}
	case *ssa.Call:
		// A call writes a field only through a reference to its object. No
		// reference derived from a local object's address leaves its frame
		// (localInlineObject), so nothing the call can reach names that object
		// and the call is not a writer of its fields. Every other call is unknown.
		if _, builtin := ins.Call.Value.(*ssa.Builtin); field != nil && !builtin && !localInlineObject(addr) {
			return definition{unknown: true}, true
		}
	case *ssa.Go, *ssa.RunDefers:
		if field != nil {
			return definition{unknown: true}, true
		}
	}
	return definition{}, false
}

// containsField reports that a value of type t holds field inline (not behind
// a pointer), at any depth.
func containsField(t types.Type, field *types.Var, depth int) bool {
	s := structOf(t)
	if s == nil || depth > 8 {
		return false
	}
	if _, isPtr := t.Underlying().(*types.Pointer); isPtr {
		return false
	}
	for i := 0; i < s.NumFields(); i++ {
		if s.Field(i) == field || containsField(s.Field(i).Type(), field, depth+1) {
			return true
		}
	}
	return false
}

// testedEdge reports the known result on the edge pred->succ when pred ends in
// a nil test of a load of the same location (same cell, or same field of the
// same base) and nothing writes the location between that load and the test.
func testedEdge(pred, succ *ssa.BasicBlock, addr ssa.Value, field *types.Var) (definition, bool) {
	iff, ok := pred.Instrs[len(pred.Instrs)-1].(*ssa.If)
	if !ok {
		return definition{}, false
	}
	tested, nilIndex, ok := NilComparison(iff.Cond)
	if !ok {
		return definition{}, false
	}
	load, ok := tested.(*ssa.UnOp)
	if !ok || load.Op != token.MUL || load.Block() != pred || !sameLocation(load.X, addr) {
		return definition{}, false
	}
	after := false
	for _, ins := range pred.Instrs {
		if ins == load {
			after = true
			continue
		}
		if _, writes := writesLocation(ins, addr, field); after && writes {
			return definition{}, false
		}
	}
	nilSucc := pred.Succs[nilIndex]
	if pred.Succs[0] == pred.Succs[1] {
		return definition{}, false
	}
	if succ == nilSucc {
		return definition{nilBranch: true, tested: load}, true
	}
	return definition{nonNilBranch: true, tested: load}, true
}

// edgeContradictsStableUse excludes only an edge testing the exact SSA value
// whose opposite nil fact holds at origin. A value defined in a cyclic block
// can denote a different dynamic definition on a prior iteration; parameters
// remain stable throughout this frame. Distinct field reloads never match.
func edgeContradictsStableUse(pred, succ, origin *ssa.BasicBlock) bool {
	if pred == nil || succ == nil || origin == nil || len(pred.Instrs) == 0 {
		return false
	}
	iff, ok := pred.Instrs[len(pred.Instrs)-1].(*ssa.If)
	if !ok || len(pred.Succs) != 2 || pred.Succs[0] == pred.Succs[1] {
		return false
	}
	value, nilIndex, ok := NilComparison(iff.Cond)
	if !ok || !StableGuardValue(value) {
		return false
	}
	nilKnown, nonNilKnown := NilAt(value, origin), NonNilAt(value, origin)
	// Contradictory facts name an unsupported/unreachable origin, not a
	// license to discard every incoming definition.
	if nilKnown == nonNilKnown {
		return false
	}
	if succ == pred.Succs[nilIndex] {
		return nonNilKnown
	}
	return succ == pred.Succs[1-nilIndex] && nilKnown
}

// StableGuardValue reports whether an SSA value has one definition throughout
// this function frame. Parameters stay fixed; definitions in cyclic blocks may
// denote different values on successive iterations and remain unproven.
// Repeated-guard proofs must also require the exact same SSA value.
func StableGuardValue(value ssa.Value) bool {
	if _, ok := value.(*ssa.Parameter); ok {
		return true
	}
	definition, ok := value.(ssa.Instruction)
	if !ok || definition.Block() == nil {
		return false
	}
	block := definition.Block()
	seen := map[*ssa.BasicBlock]bool{}
	pending := append([]*ssa.BasicBlock(nil), block.Succs...)
	for len(pending) > 0 {
		next := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if next == block {
			return false
		}
		if !seen[next] {
			seen[next] = true
			pending = append(pending, next.Succs...)
		}
	}
	return true
}

// reachingDefinitions walks backwards from load and returns, for every path,
// the nearest definition of its location.
func reachingDefinitions(load *ssa.UnOp, addr ssa.Value, field *types.Var) []definition {
	// A nested base may itself be a freshly reloaded pointer. Its SSA
	// definition is not the allocation boundary of the guarded object. Establish
	// the stable source load before the ordinary backwards location walk stops
	// at that reload; require both the nil guard and the complete interval.
	for _, block := range load.Parent().Blocks {
		if len(block.Instrs) == 0 || !block.Dominates(load.Block()) {
			continue
		}
		branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		value, _, ok := NilComparison(branch.Cond)
		if !ok {
			continue
		}
		tested, ok := value.(*ssa.UnOp)
		if !ok || tested == load || tested.Op != token.MUL || !NonNilAt(tested, load.Block()) {
			continue
		}
		if sameLocation(tested.X, addr) && stableLoadInterval(tested, load) {
			return []definition{{nonNilBranch: true, tested: tested}}
		}
	}
	return definitionsBefore(load.Block(), instructionIndex(load), addr, field)
}

// definitionsBefore walks backwards from (block, index) and returns, for every
// path, the nearest definition of the location, with one shared walk for every
// reader of a location (a load, a closure's capture).
func definitionsBefore(block *ssa.BasicBlock, index int, addr ssa.Value, field *types.Var) []definition {
	origin := block
	pruned := false
	var out []definition
	if block == nil || index < 0 {
		return nil
	}
	scanned := map[*ssa.BasicBlock]bool{}
	var scan func(block *ssa.BasicBlock, end int)
	scan = func(block *ssa.BasicBlock, end int) {
		for i := end - 1; i >= 0; i-- {
			if d, ok := writesLocation(block.Instrs[i], addr, field); ok {
				out = append(out, d)
				return
			}
		}
		if len(block.Preds) == 0 {
			out = append(out, definition{})
			return
		}
		for _, pred := range block.Preds {
			if edgeContradictsStableUse(pred, block, origin) {
				pruned = true
				continue
			}
			if d, ok := testedEdge(pred, block, addr, field); ok {
				out = append(out, d)
				continue
			}
			if !scanned[pred] {
				scanned[pred] = true
				scan(pred, len(pred.Instrs))
			}
		}
	}
	scan(block, index)
	if pruned && len(out) == 0 {
		return []definition{{unknown: true}}
	}
	return out
}

// errorCheckedBetween reports that every path from store to load passes the
// success block of call (callSuccess). The success block must be entered only
// by that edge.
func errorCheckedBetween(call *ssa.Call, store *ssa.Store, load *ssa.UnOp) bool {
	_, success := callSuccess(call)
	if success == nil || len(success.Preds) != 1 || success == store.Block() {
		return false
	}
	return !reachesAvoiding(store, load, success)
}

// reachesAvoiding reports a control-flow path from store to load that never
// enters the block avoid.
func reachesAvoiding(store, load ssa.Instruction, avoid *ssa.BasicBlock) bool {
	from, to := store.Block(), load.Block()
	if from == to {
		for _, ins := range from.Instrs {
			if ins == store {
				return true
			}
			if ins == load {
				break
			}
		}
	}
	seen := map[*ssa.BasicBlock]bool{}
	queue := append([]*ssa.BasicBlock(nil), from.Succs...)
	for len(queue) > 0 {
		block := queue[0]
		queue = queue[1:]
		if block == avoid || seen[block] {
			continue
		}
		if block == to {
			return true
		}
		seen[block] = true
		queue = append(queue, block.Succs...)
	}
	return false
}

// globalLoad keeps the zero-value arm unless source proves one unconditional
// package initializer and no other write or escaped address. A store somewhere
// in a setter is not initialization; Global.Referrers may itself be absent.
func (f *nilFlow) globalLoad(global *ssa.Global, load *ssa.UnOp) []string {
	if f.initializedGlobal(global, load) {
		f.discharge("proof: source-verified single package initializer with no other writes or address escape: " + global.String())
		return nil
	}
	return []string{"zero or unproved initializer of global " + global.String()}
}

func sameGlobal(value ssa.Value, global *ssa.Global) bool {
	other, ok := value.(*ssa.Global)
	return ok && other.Pkg != nil && global.Pkg != nil && other.Pkg.Pkg.Path() == global.Pkg.Pkg.Path() && other.Name() == global.Name()
}

type globalInitializer struct {
	owner  *ssa.Package
	store  *ssa.Store
	nonNil bool
}

func (f *nilFlow) initializedGlobal(global *ssa.Global, load *ssa.UnOp) bool {
	if global.Pkg == nil || load.Parent() == nil || load.Parent().Pkg == nil {
		return false
	}
	fact := f.globalInitializer(global)
	if fact == nil || !fact.nonNil {
		return false
	}
	if load.Parent().Pkg.Pkg.Path() == fact.owner.Pkg.Path() {
		// Do not infer a phase for a same-package read, including an initializer
		// calling a function before the global is assigned.
		if load.Parent() == fact.owner.Func("init") {
			if !fact.store.Block().Dominates(load.Block()) || (fact.store.Block() == load.Block() && instructionIndex(fact.store) >= instructionIndex(load)) {
				return false
			}
		} else if !f.initializerPrecedesFunction(fact, load.Parent()) {
			return false
		} else {
			f.discharge("proof: complete source initializer prefix cannot enter " + load.Parent().String() + " before " + global.String() + " initializer store")
		}
	} else {
		imported := false
		for _, dependency := range load.Parent().Pkg.Pkg.Imports() {
			imported = imported || dependency.Path() == fact.owner.Pkg.Path()
		}
		if !imported {
			return false
		}
	}
	return true
}

// initializerPrecedesFunction proves that package initialization cannot enter
// reader before this exact global store. Unknown callbacks, unavailable source,
// escaping function values and recursive call prefixes keep the zero arm.
// Imported package init functions have already completed before owner init.
func (f *nilFlow) initializerPrecedesFunction(fact *globalInitializer, reader *ssa.Function) bool {
	if f.initializationOrder == nil {
		f.initializationOrder = map[*ssa.Store]map[*ssa.Function]bool{}
	}
	key := reader
	if cached := f.initializationOrder[fact.store]; cached != nil {
		if result, ok := cached[key]; ok {
			return result
		}
	}
	result := f.proveInitializerCapabilities(fact, reader)
	if f.initializationOrder[fact.store] == nil {
		f.initializationOrder[fact.store] = map[*ssa.Function]bool{}
	}
	f.initializationOrder[fact.store][key] = result
	return result
}

func (f *nilFlow) globalInitializer(global *ssa.Global) *globalInitializer {
	if fact, checked := f.globalInitializers[global]; checked {
		return fact
	}
	f.globalInitializers[global] = nil // recursive initialization remains unproved
	owner := global.Pkg
	if init := owner.Func("init"); init == nil || len(init.Blocks) == 0 {
		owner = f.sourcePackage(owner.Pkg.Path())
	}
	if owner == nil {
		return nil
	}
	init := owner.Func("init")
	if init == nil || init.Synthetic != "package initializer" {
		return nil
	}
	var store *ssa.Store
	inspect := func(fn *ssa.Function) bool {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if _, debug := ins.(*ssa.DebugRef); debug {
					continue
				}
				used := false
				for _, operand := range ins.Operands(nil) {
					if operand != nil && *operand != nil && sameGlobal(*operand, global) {
						used = true
					}
				}
				if !used {
					continue
				}
				switch instruction := ins.(type) {
				case *ssa.UnOp:
					if instruction.Op != token.MUL || !sameGlobal(instruction.X, global) {
						return false
					}
				case *ssa.Store:
					if !sameGlobal(instruction.Addr, global) || sameGlobal(instruction.Val, global) || store != nil {
						return false
					}
					store = instruction
				default:
					return false
				}
			}
		}
		return true
	}
	// Original bodies include writes by importing packages, while the owner may
	// have been loaded as an export-only dependency and needs its own full source.
	for _, fn := range f.p.Functions {
		if !inspect(fn) {
			return nil
		}
	}
	if owner != global.Pkg {
		for _, fn := range f.bodies[owner.Pkg.Path()] {
			if !inspect(fn) {
				return nil
			}
		}
	}
	if store == nil || store.Parent() != init {
		return nil
	}
	fact := &globalInitializer{owner: owner, store: store}
	fact.nonNil = f.sourceInitializerNonNil(store.Val, store, map[ssa.Value]bool{})
	f.globalInitializers[global] = fact
	return fact
}

func (f *nilFlow) sourceInitializerNonNil(value ssa.Value, at ssa.Instruction, active map[ssa.Value]bool) bool {
	if value == nil || active[value] {
		return false
	}
	active[value] = true
	defer delete(active, value)
	switch v := value.(type) {
	case *ssa.Const:
		return !v.IsNil()
	case *ssa.MakeInterface:
		// An error interface with a fixed concrete dynamic type is non-nil even
		// when its pointer payload is nil. Do not apply that predicate fact to a
		// typed-nil context receiver, whose methods may dereference that payload.
		if isError(v.Type()) {
			return true
		}
	case *ssa.ChangeInterface:
		return f.sourceInitializerNonNil(v.X, at, active)
	case *ssa.ChangeType:
		return f.sourceInitializerNonNil(v.X, at, active)
	case *ssa.Call:
		return f.sourceInitializerCall(v, 0, active)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return f.sourceInitializerCall(call, v.Index, active)
		}
	}
	start := len(f.trail)
	if f.use(value, at) != nil {
		return false
	}
	reasons := f.trail[start:]
	if len(reasons) == 0 {
		return !nilable(value.Type())
	}
	for _, reason := range reasons {
		if !strings.HasPrefix(reason, "proof:") {
			return false
		}
	}
	return true
}

func (f *nilFlow) sourceInitializerCall(call *ssa.Call, index int, active map[ssa.Value]bool) bool {
	callee := call.Common().StaticCallee()
	if callee == nil {
		return false
	}
	if len(callee.Blocks) == 0 {
		callee = f.externalBody(callee)
	}
	if callee == nil || len(callee.Blocks) == 0 || callee.Recover != nil {
		return false
	}
	returned := false
	for _, block := range callee.Blocks {
		for _, ins := range block.Instrs {
			if ret, ok := ins.(*ssa.Return); ok {
				returned = true
				if index < 0 || index >= len(ret.Results) || !f.sourceInitializerNonNil(ret.Results[index], ret, active) {
					return false
				}
			}
		}
	}
	return returned
}

// stores reports a nil reaching any store to cell, by its own function or a
// capturing closure, or its zero value when no store exists. A local cell
// whose address escapes (to a call, or stored as a value) can be written by
// code the analysis does not follow, so any value may reach it.
func (f *nilFlow) stores(cell ssa.Value, name string) []string {
	if cell.Referrers() == nil {
		return nil
	}
	if alloc, ok := cell.(*ssa.Alloc); ok && !localCell(alloc) {
		return []string{"the address of " + name + " escapes to code that may write it"}
	}
	stored := false
	all := capturedStores(cell)
	for _, ref := range *cell.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == cell {
			all = append(all, store)
		}
	}
	for _, store := range all {
		stored = true
		if w := f.use(store.Val, store); w != nil {
			return append(w, "-> stored in "+name)
		}
	}
	if !stored {
		return []string{"zero " + name}
	}
	return nil
}

func (f *nilFlow) field(field *types.Var) (witness []string) {
	if field == nil {
		return nil
	}
	if w, done := f.fields[field]; done {
		if w == nil {
			f.trail = append(f.trail, f.fieldWhy[field]...)
		}
		return w
	}
	if f.active[field] {
		f.discharge("proof: cycle adds no new source")
		return nil
	}
	f.active[field] = true
	start := len(f.trail)
	defer func() {
		delete(f.active, field)
		f.fields[field] = witness
		if witness == nil {
			f.fieldWhy[field] = dedupe(f.trail[start:])
		}
	}()
	for _, fn := range f.p.Functions {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				switch ins := ins.(type) {
				case *ssa.Store:
					if addr, ok := ins.Addr.(*ssa.FieldAddr); ok && structField(addr.X.Type(), addr.Field) == field {
						if w := f.use(ins.Val, ins); w != nil {
							return append(w, "-> stored in field "+field.Name())
						}
					} else if containsField(ins.Val.Type(), field, 0) {
						// A whole value stored over a struct sets this field
						// to the stored value's own field.
						if w := f.projectField(ins.Val, field); w != nil {
							return append(w[:len(w):len(w)], "-> stored whole at "+f.position(fn, ins.Pos()))
						}
					}
				case *ssa.Alloc:
					if index := fieldIndex(ins.Type(), field); index >= 0 {
						if !storesField(ins, index) {
							return []string{fmt.Sprintf("zero field %s at %s", field.Name(), f.position(fn, ins.Pos()))}
						}
						f.discharge("proof: the allocating function stores the field")
					}
				}
			}
		}
	}
	return nil
}

func structOf(t types.Type) *types.Struct {
	if ptr, ok := types.Unalias(t).Underlying().(*types.Pointer); ok {
		t = ptr.Elem()
	}
	st, _ := types.Unalias(t).Underlying().(*types.Struct)
	return st
}

func structField(t types.Type, index int) *types.Var {
	if st := structOf(t); st != nil && index < st.NumFields() {
		return st.Field(index)
	}
	return nil
}

func fieldIndex(t types.Type, field *types.Var) int {
	if st := structOf(t); st != nil {
		for i := 0; i < st.NumFields(); i++ {
			if st.Field(i) == field {
				return i
			}
		}
	}
	return -1
}

// storesField reports whether the allocating function stores the field of
// that allocation, as a composite literal or a later assignment does, also
// through loads of an immutable cell holding it (cellObject). A whole value
// stored into the allocation (a struct built elsewhere and assigned)
// initializes every field; that value's fields are judged where it was built.
// The zero value a partial composite literal stores first sets no field.
func storesField(alloc *ssa.Alloc, index int) bool {
	if alloc.Referrers() == nil {
		return false
	}
	bases := []ssa.Value{alloc}
	for _, ref := range *alloc.Referrers() {
		held, ok := ref.(*ssa.Store)
		if !ok || held.Val != alloc || held.Addr.Referrers() == nil {
			continue
		}
		for _, use := range *held.Addr.Referrers() {
			if load, ok := use.(*ssa.UnOp); ok && load.Op == token.MUL && cellObject(load) == ssa.Value(alloc) {
				bases = append(bases, load)
			}
		}
	}
	for _, base := range bases {
		if addressSetsField(base, index) {
			return true
		}
	}
	return false
}

// addressSetsField reports that the function stores field index of the
// struct at addr: a store to that field's address (fieldStoredAt), or a whole
// value other than the zero value stored at addr, whose own fields are judged
// where it was built (a partial composite literal assigned over a variable
// stores the zero value first and then only the fields it names).
func addressSetsField(addr ssa.Value, index int) bool {
	if fieldStoredAt(addr, index) {
		return true
	}
	if addr.Referrers() == nil {
		return false
	}
	for _, ref := range *addr.Referrers() {
		if store, ok := ref.(*ssa.Store); ok && store.Addr == addr {
			if _, zero := store.Val.(*ssa.Const); !zero {
				return true
			}
		}
	}
	return false
}

// fieldStoredAt reports a store to the address of field index of the struct
// at addr.
func fieldStoredAt(addr ssa.Value, index int) bool {
	if addr.Referrers() == nil {
		return false
	}
	for _, ref := range *addr.Referrers() {
		field, ok := ref.(*ssa.FieldAddr)
		if !ok || field.X != addr || field.Field != index || field.Referrers() == nil {
			continue
		}
		for _, use := range *field.Referrers() {
			if store, ok := use.(*ssa.Store); ok && store.Addr == field {
				return true
			}
		}
	}
	return false
}
