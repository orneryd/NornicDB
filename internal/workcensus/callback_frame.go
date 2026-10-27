package workcensus

import (
	"go/constant"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

// CallbackFrameProof is a source proof for work inside a retained callback.
// The function value may escape. Only instructions covered by Covers are proved
// to finish before Frame returns: one local mutex serializes entry and joins it
// at sealing, a closed flag rejects later entry, and a second flag prevents a
// repeated invocation. This is not proof that an arbitrary callback is synchronous
// or that every effect of its body is protected. Clients must check each effect.
type CallbackFrameProof struct {
	Frame    *ssa.Function
	Callback *ssa.MakeClosure
	Mutex    ssa.Value
	Closed   ssa.Value
	Called   ssa.Value
	Seal     ssa.Instruction
	lock     ssa.Instruction
	claimed  ssa.Instruction
	open     *ssa.BasicBlock
	first    *ssa.BasicBlock
}

// Covers reports whether an ordinary instruction is inside the proved critical
// section, below both rejection tests and the once claim. Starting asynchronous
// work or registering another deferred callback does not inherit that lifetime.
func (p *CallbackFrameProof) Covers(ins ssa.Instruction) bool {
	if p == nil || ins == nil || ins.Parent() != p.Callback.Fn {
		return false
	}
	switch ins.(type) {
	case *ssa.Go, *ssa.Defer, *ssa.MakeClosure:
		return false
	}
	return instructionDominates(p.lock, ins) && instructionDominates(p.claimed, ins) &&
		p.open.Dominates(ins.Block()) && p.first.Dominates(ins.Block())
}

// ProveCallbackFrame recognizes a deliberately narrow, name-independent SSA
// lifetime protocol. The callback and seal use the same local sync.Mutex; a seal
// is deferred before the callback can be handed off; that seal joins the mutex
// and permanently closes it on every normal exit. All relevant cells must remain
// private to this frame and the two closures. Unsupported aliases, invisible
// helper bodies and missing proof return false, never a non-escaping assumption.
func ProveCallbackFrame(mc *ssa.MakeClosure) (*CallbackFrameProof, bool) {
	if mc == nil {
		return nil, false
	}
	fn, ok := mc.Fn.(*ssa.Function)
	if !ok || fn.Parent() != mc.Parent() {
		return nil, false
	}
	for mi, binding := range mc.Bindings {
		mu, ok := binding.(*ssa.Alloc)
		if !ok || mu.Parent() != mc.Parent() || !isMutexPointer(mu.Type()) || mi >= len(fn.FreeVars) {
			continue
		}
		mutex := fn.FreeVars[mi]
		lock, unlock := mutexSpan(fn, mutex)
		if lock == nil || !unlock {
			continue
		}
		for si, binding := range mc.Bindings {
			closed, ok := binding.(*ssa.Alloc)
			if !ok || closed.Parent() != mc.Parent() || !isBoolPointer(closed.Type()) || si >= len(fn.FreeVars) {
				continue
			}
			open := falseGuard(fn, fn.FreeVars[si])
			if open == nil {
				continue
			}
			sealClosure, registration, seal := deferredSeal(mc, mu, closed)
			if seal == nil || !privateFrameCell(mu, mc, sealClosure, registration, false) || !privateFrameCell(closed, mc, sealClosure, registration, true) || !frameFlagWritesLocked(closed, mu) {
				continue
			}
			for ci, binding := range mc.Bindings {
				called, ok := binding.(*ssa.Alloc)
				if !ok || called == closed || called.Parent() != mc.Parent() || !isBoolPointer(called.Type()) || ci >= len(fn.FreeVars) {
					continue
				}
				first := falseGuard(fn, fn.FreeVars[ci])
				if first == nil || !privateFrameCell(called, mc, nil, registration, true) || !frameFlagWritesLocked(called, mu) {
					continue
				}
				var claim *ssa.Store
				valid := true
				for _, block := range fn.Blocks {
					for _, ins := range block.Instrs {
						if store, ok := ins.(*ssa.Store); ok && store.Addr == fn.FreeVars[ci] {
							if claim != nil || !boolConstant(store.Val, true) || !open.Dominates(block) || !first.Dominates(block) || !instructionDominates(lock, ins) {
								valid = false
							}
							claim = store
						}
					}
				}
				if valid && claim != nil && closureBoolUses(fn.FreeVars[si], false) && closureBoolUses(fn.FreeVars[ci], true) {
					return &CallbackFrameProof{Frame: mc.Parent(), Callback: mc, Mutex: mu, Closed: closed, Called: called, Seal: seal, lock: lock, claimed: claim, open: open, first: first}, true
				}
			}
		}
	}
	return nil, false
}

func isMutexPointer(t types.Type) bool {
	pointer, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := pointer.Elem().(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "sync" && named.Obj().Name() == "Mutex"
}

func isBoolPointer(t types.Type) bool {
	pointer, ok := t.(*types.Pointer)
	return ok && types.Identical(pointer.Elem(), types.Typ[types.Bool])
}

func boolConstant(v ssa.Value, want bool) bool {
	c, ok := v.(*ssa.Const)
	return ok && c.Value != nil && c.Value.Kind() == constant.Bool && constant.BoolVal(c.Value) == want
}

func instructionDominates(a, b ssa.Instruction) bool {
	if a == nil || b == nil || a.Parent() != b.Parent() || !a.Block().Dominates(b.Block()) {
		return false
	}
	if a.Block() != b.Block() {
		return true
	}
	for _, ins := range a.Block().Instrs {
		if ins == a {
			return true
		}
		if ins == b {
			return false
		}
	}
	return false
}

func mutexMethod(call *ssa.CallCommon, mu ssa.Value, name string) bool {
	callee := call.StaticCallee()
	return callee != nil && callee.Pkg != nil && callee.Pkg.Pkg.Path() == "sync" && callee.Name() == name && len(call.Args) == 1 && call.Args[0] == mu
}

// A guard holds its mutex until its deferred unlock. An ordinary early unlock,
// another lock, or passing the mutex to an unknown user is outside this proof.
func mutexSpan(fn *ssa.Function, mu ssa.Value) (ssa.Instruction, bool) {
	var lock ssa.Instruction
	var deferred ssa.Instruction
	refs := mu.Referrers()
	if refs == nil {
		return nil, false
	}
	for _, ref := range *refs {
		switch x := ref.(type) {
		case *ssa.Call:
			if lock != nil || !mutexMethod(x.Common(), mu, "Lock") {
				return nil, false
			}
			lock = x
		case *ssa.Defer:
			if deferred != nil || !mutexMethod(x.Common(), mu, "Unlock") {
				return nil, false
			}
			deferred = x
		case *ssa.DebugRef:
		default:
			return nil, false
		}
	}
	return lock, instructionDominates(lock, deferred)
}

func falseGuard(fn *ssa.Function, cell ssa.Value) *ssa.BasicBlock {
	for _, block := range fn.Blocks {
		if len(block.Instrs) == 0 {
			continue
		}
		iff, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		load, ok := iff.Cond.(*ssa.UnOp)
		if ok && load.Op == token.MUL && load.X == cell && len(block.Succs[1].Preds) == 1 && block.Succs[1].Preds[0] == block {
			return block.Succs[1]
		}
	}
	return nil
}

func closureBoolUses(cell ssa.Value, writable bool) bool {
	refs := cell.Referrers()
	if refs == nil {
		return false
	}
	for _, ref := range *refs {
		switch x := ref.(type) {
		case *ssa.UnOp:
			if x.Op != token.MUL || x.X != cell {
				return false
			}
		case *ssa.Store:
			if !writable || x.Addr != cell || !boolConstant(x.Val, true) {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

func deferredSeal(mc *ssa.MakeClosure, mu, closed *ssa.Alloc) (*ssa.MakeClosure, ssa.Instruction, ssa.Instruction) {
	for _, block := range mc.Parent().Blocks {
		for _, ins := range block.Instrs {
			deferred, ok := ins.(*ssa.Defer)
			if !ok {
				continue
			}
			seal, ok := deferred.Common().Value.(*ssa.MakeClosure)
			if !ok {
				continue
			}
			fn := seal.Fn.(*ssa.Function)
			var mutex, flag ssa.Value
			for i, binding := range seal.Bindings {
				if binding == mu {
					mutex = fn.FreeVars[i]
				}
				if binding == closed {
					flag = fn.FreeVars[i]
				}
			}
			if mutex == nil || flag == nil || !sealBeforeUse(mc, deferred) {
				continue
			}
			var lock, store, unlock ssa.Instruction
			valid := true
			for _, b := range fn.Blocks {
				for _, instruction := range b.Instrs {
					switch x := instruction.(type) {
					case *ssa.Call:
						switch {
						case lock == nil && mutexMethod(x.Common(), mutex, "Lock"):
							lock = x
						case unlock == nil && mutexMethod(x.Common(), mutex, "Unlock"):
							unlock = x
						default:
							valid = false
						}
					case *ssa.Store:
						if store != nil || x.Addr != flag || !boolConstant(x.Val, true) {
							valid = false
						}
						store = x
					case *ssa.Go, *ssa.Defer, *ssa.MakeClosure:
						valid = false
					}
				}
			}
			if !valid || !instructionDominates(lock, store) || !instructionDominates(store, unlock) {
				continue
			}
			for _, b := range fn.Blocks {
				for _, instruction := range b.Instrs {
					if _, ok := instruction.(*ssa.Return); ok && !instructionDominates(unlock, instruction) {
						valid = false
					}
				}
			}
			if valid {
				return seal, deferred, store
			}
		}
	}
	return nil, nil, nil
}

func sealBeforeUse(mc *ssa.MakeClosure, registration ssa.Instruction) bool {
	refs := mc.Referrers()
	if refs == nil {
		return false
	}
	for _, ref := range *refs {
		if _, debug := ref.(*ssa.DebugRef); !debug && !instructionDominates(registration, ref) {
			return false
		}
	}
	return true
}

func privateFrameCell(cell *ssa.Alloc, guard, seal *ssa.MakeClosure, registration ssa.Instruction, flag bool) bool {
	refs := cell.Referrers()
	if refs == nil {
		return false
	}
	for _, ref := range *refs {
		switch x := ref.(type) {
		case *ssa.MakeClosure:
			if x != guard && x != seal {
				return false
			}
		case *ssa.Store:
			if !flag || x.Addr != cell || (!boolConstant(x.Val, true) && !(boolConstant(x.Val, false) && instructionDominates(x, registration))) {
				return false
			}
		case *ssa.UnOp:
			if !flag || x.Op != token.MUL {
				return false
			}
		case *ssa.Call:
			if flag || !mutexMethod(x.Common(), cell, "Lock") {
				return false
			}
		case *ssa.Defer:
			if flag || !mutexMethod(x.Common(), cell, "Unlock") {
				return false
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return true
}

// Parent-side closing writes must use the same mutex too. The narrow supported
// parent span has no ordinary unlock; its deferred unlock follows the store.
func frameFlagWritesLocked(flag, mu *ssa.Alloc) bool {
	for _, ref := range *flag.Referrers() {
		store, ok := ref.(*ssa.Store)
		if !ok || !boolConstant(store.Val, true) {
			continue
		}
		held := false
		for _, use := range *mu.Referrers() {
			if call, ok := use.(*ssa.Call); ok {
				if mutexMethod(call.Common(), mu, "Unlock") {
					return false
				}
				if mutexMethod(call.Common(), mu, "Lock") && instructionDominates(call, store) {
					held = true
				}
			}
		}
		if !held {
			return false
		}
	}
	return true
}
