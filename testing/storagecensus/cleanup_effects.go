package storagecensus

import (
	"go/constant"
	"go/token"
	"go/types"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// cleanupEffects proves effects as well as error preservation. Capturing a
// source error does not itself taint a closure in the decision analysis, so an
// arbitrary write in a deferred failure arm cannot be delegated to that pass.
// The only terminal effects admitted here are the reviewed lifecycle owners;
// an unknown helper must have a completely effect-free body.
func cleanupEffects(fn *ssa.Function, capture int, errorCell *ssa.Alloc, branch *ssa.BasicBlock) bool {
	var binding *ssa.MakeClosure
	for _, use := range *errorCell.Referrers() {
		if closure, ok := use.(*ssa.MakeClosure); ok && closure.Fn == fn {
			binding = closure
		}
	}
	if binding == nil || capture < 0 || capture >= len(fn.FreeVars) {
		return false
	}
	proof := cleanupEffectProof{root: fn, binding: binding, errorCell: errorCell, capture: capture, branch: branch, visiting: map[*ssa.Function]bool{}}
	return proof.function(fn, true)
}

type cleanupEffectProof struct {
	root      *ssa.Function
	binding   *ssa.MakeClosure
	errorCell *ssa.Alloc
	capture   int
	branch    *ssa.BasicBlock
	visiting  map[*ssa.Function]bool
}

func (p *cleanupEffectProof) function(fn *ssa.Function, root bool) bool {
	if fn == nil || len(fn.Blocks) == 0 || p.visiting[fn] {
		return false
	}
	p.visiting[fn] = true
	defer delete(p.visiting, fn)
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			switch ins := instruction.(type) {
			case *ssa.Call:
				if !p.call(ins.Common(), root) {
					return false
				}
			case *ssa.Store:
				if root && ins.Addr == p.root.FreeVars[p.capture] {
					continue
				} // cleanupComposes proved every such store.
				if root && p.siblingNil(ins) {
					continue
				}
				if !cleanupOutcomeScratch(ins) {
					return false
				}
			case *ssa.UnOp:
				if ins.Op == token.ARROW {
					return false
				}
			case *ssa.Alloc, *ssa.BinOp, *ssa.Extract, *ssa.Field, *ssa.FieldAddr,
				*ssa.Index, *ssa.IndexAddr, *ssa.Lookup, *ssa.Slice, *ssa.ChangeInterface,
				*ssa.MakeInterface, *ssa.ChangeType, *ssa.Convert, *ssa.TypeAssert,
				*ssa.Phi, *ssa.If, *ssa.Jump, *ssa.Return, *ssa.DebugRef:
				// These instructions compute/read values; stores and calls are checked separately.
			default:
				return false // goroutines, sends, map writes, panic, nested defers and unknown effects.
			}
		}
	}
	return true
}

func (p *cleanupEffectProof) siblingNil(store *ssa.Store) bool {
	nilValue, ok := store.Val.(*ssa.Const)
	if !ok || !nilValue.IsNil() {
		return false
	}
	free, ok := store.Addr.(*ssa.FreeVar)
	if !ok {
		return false
	}
	for i, candidate := range p.root.FreeVars {
		if candidate != free || i >= len(p.binding.Bindings) {
			continue
		}
		cell, ok := p.binding.Bindings[i].(*ssa.Alloc)
		return ok && unaliasedSiblingResult(cell, p.branch.Parent(), p.root, p.errorCell)
	}
	return false
}

func (p *cleanupEffectProof) call(call *ssa.CallCommon, root bool) bool {
	if trustedOutcomes(call) {
		return true
	}
	if builtin, ok := call.Value.(*ssa.Builtin); ok {
		switch builtin.Name() {
		case "len", "cap", "real", "imag", "complex", "min", "max":
			return true
		}
		return false
	}
	if call.IsInvoke() {
		method := call.Method
		return method != nil && method.Pkg() != nil && method.Pkg().Path() == "context" && method.Name() == "Err" && method.Type().(*types.Signature).Params().Len() == 0 && oneErrorResult(method.Type().(*types.Signature))
	}
	if fn := call.StaticCallee(); fn != nil {
		if cleanupContextFunction(fn) {
			return true
		}
		if root && cleanupTerminal(fn) {
			return true
		}
		return p.function(fn, false)
	}
	// A completion callback is a terminal capability only when its value is
	// the release result of the exact WorkLifetime.Acquire method. Arbitrary
	// function parameters, reassigned cells and escaped aliases grant nothing.
	if !root {
		return false
	}
	value := p.boundValue(call.Value)
	if fn, ok := value.(*ssa.Function); ok {
		return p.function(fn, false)
	}
	release, ok := value.(*ssa.Extract)
	if !ok || release.Index != 1 {
		return false
	}
	acquisition, ok := release.Tuple.(*ssa.Call)
	if !ok {
		return false
	}
	fn := acquisition.Common().StaticCallee()
	if !cleanupMethod(fn, Module+"pkg/lifecycle", "WorkLifetime", "Acquire") {
		return false
	}
	sig := fn.Signature
	return sig.Params().Len() == 1 && contextType(sig.Params().At(0).Type()) && sig.Results().Len() == 3 && contextType(sig.Results().At(0).Type()) && voidFunction(sig.Results().At(1).Type()) && basicKind(sig.Results().At(2).Type(), types.Bool)
}

func (p *cleanupEffectProof) boundValue(value ssa.Value) ssa.Value {
	if load, ok := value.(*ssa.UnOp); ok && load.Op == token.MUL {
		if free, ok := load.X.(*ssa.FreeVar); ok {
			for i, candidate := range p.root.FreeVars {
				if candidate == free && i < len(p.binding.Bindings) {
					bound := p.binding.Bindings[i]
					if stored, ok := workcensus.ImmutableCellValueAt(bound, p.binding); ok {
						return stored
					}
					return nil
				}
			}
		}
		if stored, ok := readOnlyCellValue(value); ok {
			return stored
		}
		return nil
	}
	if free, ok := value.(*ssa.FreeVar); ok {
		for i, candidate := range p.root.FreeVars {
			if candidate == free && i < len(p.binding.Bindings) {
				return p.binding.Bindings[i]
			}
		}
	}
	return value
}

// These are compiler-bound effects, never a constructor-name exemption or a
// method-name-only allowance. Their own lifecycle/state contracts are reviewed
// separately; other code cannot inherit their authority through a wrapper.
func cleanupTerminal(fn *ssa.Function) bool {
	if fn == nil || fn.Signature == nil {
		return false
	}
	sig := fn.Signature
	if cleanupMethod(fn, Module+"pkg/nornicdb", "DB", "Close") {
		return sig.Params().Len() == 0 && oneErrorResult(sig)
	}
	if cleanupMethod(fn, Module+"pkg/nornicdb", "DB", "LifetimeContext") {
		return sig.Params().Len() == 0 && sig.Results().Len() == 1 && contextType(sig.Results().At(0).Type())
	}
	if cleanupMethod(fn, Module+"pkg/server", "Server", "beginShutdown") {
		return sig.Params().Len() == 1 && contextType(sig.Params().At(0).Type()) && sig.Results().Len() == 1 && basicKind(sig.Results().At(0).Type(), types.Bool)
	}
	return cleanupMethod(fn, Module+"pkg/server", "Server", "WaitStopped") && sig.Params().Len() == 1 && contextType(sig.Params().At(0).Type()) && oneErrorResult(sig)
}

func cleanupMethod(fn *ssa.Function, path, receiver, method string) bool {
	if fn == nil || fn.Name() != method || fn.Signature == nil || fn.Signature.Recv() == nil {
		return false
	}
	pointer, ok := types.Unalias(fn.Signature.Recv().Type()).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == path && named.Obj().Name() == receiver && fn.Object() != nil && fn.Object().Pkg() != nil && fn.Object().Pkg().Path() == path
}
func contextType(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context"
}
func basicKind(t types.Type, kind types.BasicKind) bool {
	basic, ok := t.Underlying().(*types.Basic)
	return ok && basic.Kind() == kind
}
func oneErrorResult(sig *types.Signature) bool {
	return sig.Results().Len() == 1 && types.Identical(sig.Results().At(0).Type(), types.Universe.Lookup("error").Type())
}
func voidFunction(t types.Type) bool {
	sig, ok := t.Underlying().(*types.Signature)
	return ok && !sig.Variadic() && sig.Params().Len() == 0 && sig.Results().Len() == 0
}
func cleanupContextFunction(fn *ssa.Function) bool {
	if fn.Pkg == nil || fn.Pkg.Pkg == nil || fn.Pkg.Pkg.Path() != "context" || fn.Signature.Recv() != nil {
		return false
	}
	sig := fn.Signature
	if sig.Results().Len() != 1 || !contextType(sig.Results().At(0).Type()) {
		return false
	}
	return (fn.Name() == "Background" && sig.Params().Len() == 0) || (fn.Name() == "WithoutCancel" && sig.Params().Len() == 1 && contextType(sig.Params().At(0).Type()))
}

// The only local stores besides named results are the compiler's variadic
// scratch array for compose.Outcomes. It must have no other reader or escape.
func cleanupOutcomeScratch(store *ssa.Store) bool {
	index, ok := store.Addr.(*ssa.IndexAddr)
	if !ok {
		return false
	}
	array, ok := index.X.(*ssa.Alloc)
	if !ok || array.Referrers() == nil {
		return false
	}
	pointer, ok := array.Type().Underlying().(*types.Pointer)
	if !ok {
		return false
	}
	typ, ok := pointer.Elem().Underlying().(*types.Array)
	if !ok || !types.Identical(typ.Elem(), types.Universe.Lookup("error").Type()) {
		return false
	}
	sliceSeen := false
	for _, ref := range *array.Referrers() {
		switch ref := ref.(type) {
		case *ssa.IndexAddr:
			position, ok := ref.Index.(*ssa.Const)
			if !ok || position.Value == nil || position.Value.Kind() != constant.Int || ref.Referrers() == nil {
				return false
			}
			offset, ok := constant.Int64Val(position.Value)
			if !ok || offset < 0 || offset >= typ.Len() {
				return false
			}
			for _, use := range *ref.Referrers() {
				if written, ok := use.(*ssa.Store); !ok || written.Addr != ref {
					return false
				}
			}
		case *ssa.Slice:
			if ref.Low != nil || ref.High != nil || ref.Max != nil || ref.Referrers() == nil {
				return false
			}
			sliceSeen = true
			for _, use := range *ref.Referrers() {
				call, ok := use.(*ssa.Call)
				if !ok || !trustedOutcomes(call.Common()) || len(call.Common().Args) != 2 || call.Common().Args[1] != ref {
					return false
				}
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return sliceSeen
}
