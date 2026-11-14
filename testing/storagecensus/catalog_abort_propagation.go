package storagecensus

import (
	"go/token"
	"go/types"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// catalogAbortReturnsExact reports whether branch is the catalog transition's
// abort arm for errv: it rolls the transaction back through the canonical
// storage transaction, releases the catalog gate through the local closure that
// only sends the token, and returns exactly the failed operation's error.
//
// The caller supplies the error the arm's test compares with nil, as
// propagatesError receives it. Every identity below is the compiler's own: the
// callee is the *types.Func declared on *storage.BadgerTransaction, not a
// same-named method of another type; the released channel is the *types.Var of
// the enclosing receiver's own catalogGate field, not a same-named field of
// another struct; and the closure captures that receiver itself. A wrong
// rollback receiver, an interface dispatch, another channel, a second release,
// extra work in the arm or in the closure, a rebound closure, and a swallowed,
// replaced or rewritten error therefore all fail closed. The pure arms this
// helper does not claim (release and return alone) stay with returnsExactError.
func catalogAbortReturnsExact(branch *ssa.BasicBlock, errv ssa.Value) bool {
	if branch == nil || len(branch.Instrs) == 0 {
		return false
	}
	owner := branch.Parent()
	if owner == nil || owner.Signature == nil || owner.Signature.Recv() == nil || len(owner.Params) == 0 {
		return false
	}
	gate := catalogGateField(owner.Signature.Recv().Type())
	if gate == nil || !catalogExactError(errv) || !catalogAbortOnNonNil(branch, errv) {
		return false
	}
	rollback, release, ok := catalogAbortInstructions(branch, errv)
	if !ok || !catalogRollbackTransaction(rollback) || !catalogFailedTransactionMatches(errv, rollback) {
		return false
	}
	return catalogGateRelease(owner, release, gate)
}

// catalogFailedTransactionMatches binds compensation to the failed transaction.
// An unrelated transaction with the same Go type cannot discharge this abort.
// Operations on another owner (including namespace input normalization) remain
// unclaimed rather than inferred to belong to the transaction being rolled back.
func catalogFailedTransactionMatches(errv ssa.Value, rollback *ssa.Call) bool {
	var failed *ssa.Call
	switch value := errv.(type) {
	case *ssa.Call:
		failed = value
	case *ssa.Extract:
		failed, _ = value.Tuple.(*ssa.Call)
	}
	if failed == nil || failed.Common().IsInvoke() || len(failed.Common().Args) == 0 || len(rollback.Common().Args) != 1 {
		return false
	}
	callee := failed.Common().StaticCallee()
	if callee == nil || callee.Signature == nil || callee.Signature.Recv() == nil {
		return false
	}
	named := catalogPointerNamed(callee.Signature.Recv().Type())
	if named == nil || named.Obj() == nil || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != Module+"pkg/storage" || named.Obj().Name() != "BadgerTransaction" {
		return false
	}
	return failed.Common().Args[0] == rollback.Common().Args[0]
}

// catalogExactError requires errv to be the error a failed operation returned,
// never a parameter, join or cell load that can be rebound elsewhere.
func catalogExactError(errv ssa.Value) bool {
	if errv == nil || errv.Type() == nil || !types.Identical(errv.Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	switch errv.(type) {
	case *ssa.Call, *ssa.Extract:
		return true
	default:
		return false
	}
}

// catalogAbortOnNonNil requires branch to be entered only from the non-nil
// successor of a nil test of errv.
func catalogAbortOnNonNil(branch *ssa.BasicBlock, errv ssa.Value) bool {
	if len(branch.Preds) != 1 {
		return false
	}
	test := branch.Preds[0]
	if len(test.Instrs) == 0 || len(test.Succs) != 2 || test.Succs[0] == test.Succs[1] {
		return false
	}
	iff, ok := test.Instrs[len(test.Instrs)-1].(*ssa.If)
	if !ok {
		return false
	}
	tested, nilIndex, ok := workcensus.NilComparison(iff.Cond)
	return ok && tested == errv && test.Succs[1-nilIndex] == branch
}

// catalogAbortInstructions splits the arm into its two calls: the rollback and
// then the gate release, with nothing else but the release call's own binding
// load and the exact return of errv.
func catalogAbortInstructions(branch *ssa.BasicBlock, errv ssa.Value) (rollback, release *ssa.Call, ok bool) {
	ret, ok := branch.Instrs[len(branch.Instrs)-1].(*ssa.Return)
	if !ok || len(ret.Results) != 1 || ret.Results[0] != errv {
		return nil, nil, false
	}
	var calls []*ssa.Call
	var loads []*ssa.UnOp
	for _, ins := range branch.Instrs[:len(branch.Instrs)-1] {
		switch ins := ins.(type) {
		case *ssa.DebugRef:
		case *ssa.Call:
			calls = append(calls, ins)
		case *ssa.UnOp:
			if ins.Op != token.MUL {
				return nil, nil, false
			}
			loads = append(loads, ins)
		default:
			return nil, nil, false
		}
	}
	if len(calls) != 2 || len(loads) > 1 {
		return nil, nil, false
	}
	rollback, release = calls[0], calls[1]
	if len(loads) == 1 && (release.Call.Value != loads[0] || rollback.Call.Value == loads[0]) {
		return nil, nil, false
	}
	return rollback, release, true
}

// catalogRollbackTransaction recognizes the canonical storage rollback: a
// static call of the Rollback method that *storage.BadgerTransaction itself
// declares, taking nothing and returning one error.
func catalogRollbackTransaction(call *ssa.Call) bool {
	if call == nil || call.Call.IsInvoke() {
		return false
	}
	callee := call.Call.StaticCallee()
	if callee == nil {
		return false
	}
	object, ok := callee.Object().(*types.Func)
	if !ok || object.Name() != "Rollback" || object.Pkg() == nil || object.Pkg().Path() != Module+"pkg/storage" {
		return false
	}
	signature, ok := object.Type().(*types.Signature)
	if !ok || signature.Recv() == nil || signature.Params().Len() != 0 || signature.Results().Len() != 1 {
		return false
	}
	if !types.Identical(signature.Results().At(0).Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	named := catalogPointerNamed(signature.Recv().Type())
	if named == nil {
		return false
	}
	if object := named.Obj(); object == nil || object.Pkg() == nil || object.Pkg().Path() != Module+"pkg/storage" || object.Name() != "BadgerTransaction" {
		return false
	}
	// The resolved callee must be that type's own declared method, not one
	// promoted from an embedded type or declared on a same-named look-alike.
	for i := 0; i < named.NumMethods(); i++ {
		if named.Method(i) == callee.Object() {
			return true
		}
	}
	return false
}

// catalogGateRelease requires the second call to invoke the local release
// closure on exactly the enclosing receiver's gate.
func catalogGateRelease(owner *ssa.Function, call *ssa.Call, gate *types.Var) bool {
	if call == nil || call.Call.IsInvoke() || len(call.Call.Args) != 0 {
		return false
	}
	closure := catalogReleaseClosure(call.Call.Value)
	if closure == nil {
		return false
	}
	function, ok := closure.Fn.(*ssa.Function)
	if !ok || function.Signature.Params().Len() != 0 || function.Signature.Results().Len() != 0 {
		return false
	}
	if len(function.FreeVars) != 1 || len(closure.Bindings) != 1 || len(function.Blocks) != 1 {
		return false
	}
	if !catalogCapturedReceiver(owner, closure.Bindings[0]) {
		return false
	}
	return catalogGateSend(function, gate)
}

// catalogReleaseClosure resolves the closure a call invokes: the make-closure
// itself, or the value of a local function cell written exactly once. A rebound
// cell has no single closure, so it fails closed.
func catalogReleaseClosure(value ssa.Value) *ssa.MakeClosure {
	switch value := value.(type) {
	case *ssa.MakeClosure:
		return value
	case *ssa.UnOp:
		if value.Op != token.MUL {
			return nil
		}
		cell, ok := value.X.(*ssa.Alloc)
		if !ok || cell.Referrers() == nil {
			return nil
		}
		var closure *ssa.MakeClosure
		for _, ref := range *cell.Referrers() {
			store, ok := ref.(*ssa.Store)
			if !ok {
				continue
			}
			made, ok := store.Val.(*ssa.MakeClosure)
			if !ok || closure != nil {
				return nil
			}
			closure = made
		}
		return closure
	}
	return nil
}

// catalogCapturedReceiver requires the closure to capture the enclosing
// method's receiver itself: the parameter, or the cell that holds only it.
func catalogCapturedReceiver(owner *ssa.Function, binding ssa.Value) bool {
	recv := owner.Params[0]
	if binding == recv {
		return true
	}
	cell, ok := binding.(*ssa.Alloc)
	if !ok || cell.Referrers() == nil {
		return false
	}
	stores := 0
	for _, ref := range *cell.Referrers() {
		store, ok := ref.(*ssa.Store)
		if !ok {
			continue
		}
		stores++
		if store.Val != recv {
			return false
		}
	}
	return stores == 1
}

// catalogGateSend requires the closure's body to be only the token send on the
// gate field of the receiver it captured.
func catalogGateSend(closure *ssa.Function, gate *types.Var) bool {
	block := closure.Blocks[0]
	ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
	if !ok || len(ret.Results) != 0 {
		return false
	}
	var send *ssa.Send
	addresses := 0
	for _, ins := range block.Instrs[:len(block.Instrs)-1] {
		switch ins := ins.(type) {
		case *ssa.DebugRef:
		case *ssa.UnOp:
			if ins.Op != token.MUL {
				return false
			}
		case *ssa.FieldAddr:
			addresses++
			if addresses > 1 || !catalogGateFieldAddr(ins, gate) {
				return false
			}
		case *ssa.Send:
			if send != nil {
				return false
			}
			send = ins
		default:
			return false
		}
	}
	if send == nil || !catalogToken(send.X) {
		return false
	}
	return catalogGateChannel(send.Chan, gate, closure.FreeVars[0])
}

// catalogGateChannel requires the send to use the channel loaded from the gate
// field of the closure's own captured receiver.
func catalogGateChannel(channel ssa.Value, gate *types.Var, captured *ssa.FreeVar) bool {
	for depth := 0; depth < 4; depth++ {
		switch value := channel.(type) {
		case *ssa.UnOp:
			if value.Op != token.MUL {
				return false
			}
			channel = value.X
		case *ssa.FieldAddr:
			if !catalogGateFieldAddr(value, gate) {
				return false
			}
			base := value.X
			if load, ok := base.(*ssa.UnOp); ok && load.Op == token.MUL {
				base = load.X
			}
			return base == ssa.Value(captured)
		default:
			return false
		}
	}
	return false
}

// catalogGateFieldAddr requires the address to select exactly the gate field
// object of its own struct type.
func catalogGateFieldAddr(address *ssa.FieldAddr, gate *types.Var) bool {
	fields := catalogStruct(address.X.Type())
	if fields == nil || address.Field < 0 || address.Field >= fields.NumFields() {
		return false
	}
	return fields.Field(address.Field) == gate
}

// catalogGateField is the receiver's own catalog gate: the chan struct{} field
// the catalog serialization token travels on.
func catalogGateField(receiver types.Type) *types.Var {
	fields := catalogStruct(receiver)
	if fields == nil {
		return nil
	}
	for i := 0; i < fields.NumFields(); i++ {
		field := fields.Field(i)
		if field.Name() == "catalogGate" && catalogTokenChannel(field.Type()) {
			return field
		}
	}
	return nil
}

// catalogTokenChannel is a bidirectional channel of the empty struct token.
func catalogTokenChannel(typ types.Type) bool {
	if typ == nil {
		return false
	}
	channel, ok := types.Unalias(typ).Underlying().(*types.Chan)
	if !ok || channel.Dir() != types.SendRecv {
		return false
	}
	token, ok := types.Unalias(channel.Elem()).Underlying().(*types.Struct)
	return ok && token.NumFields() == 0
}

// catalogToken requires the sent value to be the empty struct token itself.
func catalogToken(value ssa.Value) bool {
	constant, ok := value.(*ssa.Const)
	if !ok || constant.Value != nil || constant.IsNil() {
		return false
	}
	token, ok := constant.Type().Underlying().(*types.Struct)
	return ok && token.NumFields() == 0
}

// catalogPointerNamed is the named type behind a pointer.
func catalogPointerNamed(typ types.Type) *types.Named {
	pointer, ok := types.Unalias(typ).(*types.Pointer)
	if !ok {
		return nil
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok {
		return nil
	}
	return named
}

// catalogStruct is the underlying struct of a named type or pointer to one.
func catalogStruct(typ types.Type) *types.Struct {
	if typ == nil {
		return nil
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	named, ok := typ.(*types.Named)
	if !ok {
		return nil
	}
	fields, ok := named.Underlying().(*types.Struct)
	if !ok {
		return nil
	}
	return fields
}
