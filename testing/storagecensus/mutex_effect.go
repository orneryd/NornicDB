package storagecensus

import (
	"go/types"

	"github.com/orneryd/nornicdb/internal/lockeffect"
	"golang.org/x/tools/go/ssa"
)

// mutexMethodEffect recognizes the compiler-bound lock implementations used by
// storage. A cancellable acquisition establishes a hold only on its nil-error
// edge; the method name alone never grants coverage to another receiver.
func mutexMethodEffect(method *types.Func) (action string, exclusive, fallible bool) {
	if method == nil || method.Pkg() == nil {
		return "", false, false
	}
	sig, ok := method.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return "", false, false
	}
	receiver := types.Unalias(sig.Recv().Type())
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = types.Unalias(pointer.Elem())
	}
	named, ok := receiver.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return "", false, false
	}
	path, name := named.Obj().Pkg().Path(), named.Obj().Name()
	standard := path == "sync" && (name == "Mutex" || name == "RWMutex")
	cancellable := path == "github.com/orneryd/nornicdb/pkg/storage" && name == "trackedRWMutex"
	if !standard && !cancellable {
		return "", false, false
	}
	if cancellable && (method.Name() == "LockContext" || method.Name() == "RLockContext") {
		return "lock", method.Name() == "LockContext", true
	}
	action, exclusive = lockeffect.Classify(method.Name(), false)
	return action, exclusive, false
}

// mutexCallEffect resolves interface dispatch as well as direct calls. Every
// possible implementation must have the same recognized effect. An unresolved
// receiver or a same-spelled method on another type grants no hold.
func (q *acquirer) mutexCallEffect(call *ssa.Call) (receiver ssa.Value, action string, exclusive, fallible bool) {
	common := call.Common()
	var callees []*ssa.Function
	if callee := common.StaticCallee(); callee != nil {
		callees = append(callees, callee)
		if len(common.Args) > 0 {
			receiver = common.Args[0]
		}
	} else if common.IsInvoke() {
		receiver = common.Value
		callees = q.p.callTargets(call)
	}
	if receiver == nil || len(callees) == 0 {
		return nil, "", false, false
	}
	for index, callee := range callees {
		method, _ := callee.Object().(*types.Func)
		a, x, f := mutexMethodEffect(method)
		if a == "" || index > 0 && (a != action || x != exclusive || f != fallible) {
			return nil, "", false, false
		}
		action, exclusive, fallible = a, x, f
	}
	return receiver, action, exclusive, fallible
}
