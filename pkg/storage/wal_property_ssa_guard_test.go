package storage

import (
	"go/token"
	"go/types"

	"github.com/orneryd/nornicdb/internal/ssawalk"
	"golang.org/x/tools/go/ssa"
)

type bindingSSAFrame struct {
	call   ssa.CallInstruction
	parent *bindingSSAFrame
}

type bindingSSAAnalysis struct {
	calls        map[*ssa.Function][]ssa.CallInstruction
	closures     map[*ssa.Function][]*ssa.MakeClosure
	stores       map[*ssa.Function][]*ssa.Store
	writes       *types.Var
	reachability *bindingSSAReachability
}

// Follow aliases backwards from mutation/escape sinks. Call frames preserve
// parameter and tuple-result identity: a decoded object or an error returned
// alongside a borrowed byte slice is not itself an alias of that slice.
func bufferedBodySSAProducers(functions []*ssa.Function, commit types.Type, transports []types.Type, bufferedOperation *types.Const, writer *ssa.Function, owners map[*ssa.Function]bool) map[string]bool {
	a := &bindingSSAAnalysis{calls: map[*ssa.Function][]ssa.CallInstruction{}, closures: map[*ssa.Function][]*ssa.MakeClosure{}, stores: map[*ssa.Function][]*ssa.Store{}}
	fields := commit.Underlying().(*types.Struct)
	for i := 0; i < fields.NumFields(); i++ {
		if fields.Field(i).Name() == "writes" {
			a.writes = fields.Field(i)
		}
	}
	steps := ssawalk.Instructions(functions)
	for _, step := range steps {
		if call, ok := step.Ins.(ssa.CallInstruction); ok {
			if callee := call.Common().StaticCallee(); callee != nil {
				a.calls[callee] = append(a.calls[callee], call)
			}
		}
		if closure, ok := step.Ins.(*ssa.MakeClosure); ok {
			callee := closure.Fn.(*ssa.Function)
			a.closures[callee] = append(a.closures[callee], closure)
		}
		if store, ok := step.Ins.(*ssa.Store); ok {
			a.stores[step.Func] = append(a.stores[step.Func], store)
		}
	}
	isCommit := func(typ types.Type) bool {
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		return types.Identical(typ, commit)
	}
	out := map[string]bool{}
	roots := []*types.Var{a.writes}
	for _, transport := range transports {
		transportFields := transport.Underlying().(*types.Struct)
		for i := 0; i < transportFields.NumFields(); i++ {
			if transportFields.Field(i).Name() == "Writes" {
				roots = append(roots, transportFields.Field(i))
			}
		}
	}
	a.reachability = newBindingSSAReachability(a, functions)
	for _, root := range roots {
		a.writes = root
		a.reachability.setRoot(root)
		for _, fn := range functions {
			if owners[fn] {
				continue
			}
			alias := func(v ssa.Value) bool { return a.alias(v, nil, map[ssa.Value]bool{}) }
			flag := func(instruction ssa.Instruction) { out[fn.String()+" @ "+instruction.String()] = true }
			for _, step := range ssawalk.Instructions([]*ssa.Function{fn}) {
				instruction := step.Ins
				switch v := instruction.(type) {
				case *ssa.Alloc:
					if isCommit(v.Type()) {
						// Copying an existing typed commit is consumption. Assigning
						// its writes field later remains an independently checked sink.
						copy := false
						for _, store := range a.stores[fn] {
							if store.Addr == v && types.Identical(store.Val.Type(), commit) {
								copy = true
							}
						}
						if !copy {
							flag(v)
						}
					}
				case *ssa.ChangeType:
					if isCommit(v.Type()) && !isCommit(v.X.Type()) {
						flag(v)
					}
				case *ssa.Convert:
					if isCommit(v.Type()) && !isCommit(v.X.Type()) {
						flag(v)
					}
				case *ssa.Store:
					_, localSlot := v.Addr.(*ssa.Alloc)
					if !localSlot && alias(v.Addr) {
						flag(v)
					}
					if bindingSSAMapCarrier(v.Val.Type()) && alias(v.Val) {
						if _, localSlot := v.Addr.(*ssa.Alloc); !localSlot {
							flag(v)
						}
					}
				case *ssa.MapUpdate:
					if alias(v.Map) {
						flag(v)
					}
					if bindingSSAMapCarrier(v.Value.Type()) && alias(v.Value) {
						flag(v)
					}
				case *ssa.Return:
					for _, result := range v.Results {
						if bindingSSAMapCarrier(result.Type()) && alias(result) {
							flag(v)
						}
					}
				}
				if call, ok := instruction.(ssa.CallInstruction); ok {
					common := call.Common()
					if builtin, ok := common.Value.(*ssa.Builtin); ok {
						switch builtin.Name() {
						case "clear", "delete", "copy", "append":
							if len(common.Args) > 0 && alias(common.Args[0]) {
								flag(instruction)
							}
						}
					} else if callee := common.StaticCallee(); callee == nil || len(callee.Blocks) == 0 {
						for _, arg := range common.Args {
							if bindingSSAMapCarrier(arg.Type()) && alias(arg) {
								flag(instruction)
							}
							if callee == nil && !common.IsInvoke() && alias(arg) {
								flag(instruction)
							}
							if root != roots[0] && callee != nil && callee.Object() != nil && callee.Object().Pkg() != nil && callee.Object().Pkg().Path() == "github.com/dgraph-io/badger/v4" && alias(arg) {
								switch callee.Name() {
								case "Set", "SetEntry", "NewEntry":
									flag(instruction)
								}
							}
						}
					}
				}
			}
		}
	}
	// Physical WAL records are emitted only by the record owner. Constants in
	// SSA retain their resolved operation type through ordinary var aliases.
	for _, step := range steps {
		if step.Func == writer {
			continue
		}
		if call, ok := step.Ins.(ssa.CallInstruction); ok {
			for _, arg := range call.Common().Args {
				if value, ok := arg.(*ssa.Const); ok && value.Value != nil && types.Identical(value.Type(), bufferedOperation.Type()) && value.Value.ExactString() == bufferedOperation.Val().ExactString() {
					out[step.Func.String()+" @ "+step.Ins.String()] = true
				}
			}
		}
	}
	return out
}

func bindingSSAMapCarrier(typ types.Type) bool {
	switch value := typ.Underlying().(type) {
	case *types.Map:
		return bindingSSAReference(typ)
	case *types.Interface:
		return value.NumMethods() == 0
	}
	return false
}

func (a *bindingSSAAnalysis) physicalField(typ types.Type, index int) bool {
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	fields, ok := typ.Underlying().(*types.Struct)
	return ok && index < fields.NumFields() && fields.Field(index) == a.writes
}

func bindingSSAReference(typ types.Type) bool {
	switch value := typ.Underlying().(type) {
	case *types.Map:
		key, ok := value.Key().Underlying().(*types.Basic)
		return ok && key.Kind() == types.String && bindingSSAReference(value.Elem())
	case *types.Slice:
		element, ok := value.Elem().Underlying().(*types.Basic)
		return ok && element.Kind() == types.Uint8
	case *types.Pointer:
		if element, ok := value.Elem().Underlying().(*types.Basic); ok {
			return element.Kind() == types.Uint8
		}
		if array, ok := value.Elem().Underlying().(*types.Array); ok {
			element, ok := array.Elem().Underlying().(*types.Basic)
			return ok && element.Kind() == types.Uint8
		}
		return bindingSSAReference(value.Elem())
	case *types.Interface:
		return value.NumMethods() == 0
	}
	return false
}

func bindingSSASameAddress(left, right ssa.Value) bool {
	if left == right {
		return true
	}
	l, lok := left.(*ssa.FieldAddr)
	r, rok := right.(*ssa.FieldAddr)
	return lok && rok && l.Field == r.Field && bindingSSASameAddress(l.X, r.X)
}

func (a *bindingSSAAnalysis) alias(value ssa.Value, frame *bindingSSAFrame, seen map[ssa.Value]bool) bool {
	if value == nil || !bindingSSAReference(value.Type()) || seen[value] {
		return false
	}
	if a.reachability != nil && !a.reachability.mayReach(value) {
		return false
	}
	seen[value] = true
	defer delete(seen, value)
	alias := func(v ssa.Value) bool { return a.alias(v, frame, seen) }
	// Call results are traced through the callee's returns with a frame that
	// binds the callee's parameters to this call's arguments. A tuple index
	// selects the matching result position.
	callAlias := func(call *ssa.Call, index int) bool {
		callee := call.Common().StaticCallee()
		if callee == nil {
			return false
		}
		for _, block := range callee.Blocks {
			if len(block.Instrs) == 0 {
				continue
			}
			if ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return); ok && index < len(ret.Results) {
				if a.alias(ret.Results[index], &bindingSSAFrame{call: call, parent: frame}, seen) {
					return true
				}
			}
		}
		return false
	}
	switch v := value.(type) {
	case *ssa.Alloc:
		// Captured locals are addresses in closure bindings. Their initializing
		// stores belong to the enclosing function, not the closure's function.
		for _, store := range a.stores[v.Parent()] {
			if store.Addr == v && alias(store.Val) {
				return true
			}
		}
	case *ssa.FieldAddr:
		return a.physicalField(v.X.Type(), v.Field) || alias(v.X)
	case *ssa.Field:
		return a.physicalField(v.X.Type(), v.Field)
	case *ssa.Parameter:
		for current := frame; current != nil; current = current.parent {
			callee := current.call.Common().StaticCallee()
			if callee != v.Parent() {
				continue
			}
			for i, parameter := range callee.Params {
				if parameter == v && i < len(current.call.Common().Args) {
					return a.alias(current.call.Common().Args[i], current.parent, seen)
				}
			}
		}
		for _, call := range a.calls[v.Parent()] {
			for i, parameter := range v.Parent().Params {
				if parameter == v && i < len(call.Common().Args) && a.alias(call.Common().Args[i], nil, seen) {
					return true
				}
			}
		}
	case *ssa.FreeVar:
		for _, closure := range a.closures[v.Parent()] {
			for i, parameter := range v.Parent().FreeVars {
				if parameter == v && alias(closure.Bindings[i]) {
					return true
				}
			}
		}
	case *ssa.UnOp:
		if v.Op != token.MUL {
			return false
		}
		if alias(v.X) {
			return true
		}
		for _, store := range a.stores[v.Parent()] {
			if bindingSSASameAddress(store.Addr, v.X) && alias(store.Val) {
				return true
			}
		}
	case *ssa.Phi:
		for _, edge := range v.Edges {
			if alias(edge) {
				return true
			}
		}
	case *ssa.Lookup:
		if alias(v.X) {
			return true
		}
		// A fresh container can retain aliases to borrowed body slices.
		for _, block := range v.Parent().Blocks {
			for _, instruction := range block.Instrs {
				if update, ok := instruction.(*ssa.MapUpdate); ok && update.Map == v.X && alias(update.Value) {
					return true
				}
			}
		}
	case *ssa.IndexAddr:
		return alias(v.X)
	case *ssa.Index:
		return alias(v.X)
	case *ssa.Slice:
		return alias(v.X)
	case *ssa.ChangeType:
		return alias(v.X)
	case *ssa.Convert:
		return alias(v.X)
	case *ssa.MakeInterface:
		return alias(v.X)
	case *ssa.ChangeInterface:
		return alias(v.X)
	case *ssa.TypeAssert:
		return alias(v.X)
	case *ssa.Extract:
		switch tuple := v.Tuple.(type) {
		case *ssa.Call:
			return callAlias(tuple, v.Index)
		case *ssa.Lookup:
			return v.Index == 0 && alias(tuple.X)
		case *ssa.TypeAssert:
			return v.Index == 0 && alias(tuple.X)
		case *ssa.Next:
			if iter, ok := tuple.Iter.(*ssa.Range); ok {
				return v.Index == 2 && alias(iter.X)
			}
		}
	case *ssa.Call:
		return callAlias(v, 0)
	}
	return false
}
