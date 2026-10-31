package workcensus

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"go/token"

	"golang.org/x/tools/go/ssa"
)

// ParameterNilAt reports that block runs only where param is known nil:
// param itself is tested nil there (NilAt), or, for a parameter a closure
// captures (SSA spills it to a cell), a load of that cell whose only reaching
// definition is the parameter's store is.
func ParameterNilAt(param *ssa.Parameter, block *ssa.BasicBlock) bool {
	if NilAt(param, block) {
		return true
	}
	if param.Referrers() == nil {
		return false
	}
	for _, ref := range *param.Referrers() {
		store, ok := ref.(*ssa.Store)
		if !ok || store.Val != param {
			continue
		}
		cell, ok := store.Addr.(*ssa.Alloc)
		if !ok || cell.Referrers() == nil {
			continue
		}
		for _, use := range *cell.Referrers() {
			load, ok := use.(*ssa.UnOp)
			if !ok || load.Op != token.MUL || !NilAt(load, block) {
				continue
			}
			defs := reachingDefinitions(load, cell, nil)
			only := len(defs) > 0
			for _, d := range defs {
				if d.store != store {
					only = false
				}
			}
			if only {
				return true
			}
		}
	}
	return false
}

// StaticCall reports that caller calls callee directly (a static callee),
// not only through an interface or a function value.
func StaticCall(caller, callee *ssa.Function) bool {
	for _, block := range caller.Blocks {
		for _, instr := range block.Instrs {
			if call, ok := instr.(ssa.CallInstruction); ok && call.Common().StaticCallee() == callee {
				return true
			}
		}
	}
	for _, anon := range caller.AnonFuncs {
		if StaticCall(anon, callee) {
			return true
		}
	}
	return false
}
