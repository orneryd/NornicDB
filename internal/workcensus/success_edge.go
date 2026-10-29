package workcensus

// Author: #1667 implementation lane (Codex). Generic typed SSA success facts.

import (
	"go/token"
	"go/types"
	"golang.org/x/tools/go/ssa"
)

// SuccessfulCallEdge is the branch taken when the call's result errIndex is nil:
// the If of "err != nil" (or "== nil") and its successor without an error.
func SuccessfulCallEdge(call *ssa.Call, errIndex int) (*ssa.If, *ssa.BasicBlock) {
	if call == nil || errIndex < 0 || call.Referrers() == nil {
		return nil, nil
	}
	// A call with one result (the error) is compared itself.
	if _, tuple := call.Type().(*types.Tuple); !tuple && errIndex == 0 {
		return NilSuccessEdge(call)
	}
	for _, ref := range *call.Referrers() {
		ex, ok := ref.(*ssa.Extract)
		if !ok || ex.Index != errIndex {
			continue
		}
		// The error, or (a named result a deferred closure captures lives
		// in a cell) a load of the cell it is stored in.
		errs := []ssa.Value{ex}
		for _, r := range *ex.Referrers() {
			if st, ok := r.(*ssa.Store); ok && st.Val == ex {
				if cell, ok := st.Addr.(*ssa.Alloc); ok {
					for _, u := range *cell.Referrers() {
						if load, ok := u.(*ssa.UnOp); ok && load.Op == token.MUL && st.Block().Dominates(load.Block()) && cellContainsAt(cell, load, ex) {
							errs = append(errs, load)
						}
					}
				}
			}
		}
		for _, e := range errs {
			if iff, succ := NilSuccessEdge(e); succ != nil {
				return iff, succ
			}
		}
	}
	return nil, nil
}

// cellContainsAt checks reaching stores at this load, not stores after the
// check. Named result cells can legitimately be reused later or by a defer.
// A clobber on even one incoming path cannot prove this call succeeded.
func cellContainsAt(cell *ssa.Alloc, load ssa.Instruction, wanted ssa.Value) bool {
	type point struct {
		block *ssa.BasicBlock
		index int
	}
	index := -1
	for i, instruction := range load.Block().Instrs {
		if instruction == load {
			index = i - 1
			break
		}
	}
	pending := []point{{load.Block(), index}}
	seen := map[point]bool{}
	for len(pending) > 0 {
		at := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[at] {
			continue
		}
		seen[at] = true
		stored := false
		for i := at.index; i >= 0; i-- {
			instruction := at.block.Instrs[i]
			if store, ok := instruction.(*ssa.Store); ok && store.Addr == cell {
				if store.Val != wanted {
					return false
				}
				stored = true
				break
			}
			if call, ok := instruction.(*ssa.Call); ok {
				for _, argument := range call.Common().Args {
					if argument == cell {
						return false
					}
				}
				if closure, ok := call.Common().Value.(*ssa.MakeClosure); ok {
					for _, binding := range closure.Bindings {
						if binding == cell {
							return false
						}
					}
				}
			}
		}
		if !stored {
			if len(at.block.Preds) == 0 {
				return false
			}
			for _, predecessor := range at.block.Preds {
				pending = append(pending, point{predecessor, len(predecessor.Instrs) - 1})
			}
		}
	}
	return true
}

// NilSuccessEdge returns the If comparing e with nil and its nil successor.
// This is a structural SSA fact, not proof of admission, dominance or lifetime.
// The input must be a valid SSA value with a referrer list.
func NilSuccessEdge(e ssa.Value) (*ssa.If, *ssa.BasicBlock) {
	{
		ex := e
		for _, r := range *ex.Referrers() {
			cmp, ok := r.(*ssa.BinOp)
			if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
				continue
			}
			other := cmp.Y
			if cmp.Y == ex {
				other = cmp.X
			}
			if c, ok := other.(*ssa.Const); !ok || !c.IsNil() {
				continue
			}
			for _, u := range *cmp.Referrers() {
				iff, ok := u.(*ssa.If)
				if !ok || len(iff.Block().Succs) != 2 {
					continue
				}
				if cmp.Op == token.NEQ {
					return iff, iff.Block().Succs[1]
				}
				return iff, iff.Block().Succs[0]
			}
		}
	}
	return nil, nil
}
