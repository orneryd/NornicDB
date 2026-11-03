package workcensus

import (
	"go/token"
	"golang.org/x/tools/go/ssa"
)

// ImmutableCellValueAt returns the one value stored in a local cell before at,
// provided no other write or opaque address use can change it and that store
// cannot run again on the same cell after at. Read-only captures are allowed; a
// mutated, aliased or externally passed cell is unknown, and so is a store a
// later iteration of one loop reaches on the cell it stored. It is a def/use
// fact, not a lifetime or acquisition witness.
func ImmutableCellValueAt(value ssa.Value, at ssa.Instruction) (ssa.Value, bool) {
	cell, ok := value.(*ssa.Alloc)
	if !ok || at == nil || cell.Parent() != at.Parent() || cell.Referrers() == nil {
		return nil, false
	}
	var store *ssa.Store
	for _, ref := range *cell.Referrers() {
		s, ok := ref.(*ssa.Store)
		if !ok || s.Addr != cell {
			continue
		}
		if store != nil {
			return nil, false
		}
		store = s
	}
	if store == nil || !store.Block().Dominates(at.Block()) {
		return nil, false
	}
	if store.Block() == at.Block() {
		before := false
		for _, ins := range at.Block().Instrs {
			if ins == at {
				break
			}
			if ins == store {
				before = true
			}
		}
		if !before {
			return nil, false
		}
	}
	if !readOnlyCellUses(cell, store, map[ssa.Value]bool{}) {
		return nil, false
	}
	// One Store is not one dynamic write. A store in a loop runs again, and a
	// reader that captured the cell reads it later, so a re-run store could
	// replace the value that reader bound, even when the store dominates the
	// capture.
	if cellRewrittenAfter(cell, store, at) {
		return nil, false
	}
	return store.Val, true
}

// cellRewrittenAfter reports whether the store can run again on the same
// allocated cell after at: a control-flow path from at back to that store that
// does not execute the cell's Alloc first. A cell declared outside a loop has
// one store that every iteration runs and no Alloc between two of its runs, so
// the iteration that captured an accepted value can be followed by an iteration
// that stores nil into the very cell that capture bound. A path through the
// Alloc allocates a new cell, so a store there writes a different object and
// cannot change the value an earlier capture read.
func cellRewrittenAfter(cell *ssa.Alloc, store *ssa.Store, at ssa.Instruction) bool {
	type point struct {
		block *ssa.BasicBlock
		index int
	}
	start := -1
	for i, ins := range at.Block().Instrs {
		if ins == at {
			start = i
			break
		}
	}
	if start < 0 {
		return true
	}
	pending := []point{{at.Block(), start}}
	seen := map[point]bool{}
	for len(pending) > 0 {
		next := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[next] {
			continue
		}
		seen[next] = true
		fresh := false
		for i := next.index; i < len(next.block.Instrs); i++ {
			ins := next.block.Instrs[i]
			if ins == cell {
				fresh = true
			}
			if ins == store && !fresh {
				return true
			}
		}
		// A path that allocated again stays on a newer cell, so every later
		// store it runs writes an object no earlier capture holds.
		if !fresh {
			for _, succ := range next.block.Succs {
				pending = append(pending, point{succ, 0})
			}
		}
	}
	return false
}

func readOnlyCellUses(value ssa.Value, initial *ssa.Store, seen map[ssa.Value]bool) bool {
	if seen[value] {
		return true
	}
	seen[value] = true
	if value.Referrers() == nil {
		return false
	}
	for _, ref := range *value.Referrers() {
		switch r := ref.(type) {
		case *ssa.DebugRef:
		case *ssa.UnOp:
			// Only an actual dereference reads the cell without forwarding it.
			if r.Op != token.MUL || r.X != value {
				return false
			}
		case *ssa.Store:
			if r != initial || r.Addr != value {
				return false
			}
		case *ssa.MakeClosure:
			fn, ok := r.Fn.(*ssa.Function)
			if !ok {
				return false
			}
			for i, binding := range r.Bindings {
				if binding == value && (i >= len(fn.FreeVars) || !readOnlyCellUses(fn.FreeVars[i], nil, seen)) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}
