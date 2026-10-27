package workcensus

import (
	"go/token"
	"golang.org/x/tools/go/ssa"
)

// ImmutableCellValueAt returns the one value stored in a local cell before at,
// provided no other write or opaque address use can change it. Read-only
// captures are allowed; a mutated, aliased or externally passed cell is unknown.
// It is a def/use fact, not a lifetime or acquisition witness.
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
	return store.Val, true
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
