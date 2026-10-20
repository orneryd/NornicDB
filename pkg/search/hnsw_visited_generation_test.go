package search

import "testing"

func TestHNSWVisitedGenerationPreservesAndClearsStamps(t *testing.T) {
	var state visitedGenState
	if generation := state.next(4); generation != 1 || len(state.gen) != 4 {
		t.Fatalf("initial state=%+v", state)
	}
	state.gen[1] = state.cur
	if generation := state.next(7); generation != 2 || state.gen[1] != 1 {
		t.Fatalf("growth lost prior stamps: %+v", state)
	}
	for i := 4; i < 7; i++ {
		if state.gen[i] != 0 {
			t.Fatalf("new slot %d has stamp %d", i, state.gen[i])
		}
	}
	state.cur = ^uint16(0)
	for i := range state.gen {
		state.gen[i] = state.cur
	}
	if generation := state.next(7); generation != 1 {
		t.Fatalf("wrap generation=%d", generation)
	}
	for i, stamp := range state.gen {
		if stamp != 0 {
			t.Fatalf("wrap left stamp %d at %d", stamp, i)
		}
	}
	if generation := state.next(3); generation != 2 || len(state.gen) != 7 {
		t.Fatalf("smaller search must retain reusable storage: %+v", state)
	}
}

func TestHNSWVisitedGenerationGrowthIsAmortized(t *testing.T) {
	allocations := testing.AllocsPerRun(10, func() {
		var state visitedGenState
		for size := 1; size <= 1024; size++ {
			state.next(size)
		}
		if len(state.gen) != 1024 || state.cur != 1024 {
			panic("incorrect generation state")
		}
	})
	// Allow growth-policy variation while rejecting one allocation per insert.
	if allocations > 32 {
		t.Fatalf("1024 successive insert sizes allocated %g buffers; want amortized growth (<=32)", allocations)
	}
}
