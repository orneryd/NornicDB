package storagecensus

import (
	"go/types"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// These small fixtures exercise the census's actual SSA collector without
// loading the repository or granting any function disposition ownership.
func TestErrorPropagationPreservesJoinedReaderErrors(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 type result struct { value int }
 var state int
 var held func()
 func source() error { return nil }
 func other() error { return nil }
 func body() { state++ }
 func retain(f func()) { held = f }
 func merged(choice bool) error {
  var err error
  if choice { err = source() } else { err = other() }
  if err != nil { return err }
  body(); return nil
 }
 func aggregate(choice bool) (r result, err error) {
  retain(func() { r.value = 1 })
  if choice { err = source() } else { err = other() }
  if err != nil { return r, err }
  body(); return r, nil
 }
 func sideEffect(choice bool) error {
  var err error
  if choice { err = source() } else { err = other() }
  if err != nil { body(); return err }
  body(); return nil
 }
 func overwritten(choice bool) error {
  var err error
  if choice { err = source() } else { err = other() }
  if err != nil { err = other(); return err }
  body(); return nil
 }
 func swallowed(choice bool) error {
  var err error
  if choice { err = source() } else { err = other() }
  if err != nil { return nil }
  body(); return nil
 }
 func aggregateSideEffect(choice bool) (r result, err error) {
  retain(func() { r.value = 1 })
  if choice { err = source() } else { err = other() }
  if err != nil { body(); return r, err }
  body(); return r, nil
 }
 func aggregateMutation(choice bool) (r result, err error) {
  retain(func() { r.value = 1 })
  if choice { err = source() } else { err = other() }
  if err != nil { r.value = 7; return r, err }
  return r, nil
 }
 func aggregateReplacement(choice bool) (r result, err error) {
  retain(func() { r.value = 1 })
  if choice { err = source() } else { err = other() }
  if err != nil { r = result{value: 7}; return r, err }
  return r, nil
 }
 func aggregateOtherCell(choice bool) (r result, err error) {
  otherResult := result{value: 8}
  retain(func() { r.value = 1; otherResult.value = 9 })
  if choice { err = source() } else { err = other() }
  if err != nil { r = otherResult; return r, err }
  return r, nil
 }
 func rewrittenByDefer(choice bool) (err error) {
  defer func() { err = nil }()
  if choice { err = source() } else { err = other() }
  if err != nil { return err }
  body(); return nil
 }
 `, "pkg/storage", ssa.SanityCheckFunctions, standaloneFixture)
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"merged", true},
		{"aggregate", true},
		{"sideEffect", false},
		{"overwritten", false},
		{"swallowed", false},
		{"aggregateSideEffect", false},
		{"aggregateMutation", false},
		{"aggregateReplacement", false},
		{"aggregateOtherCell", false},
		{"rewrittenByDefer", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			var guard *ssa.If
			var checked ssa.Value
			for _, block := range fn.Blocks {
				branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
				if !ok {
					continue
				}
				cmp, ok := branch.Cond.(*ssa.BinOp)
				if !ok {
					continue
				}
				value, _, nilComparison := workcensus.NilComparison(cmp)
				if nilComparison && types.Identical(value.Type(), types.Universe.Lookup("error").Type()) {
					require.Nil(t, guard, "one error guard per fixture")
					guard, checked = branch, value
				}
			}
			require.NotNil(t, guard)
			if tc.name != "rewrittenByDefer" {
				_, joined := checked.(*ssa.Phi)
				require.True(t, joined, "the reader errors must join before the guard")
			}
			if tc.name == "aggregate" {
				// Pin the other production shape: the unchanged error returns
				// beside a load of the captured named aggregate result.
				found := false
				for _, successor := range guard.Block().Succs {
					ret, ok := successor.Instrs[len(successor.Instrs)-1].(*ssa.Return)
					if !ok {
						continue
					}
					for _, result := range ret.Results {
						load, ok := result.(*ssa.UnOp)
						if !ok {
							continue
						}
						_, allocated := load.X.(*ssa.Alloc)
						_, aggregate := load.Type().Underlying().(*types.Struct)
						found = found || allocated && aggregate
					}
				}
				require.True(t, found, "fixture must keep its aggregate result load")
			}
			require.Equal(t, tc.want, (&decider{}).propagatesError(guard))
		})
	}
}

func TestHiddenDecisionDoesNotReachEarlierWritesThroughInitialBackedge(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type BadgerEngine struct{}
 var state int
 func(b *BadgerEngine) filterEntityByDecay() bool { return false }
 func body() { state++ }
 func marker() { state += 2 }
 func afterOnly(b *BadgerEngine, entries []int) {
  for range entries {
   body()
   if b.filterEntityByDecay() { marker() }
  }
 }
 func guardedBody(b *BadgerEngine, entries []int) {
  for range entries {
   if b.filterEntityByDecay() { body() }
  }
 }
 func guardedTraversal(b *BadgerEngine, entries []int) {
  if b.filterEntityByDecay() {
   for range entries { body() }
  }
 }
 `, "pkg/storage", ssa.SanityCheckFunctions, standaloneFixture)
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg != pkg {
			continue
		}
		p.funcs = append(p.funcs, fn)
		if fn.Name() == "body" {
			ins := fn.Blocks[0].Instrs[0]
			a.bodyMarks[ins] = true
			a.bodyChains[ins] = map[string][]string{"body": {p.funcName(fn) + " body"}}
		}
	}
	a.markBodyWriters()
	// Pin the optimized shape which caused the backwards gate: one hidden
	// branch's immediate successor is already a loop-header back edge.
	fn := pkg.Func("afterOnly")
	initialBackedge := false
	for _, block := range fn.Blocks {
		branch, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		call, ok := branch.Cond.(*ssa.Call)
		if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "filterEntityByDecay" {
			continue
		}
		for _, successor := range block.Succs {
			if successor.Dominates(block) {
				initialBackedge = true
			}
		}
		require.Empty(t, (&decider{a: a}).gatedWrites(fn, block), "neither arm decides the body write already executed")
	}
	require.True(t, initialBackedge, "fixture must expose an initial successor backedge")
	decisions := map[string][]Decision{}
	for _, decision := range a.decisions() {
		decisions[decision.Func] = append(decisions[decision.Func], decision)
	}
	require.Empty(t, decisions["pkg/storage.afterOnly"])
	for _, name := range []string{"pkg/storage.guardedBody", "pkg/storage.guardedTraversal"} {
		require.Len(t, decisions[name], 1, "a same-iteration or ordinary loop write stays visible")
		require.True(t, decisions[name][0].unowned, "an actual hidden-state body write must remain rejected")
	}
}
