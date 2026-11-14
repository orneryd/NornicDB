package storagecensus

import (
	"go/constant"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// wrappedReadFixture is the engine's error-return shapes around a read. It
// imports the real fmt, so the positive shape is the actual fmt.Errorf the
// census trusts; fixtureWrap keeps the identical lowered call shape under a
// local stand-in, which must stay a decision.
const wrappedReadFixture = `package storage
 import "fmt"
 var state int
 func source() error { return nil }
 func other() error { return nil }
 func body() { state++ }
 func fixtureWrap(format string, args ...any) error { return nil }
 func raw() error { err := source(); if err != nil { return err }; body(); return nil }
 func wrapped() error { err := source(); if err != nil { return fmt.Errorf("catalog: %w", err) }; body(); return nil }
 func lookalike() error { err := source(); if err != nil { return fixtureWrap("catalog: %w", err) }; body(); return nil }
 func verbOnly() error { err := source(); if err != nil { return fmt.Errorf("catalog: %v", err) }; body(); return nil }
 func otherError() error { err := source(); if err != nil { return fmt.Errorf("catalog: %w", other()) }; body(); return nil }
 func twoWraps() error { err := source(); if err != nil { return fmt.Errorf("catalog: %w: %w", err, other()) }; body(); return nil }
 func extraArgument() error { err := source(); if err != nil { return fmt.Errorf("catalog: %w", err, state) }; body(); return nil }
 func sparseArgument() error { err := source(); if err != nil { var args [2]any; args[1] = err; return fmt.Errorf("catalog: %w", args[:]...) }; body(); return nil }
 func branchEffect() error { err := source(); if err != nil { body(); return fmt.Errorf("catalog: %w", err) }; body(); return nil }
 func branchStore(p *int) error { err := source(); if err != nil { *p = 1; return fmt.Errorf("catalog: %w", err) }; return nil }
 func overwritten() error { err := source(); if err != nil { err = other(); return fmt.Errorf("catalog: %w", err) }; body(); return nil }
 func swallowed() error { err := source(); if err != nil { return nil }; body(); return nil }
 type BadgerEngine struct{}
 func (b *BadgerEngine) filterEntityByDecay() bool { return false }
 func valueGates(value int) { if value != 0 { body() } }
 func suppressionGates(b *BadgerEngine) { if b.filterEntityByDecay() { body() } }
`

// fixtureBranch is the one branch in fn: every fixture function holds exactly
// one decision point.
func fixtureBranch(t *testing.T, fn *ssa.Function) *ssa.If {
	t.Helper()
	var branch *ssa.If
	for _, block := range fn.Blocks {
		if iff, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If); ok {
			require.Nil(t, branch, "one branch per fixture")
			branch = iff
		}
	}
	require.NotNil(t, branch, "fixture must keep its branch")
	return branch
}

// fixtureValue is the value a variadic element holds: a concrete value converts
// to the empty interface, while an error changes interface.
func fixtureValue(value ssa.Value) (ssa.Value, bool) {
	switch value := value.(type) {
	case *ssa.MakeInterface:
		return value.X, true
	case *ssa.ChangeInterface:
		return value.X, true
	}
	return nil, false
}

// fixtureWrapCall reads back what the lowered variadic wrap call named callee
// receives in a branch arm: its static callee, its constant format and the
// values stored into the variadic array. It pins the fixture shape rather than
// production behavior.
func fixtureWrapCall(t *testing.T, branch *ssa.If, callee string) (*ssa.Function, string, []ssa.Value) {
	t.Helper()
	for _, successor := range branch.Block().Succs {
		for _, ins := range successor.Instrs {
			call, ok := ins.(*ssa.Call)
			if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != callee {
				continue
			}
			format, ok := call.Common().Args[0].(*ssa.Const)
			require.True(t, ok, "the wrap format is a constant")
			slice, ok := call.Common().Args[1].(*ssa.Slice)
			require.True(t, ok, "the wrap arguments are one variadic slice")
			array, ok := slice.X.(*ssa.Alloc)
			require.True(t, ok, "the variadic slice holds the fixture's values")
			var values []ssa.Value
			for _, element := range successor.Instrs {
				store, ok := element.(*ssa.Store)
				if !ok {
					continue
				}
				index, ok := store.Addr.(*ssa.IndexAddr)
				if !ok || index.X != array {
					continue
				}
				value, ok := fixtureValue(store.Val)
				require.True(t, ok, "a variadic element holds an interface value")
				values = append(values, value)
			}
			return call.Common().StaticCallee(), constant.StringVal(format.Value), values
		}
	}
	require.Fail(t, "fixture must reach its wrap call from the branch")
	return nil, "", nil
}

// TestWrappedReadErrorPropagation keeps a pure fmt.Errorf wrap of the read
// error the census's propagation case and every other shape around it a
// decision.
func TestWrappedReadErrorPropagation(t *testing.T) {
	p, pkg := ssaFixture(t, wrappedReadFixture, "pkg/storage", ssa.SanityCheckFunctions, standaloneFixture)
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
	d := &decider{a: a}
	for _, tc := range []struct {
		name  string
		want  bool
		gates bool
	}{
		{"raw", true, true},
		{"wrapped", true, true},
		{"lookalike", false, true},
		{"verbOnly", false, false},
		{"otherError", false, false},
		{"twoWraps", false, false},
		{"extraArgument", false, false},
		{"sparseArgument", false, false},
		{"branchEffect", false, false},
		{"branchStore", false, false},
		{"overwritten", false, false},
		{"swallowed", false, false},
		{"valueGates", false, true},
		{"suppressionGates", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			require.NotNil(t, fn)
			branch := fixtureBranch(t, fn)
			errv, _, nilComparison := workcensus.NilComparison(branch.Cond)
			if tc.gates {
				// A branch rejected here and gating a body write becomes a
				// hidden disposition the census reports.
				require.NotEmpty(t, d.gatedWrites(fn, branch.Block()), "fixture branch must gate a body write")
			}
			switch tc.name {
			case "wrapped":
				callee, format, values := fixtureWrapCall(t, branch, "Errorf")
				require.Equal(t, "fmt", callee.Pkg.Pkg.Path(), "the trusted wrapper is the imported fmt.Errorf")
				require.True(t, nilComparison, "the guard compares the read error with nil")
				require.Equal(t, "catalog: %w", format, "one literal %w in the format")
				require.Len(t, values, 1, "the read error is the sole argument")
				require.True(t, values[0] == errv, "the wrap must carry the exact read error")
			case "lookalike":
				callee, format, values := fixtureWrapCall(t, branch, "fixtureWrap")
				require.NotEqual(t, "fmt", callee.Pkg.Pkg.Path(), "a local wrapper keeps the shape but not the identity")
				require.Equal(t, "catalog: %w", format, "only the callee identity differs from wrapped")
				require.Len(t, values, 1)
				require.True(t, values[0] == errv)
			case "verbOnly":
				_, format, values := fixtureWrapCall(t, branch, "Errorf")
				require.Equal(t, "catalog: %v", format, "only the verb differs from wrapped")
				require.Len(t, values, 1)
				require.True(t, values[0] == errv, "the verb, not the argument, must decide this case")
			case "otherError":
				_, format, values := fixtureWrapCall(t, branch, "Errorf")
				require.Equal(t, "catalog: %w", format)
				require.Len(t, values, 1)
				require.False(t, values[0] == errv, "a wrap of another error is not this read's propagation")
			case "twoWraps", "extraArgument":
				_, _, values := fixtureWrapCall(t, branch, "Errorf")
				require.Len(t, values, 2, "two formatted arguments are not one read error")
			case "branchEffect", "branchStore":
				_, format, values := fixtureWrapCall(t, branch, "Errorf")
				require.Equal(t, "catalog: %w", format)
				require.Len(t, values, 1)
				require.True(t, values[0] == errv)
			case "overwritten":
				_, format, values := fixtureWrapCall(t, branch, "Errorf")
				require.Equal(t, "catalog: %w", format)
				require.Len(t, values, 1)
				require.False(t, values[0] == errv, "the branch overwrote the read error before wrapping")
			}
			require.Equal(t, tc.want, d.propagatesError(branch),
				"propagatesError must report whether this branch only fails with the read")
		})
	}
}
