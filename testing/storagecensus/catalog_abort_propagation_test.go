package storagecensus

import (
	"go/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

// catalogAbortProgram loads only the catalog owner's package with the census's
// own build settings. The proof needs the real *storage.BadgerTransaction and
// the real (*multidb.DatabaseManager).catalogGate, which the in-memory
// single-package ssaFixture cannot supply because it has no storage or multidb
// identity to resolve; the rest of the module is not needed, so ./pkg/multidb is
// the whole population this proof loads. An overlay plants the near-miss
// fixtures in that package, so they see the same compiler identities.
func catalogAbortProgram(t *testing.T, overlay map[string][]byte) *workcensus.Program {
	t.Helper()
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	loaded, err := workcensus.LoadProgram(workcensus.Config{
		Dir:        abs,
		Patterns:   []string{"./pkg/multidb"},
		BuildFlags: []string{"-tags=noui,nolocalllm"},
		LoadMode:   packages.NeedTypesSizes,
		Env:        append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "GOFLAGS=-mod=mod"),
		Overlay:    overlay,
	})
	require.NoError(t, err)
	return loaded
}

// catalogAbortFunction finds a function or method of the loaded program by name.
func catalogAbortFunction(t *testing.T, loaded *workcensus.Program, name string) *ssa.Function {
	t.Helper()
	var found *ssa.Function
	for _, fn := range loaded.Functions {
		if fn.Parent() == nil && fn.Name() == name {
			require.Nil(t, found, "duplicate function %q in the loaded program", name)
			found = fn
		}
	}
	require.NotNil(t, found, "function %q is not in the loaded program", name)
	return found
}

// catalogAbortArms are every nil test of an error in fn, from the SSA shape
// alone: the block holding the test, the arm taken when the error is not nil,
// and the tested error.
func catalogAbortArms(t *testing.T, fn *ssa.Function) (tests, arms []*ssa.BasicBlock, errv []ssa.Value) {
	t.Helper()
	for _, block := range fn.Blocks {
		if len(block.Instrs) == 0 || len(block.Succs) != 2 {
			continue
		}
		iff, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
		if !ok {
			continue
		}
		tested, nilIndex, ok := workcensus.NilComparison(iff.Cond)
		if !ok || tested == nil || !types.Identical(tested.Type(), types.Universe.Lookup("error").Type()) {
			continue
		}
		tests = append(tests, block)
		arms = append(arms, block.Succs[1-nilIndex])
		errv = append(errv, tested)
	}
	return tests, arms, errv
}

// catalogAbortCallsRollback reports whether a block calls a method named
// Rollback. The tests classify the shipped arms with this name alone, so the
// classification cannot borrow the helper's identity checks.
func catalogAbortCallsRollback(arm *ssa.BasicBlock) bool {
	for _, ins := range arm.Instrs {
		call, ok := ins.(*ssa.Call)
		if !ok {
			continue
		}
		callee := call.Call.StaticCallee()
		if callee == nil {
			continue
		}
		if object, ok := callee.Object().(*types.Func); ok && object.Name() == "Rollback" {
			return true
		}
	}
	return false
}

// TestCatalogAbortRecognizesProductionArm proves the helper accepts exactly the
// shipped catalog transition's rollback arms, and claims none of the arms that
// release the gate and return the error unchanged: those stay with the pure
// propagation owner. The population is the current source of
// pkg/multidb/catalog_transition.go, so the counts pin that shape.
func TestCatalogAbortRecognizesProductionArm(t *testing.T) {
	loaded := catalogAbortProgram(t, nil)
	fn := catalogAbortFunction(t, loaded, "transitionCatalog")
	_, arms, errv := catalogAbortArms(t, fn)
	require.Len(t, errv, 11, "catalog_transition.go's eleven failed-operation arms")
	rollbacks, pure := 0, 0
	for i, arm := range arms {
		accepted := catalogAbortReturnsExact(arm, errv[i])
		if catalogAbortCallsRollback(arm) {
			call, _ := errv[i].(*ssa.Call)
			if tuple, ok := errv[i].(*ssa.Extract); ok {
				call, _ = tuple.Tuple.(*ssa.Call)
			}
			if call != nil && call.Common().StaticCallee() != nil && call.Common().StaticCallee().Name() == "NodeWriteInput" {
				pure++
				require.False(t, accepted, "input normalization is not a failed transaction operation")
				continue
			}
			rollbacks++
			require.True(t, accepted, "the rollback arm at block %d must be recognized", arm.Index)
			continue
		}
		pure++
		require.False(t, accepted, "the arm at block %d releases and returns the error alone: not this proof", arm.Index)
	}
	require.Equal(t, 3, rollbacks, "three failed transaction operations are bound to their rollback receiver")
	require.Equal(t, 8, pure, "normalization and pure release arms remain outside this proof")
}

// catalogAbortFixtureSource mirrors the shipped abort arm and the near misses
// that must stay decisions. The positive cases are the shipped shape: the
// canonical storage transaction, the local release closure, and the unchanged
// error. Every call and field below is the real one; only the surrounding
// function is a fixture.
const catalogAbortFixtureSource = `package multidb

import (
	"fmt"
 "context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func zzCatalogAbortTouch(*DatabaseManager) {}

type zzCatalogAbortTransaction struct{}

func (zzCatalogAbortTransaction) Rollback() error { return nil }

type zzCatalogAbortRollbacker interface{ Rollback() error }

func (m *DatabaseManager) zzCatalogAbortWrongTransaction(tx, other *storage.BadgerTransaction) error {
 <-m.catalogGate
 release:=func(){m.catalogGate <-struct{}{}}
 if err:=tx.SetNamespace("system");err!=nil {
  _=other.Rollback()
  release()
  return err
 }
 release()
 return nil
}

func (m *DatabaseManager) zzCatalogAbortPositive(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortPositiveDirect(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	return nil
}

func (m *DatabaseManager) zzCatalogAbortPositiveTuple(tx *storage.BadgerTransaction, prepare func() (int, error)) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if _, err := tx.CreateNode(context.Background(),nil); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortLookalike(tx *zzCatalogAbortTransaction, source *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := source.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortInterface(tx zzCatalogAbortRollbacker, source *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := source.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortOtherReceiver(other *DatabaseManager, tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { other.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortExtraCall(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		zzCatalogAbortTouch(m)
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortWrite(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		m.serverID = ""
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortSwallow(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return nil
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortReplace(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return storage.ErrInvalidData
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortOverwrite(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		err = storage.ErrInvalidData
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortWrap(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return fmt.Errorf("catalog abort: %w", err)
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortWrongChannel(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	gate := make(chan struct{}, 1)
	release := func() { gate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortTwoSends(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{}; m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortReleaseTwice(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortDeferredRollback(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		defer tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortNoRelease(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func (m *DatabaseManager) zzCatalogAbortInlineSend(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		m.catalogGate <- struct{}{}
		return err
	}
	return nil
}

func (m *DatabaseManager) zzCatalogAbortMutableBinding(tx *storage.BadgerTransaction, run func() error, replace bool) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if replace {
		release = func() { m.catalogGate <- struct{}{} }
	}
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortClosureExtra(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { zzCatalogAbortTouch(m); m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortClosureArg(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func(token struct{}) { m.catalogGate <- token }
	if err := tx.SetNamespace("system"); err != nil {
		_ = tx.Rollback()
		release(struct{}{})
		return err
	}
	release(struct{}{})
	return nil
}

func (m *DatabaseManager) zzCatalogAbortWrongOrder(tx *storage.BadgerTransaction, run func() error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err := tx.SetNamespace("system"); err != nil {
		release()
		_ = tx.Rollback()
		return err
	}
	release()
	return nil
}

func (m *DatabaseManager) zzCatalogAbortParameterError(tx *storage.BadgerTransaction, err error) error {
	<-m.catalogGate
	release := func() { m.catalogGate <- struct{}{} }
	if err != nil {
		_ = tx.Rollback()
		release()
		return err
	}
	release()
	return nil
}
`

// TestCatalogAbortRejectsLookalikes proves the helper's identity checks: the
// shipped shape is accepted, and a wrong rollback receiver type, an interface
// dispatch, another receiver, another channel, a second release, a rebound or
// extended closure, extra work in the arm, and a swallowed, replaced, rewritten
// or caller-supplied error are all refused.
func TestCatalogAbortRejectsLookalikes(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	plant := filepath.Join(abs, "pkg", "multidb", "zz_catalog_abort_fixture.go")
	loaded := catalogAbortProgram(t, map[string][]byte{plant: []byte(catalogAbortFixtureSource)})
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"zzCatalogAbortPositive", true},
		{"zzCatalogAbortPositiveDirect", true},
		{"zzCatalogAbortPositiveTuple", true},
		{"zzCatalogAbortWrongTransaction", false},
		{"zzCatalogAbortLookalike", false},
		{"zzCatalogAbortInterface", false},
		{"zzCatalogAbortOtherReceiver", false},
		{"zzCatalogAbortExtraCall", false},
		{"zzCatalogAbortWrite", false},
		{"zzCatalogAbortSwallow", false},
		{"zzCatalogAbortReplace", false},
		{"zzCatalogAbortOverwrite", false},
		{"zzCatalogAbortWrap", false},
		{"zzCatalogAbortWrongChannel", false},
		{"zzCatalogAbortTwoSends", false},
		{"zzCatalogAbortReleaseTwice", false},
		{"zzCatalogAbortDeferredRollback", false},
		{"zzCatalogAbortNoRelease", false},
		{"zzCatalogAbortInlineSend", false},
		{"zzCatalogAbortMutableBinding", false},
		{"zzCatalogAbortClosureExtra", false},
		{"zzCatalogAbortClosureArg", false},
		{"zzCatalogAbortWrongOrder", false},
		{"zzCatalogAbortParameterError", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := catalogAbortFunction(t, loaded, tc.name)
			_, arms, errv := catalogAbortArms(t, fn)
			require.Len(t, arms, 1, "the fixture has exactly one failed-operation test")
			require.Equal(t, tc.want, catalogAbortReturnsExact(arms[0], errv[0]))
		})
	}
}

// TestCatalogAbortRequiresAbortArm proves the helper does not claim the arm
// that runs when the error is nil: only the non-nil successor is the abort.
func TestCatalogAbortRequiresAbortArm(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	plant := filepath.Join(abs, "pkg", "multidb", "zz_catalog_abort_fixture.go")
	loaded := catalogAbortProgram(t, map[string][]byte{plant: []byte(catalogAbortFixtureSource)})
	fn := catalogAbortFunction(t, loaded, "zzCatalogAbortPositive")
	_, arms, errv := catalogAbortArms(t, fn)
	require.Len(t, arms, 1)
	test := arms[0].Preds[0]
	iff, ok := test.Instrs[len(test.Instrs)-1].(*ssa.If)
	require.True(t, ok)
	_, nilIndex, ok := workcensus.NilComparison(iff.Cond)
	require.True(t, ok)
	nilArm := test.Succs[nilIndex]
	require.NotSame(t, arms[0], nilArm)
	require.False(t, catalogAbortReturnsExact(nilArm, errv[0]), "the nil successor is not the abort arm")
	require.False(t, catalogAbortReturnsExact(nil, errv[0]), "no block claims nothing")
	require.False(t, catalogAbortReturnsExact(arms[0], nil), "no error claims nothing")
}
