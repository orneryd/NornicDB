package storagecensus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// ssaFixture is the one typed-SSA fixture builder: it parses source as a
// standalone package on the analyzer's real path, with mode selecting the
// builder features a given fixture needs.
func ssaFixture(t *testing.T, source, packagePath string, mode ssa.BuilderMode) (*program, *ssa.Package) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "coverage.go", source, 0)
	require.NoError(t, err)
	pkg, _, err := ssautil.BuildPackage(&types.Config{}, set, types.NewPackage(Module+packagePath, file.Name.Name), []*ast.File{file}, mode)
	require.NoError(t, err)
	return &program{fset: set, ssa: pkg.Prog}, pkg
}

func TestCallbackCoverageRequiresActualNonemptyArgument(t *testing.T) {
	source := `package storage
 type BadgerEngine struct { writeBehind *int }
 func(b *BadgerEngine) noop() error{return nil}
 func run(fn func()error)error{return fn()}
 type writtenEntities struct{}
 type orderHold struct{}
 func (b *BadgerEngine) orderAfterBufferedCommits(o *orderHold,e writtenEntities)(int,error){return 0,nil}
 func(o *orderHold) release(){}
 func(b *BadgerEngine) withUpdate(fn func()error, entities ...writtenEntities)error{
  if len(entities)>0 && b.writeBehind!=nil {
   var order orderHold
   if _,err:=b.orderAfterBufferedCommits(&order,entities[0]);err!=nil{return err}
   defer order.release()
  }
  return run(func()error{return fn()})
 }
 func nonempty(b *BadgerEngine)error{return b.withUpdate(func()error{return b.noop()},writtenEntities{})}
 func empty(b *BadgerEngine)error{return b.withUpdate(func()error{return b.noop()},[]writtenEntities{}...)}
 func omitted(b *BadgerEngine)error{return b.withUpdate(func()error{return b.noop()})}
 func unknown(b *BadgerEngine,entities []writtenEntities)error{return b.withUpdate(func()error{return b.noop()},entities...)}
 func alias(b *BadgerEngine)error{callback:=func()error{return b.noop()};other:=callback;if err:=b.withUpdate(other,writtenEntities{});err!=nil{return err};return callback()}
 func separate(b *BadgerEngine)error{if err:=b.withUpdate(func()error{return b.noop()},writtenEntities{});err!=nil{return err};return b.withUpdate(func()error{return b.noop()})}
 `
	p, pkg := ssaFixture(t, source, "pkg/storage", ssa.SanityCheckFunctions)
	a := &analysis{p: p, acq: &acquirer{p: p, cache: map[flowKey]*flow{}}}
	for _, tc := range []struct {
		name string
		want []bool
	}{
		{"nonempty", []bool{true}}, {"empty", []bool{false}}, {"omitted", []bool{false}}, {"unknown", []bool{false}}, {"alias", []bool{false}}, {"separate", []bool{true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []bool
			fn := pkg.Func(tc.name)
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					if closure, ok := ins.(*ssa.MakeClosure); ok {
						got = append(got, a.closureArgCover(fn, closure)&orderKind != 0)
					}
				}
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestBoundLockHelperRequiresOriginalReceiverMethod(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type BadgerTransaction struct{}
 type Other struct{}
 func(tx *BadgerTransaction) acquireCommitPublicationLocked()error{return nil}
 func(tx *BadgerTransaction) releaseEdgeTypeCountLockLocked(){}
 func(tx *Other) acquireCommitPublicationLocked()error{return nil}
 func sink(){}
 func direct(tx *BadgerTransaction)error{if err:=tx.acquireCommitPublicationLocked();err!=nil{return err};sink();return nil}
 func bound(tx *BadgerTransaction)error{take:=tx.acquireCommitPublicationLocked;if err:=take();err!=nil{return err};sink();return nil}
 func wrongReceiver(tx *Other)error{take:=tx.acquireCommitPublicationLocked;if err:=take();err!=nil{return err};sink();return nil}
 func unused(tx *BadgerTransaction){_ =tx.acquireCommitPublicationLocked;sink()}
 func released(tx *BadgerTransaction)error{take:=tx.acquireCommitPublicationLocked;if err:=take();err!=nil{return err};tx.releaseEdgeTypeCountLockLocked();sink();return nil}
 func ignored(tx *BadgerTransaction){_ = tx.acquireCommitPublicationLocked();sink()}
 func failed(tx *BadgerTransaction){if err:=tx.acquireCommitPublicationLocked();err!=nil{sink()}}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	q := &acquirer{p: p, cache: map[flowKey]*flow{}}
	for _, tc := range []struct {
		name string
		held bool
	}{{"direct", true}, {"bound", true}, {"wrongReceiver", false}, {"unused", false}, {"released", false}, {"ignored", false}, {"failed", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			found := false
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == pkg.Func("sink") {
						found = true
						require.Equal(t, tc.held, q.covers(fn, call).has("prefixMVCCMetaEdgeTypeStartLabelCount"))
					}
				}
			}
			require.True(t, found, "the probe's write boundary must execute")
		})
	}
}

func TestKeyBuilderPhiRetainsEveryArm(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 func label(ns,name string)[]byte{return []byte{1}}
 func edge(ns,name string)[]byte{return []byte{2}}
 func known(kind byte)[]byte { builder:=label;if kind==2{builder=edge};return builder("ns","name") }
 func unknown(kind byte, other func(string,string)[]byte)[]byte { builder:=label;if kind==2{builder=other};return builder("ns","name") }
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for _, tc := range []struct {
		name        string
		wantUnknown bool
	}{{"known", false}, {"unknown", true}} {
		t.Run(tc.name, func(t *testing.T) {
			k := &keys{p: p, first: map[byte]string{1: "prefixNode", 2: "prefixEdge"}, returns: map[*ssa.Function][]keyShape{}, active: map[*ssa.Function]bool{}}
			var shapes []keyShape
			for _, block := range pkg.Func(tc.name).Blocks {
				for _, ins := range block.Instrs {
					if phi, ok := ins.(*ssa.Phi); ok {
						shapes = k.argShapes(keyShape{mode: "call", param: -1, free: -1}, phi, 0)
					}
				}
			}
			require.Len(t, shapes, 2, "both reachable builders must survive")
			names := map[string]bool{}
			unknown := false
			for _, shape := range shapes {
				name, param, why := k.space(shape)
				names[name] = true
				unknown = unknown || why != "" || param >= 0
				require.Len(t, shape.selectors, 1, "each arm retains its actual selector predicate")
			}
			require.True(t, names["prefixNode"])
			require.Equal(t, tc.wantUnknown, unknown)
			if !tc.wantUnknown {
				require.True(t, names["prefixEdge"])
			}
		})
	}
}

func TestSelectorFactRequiresDominatingBranch(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 func sink(){}
 func bypass(kind byte,bypass bool){
  if bypass {goto joined}
  if kind==2 {goto joined}
  return
 joined:
  sink()
 }
 func dominated(kind byte){if kind==2{sink()}}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for _, tc := range []struct {
		name string
		want int
	}{{"bypass", 0}, {"dominated", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			found := false
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == pkg.Func("sink") {
						found = true
						require.Len(t, blockSelectors(fn, block, nil), tc.want)
					}
				}
			}
			require.True(t, found)
		})
	}
}

func TestConditionalShapeIdentityRetainsDistinctSSAValues(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 func first(kind byte){}
 func second(kind byte){}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	first, second := pkg.Func("first").Params[0], pkg.Func("second").Params[0]
	require.Equal(t, first.String(), second.String(), "this regression requires same printed SSA names")
	a := bytesShape([]byteSrc{{known: true, val: 1, param: -1}})
	a.selectors = []selectorFact{{first, 2, token.EQL}}
	b := a
	b.selectors = []selectorFact{{second, 2, token.EQL}}
	c := a
	c.selectors = []selectorFact{{first, 2, token.NEQ}}
	shapes := dedupShapes([]keyShape{a, b, c, a})
	require.Len(t, shapes, 3, "identical keys with different selector identities or predicates remain separate")
	require.NotEqual(t, (pend{shape: a}).id(), (pend{shape: b}).id(), "pending summaries retain the same actual SSA distinction")
	require.NotEqual(t, (pend{shape: a}).id(), (pend{shape: c}).id())
}
