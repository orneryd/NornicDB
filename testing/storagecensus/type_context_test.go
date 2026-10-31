package storagecensus

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTypeGuardRequiresConcreteWriterContext(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type writer interface{ write() }
 type stagedKV struct{}
 func(*stagedKV)write(){}
 type nativeKV struct{}
 func(*nativeKV)write(){}
 type lookalike struct{}
 func(*lookalike)write(){}
 func rawWrite(){}
 func kernel(w writer){if _,staged:=w.(*stagedKV);!staged{rawWrite()}}
 func staged(){kernel(&stagedKV{})}
 func native(){kernel(&nativeKV{})}
 func unknown(w writer){kernel(w)}
 func mixed(choose bool){var w writer=&stagedKV{};if choose{w=&nativeKV{}};kernel(w)}
 func similar(){kernel(&lookalike{})}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	a := &analysis{p: p, acq: &acquirer{p: p, cache: map[flowKey]*flow{}}}
	kernel := pkg.Func("kernel")
	var facts []typeGuard
	for _, b := range kernel.Blocks {
		for _, ins := range b.Instrs {
			if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == pkg.Func("rawWrite") {
				facts = typeGuardsAt(kernel, ins)
			}
		}
	}
	require.Len(t, facts, 1)
	r := req{space: "prefixNode", class: "entity-body", origin: "typed-context", chain: []string{"kernel typed-context"}, guards: facts}
	calleeSummary := &summary{reqs: map[string]req{r.id(): r}, pends: map[string]pend{}}
	for _, tc := range []struct {
		name string
		want int
	}{{"staged", 0}, {"native", 1}, {"unknown", 1}, {"mixed", 1}, {"similar", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			out := &summary{reqs: map[string]req{}, pends: map[string]pend{}}
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == kernel {
						a.transfer(fn, call, kernel, calleeSummary, call.Common().Args, nil, out, 0)
					}
				}
			}
			require.Len(t, out.reqs, tc.want)
		})
	}
	// A guarded and an unconditional path to one physical write remain distinct.
	plain := r
	plain.guards = nil
	calleeSummary.reqs[plain.id()] = plain
	require.Len(t, calleeSummary.reqs, 2)
}

func TestKernelWriterContextsRemainFailClosed(t *testing.T) {
	abs, err := filepath.Abs(root)
	require.NoError(t, err)
	kernelPath := filepath.Join(abs, "pkg/storage/badger_node_deletion.go")
	kernel, err := os.ReadFile(kernelPath)
	require.NoError(t, err)
	baseOverlay := map[string][]byte{kernelPath: kernel}
	base, err := Census(root, baseOverlay)
	require.NoError(t, err)
	require.Empty(t, base.Violations)
	for _, tc := range []struct{ name, source, entry string }{{"native", `func(b *BadgerEngine) CensusNativeDelete(tx *BadgerTransaction,id NodeID)error{_,err:=b.executeNodeDeletion(tx.badgerTx,id,nil);return err}`, "pkg/storage.BadgerEngine.CensusNativeDelete"}, {"unknown", `func(b *BadgerEngine) CensusUnknownDelete(w kvWriter,id NodeID)error{_,err:=b.executeNodeDeletion(w,id,nil);return err}`, "pkg/storage.BadgerEngine.CensusUnknownDelete"}, {"mixed", `func(b *BadgerEngine) CensusMixedDelete(tx *BadgerTransaction,id NodeID,native bool)error{var w kvWriter=tx.staged();if native{w=tx.badgerTx};_,err:=b.executeNodeDeletion(w,id,nil);return err}`, "pkg/storage.BadgerEngine.CensusMixedDelete"}} {
		t.Run(tc.name, func(t *testing.T) {
			overlay := map[string][]byte{kernelPath: kernel, filepath.Join(abs, "pkg/storage/zz_census_type_context.go"): []byte("package storage\n" + tc.source)}
			report, err := Census(root, overlay)
			require.NoError(t, err)
			found := false
			for _, row := range report.Unordered {
				for _, entry := range row.Entries {
					if entry == tc.entry {
						found = true
					}
				}
			}
			require.True(t, found, "native/unknown/mixed writer entry must still require real write order: %s", strings.Join(report.Violations, "\n"))
		})
	}
	// The real public transaction consumer must not be protected by its name:
	// replacing staged() with the actual raw Badger writer restores violations.
	txPath := filepath.Join(abs, "pkg/storage/badger_transaction.go")
	txSource, err := os.ReadFile(txPath)
	require.NoError(t, err)
	anchor := "tx.engine.executeNodeDeletion(tx.staged(), nodeID, oldNode)"
	require.Equal(t, 1, strings.Count(string(txSource), anchor))
	mutant := strings.Replace(string(txSource), anchor, "tx.engine.executeNodeDeletion(tx.badgerTx, nodeID, oldNode)", 1)
	report, err := Census(root, map[string][]byte{kernelPath: kernel, txPath: []byte(mutant)})
	require.NoError(t, err)
	require.Contains(t, strings.Join(report.Violations, "\n"), "unordered write: pkg/storage.BadgerTransaction.DeleteGraphNode")
}

func TestTypeGuardRequiresActualBranchDependence(t *testing.T) {
	_, pkg := ssaFixture(t, `package storage
 type writer interface{write()};type stagedKV struct{};func(*stagedKV)write(){}
 func rawWrite(){}
 func before(w writer){rawWrite();if _,s:=w.(*stagedKV);s{return}}
 func joined(w writer){if _,s:=w.(*stagedKV);!s{_ = 7};rawWrite()}
 func loop(w writer,again bool){for again{if _,s:=w.(*stagedKV);s{continue};rawWrite();break}}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for _, tc := range []struct {
		name string
		want int
	}{{"before", 0}, {"joined", 0}, {"loop", 1}} {
		fn := pkg.Func(tc.name)
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == pkg.Func("rawWrite") {
					require.Len(t, typeGuardsAt(fn, ins), tc.want, tc.name)
				}
			}
		}
	}
}
