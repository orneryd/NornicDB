package storagecensus

// Author: #1667 implementation lane (Codex). All fixture source is loaded by
// the shared engine; the actual domain acquisition-flow oracle checks effects.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

func TestRetainedCancellableLockCoverage(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module "+strings.TrimSuffix(Module, "/")+"\n\ngo 1.26\n"), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "pkg/storage"), 0700))
	source := `package storage
 import "context"
 type trackedRWMutex struct{}
 func (*trackedRWMutex) LockContext(context.Context)error{return nil}
 func (*trackedRWMutex) RLockContext(context.Context)error{return nil}
 func (*trackedRWMutex) Lock(){}
 func (*trackedRWMutex) Unlock(){}
 type stranger struct{}
 func (*stranger) LockContext(context.Context)error{return nil}
 func (*stranger) Unlock(){}
 type BadgerEngine struct{labelCountWriteMu,edgeTypeCountWriteMu,suppressionWriteMu trackedRWMutex;other stranger}
 func effect(){}
 func held(b *BadgerEngine,ctx context.Context)error{if err:=b.labelCountWriteMu.LockContext(ctx);err!=nil{return err};defer b.labelCountWriteMu.Unlock();effect();return nil}
 func legacy(b *BadgerEngine){b.labelCountWriteMu.Lock();defer b.labelCountWriteMu.Unlock();effect()}
 func unchecked(b *BadgerEngine,ctx context.Context){b.labelCountWriteMu.LockContext(ctx);effect()}
 func failed(b *BadgerEngine,ctx context.Context)error{if err:=b.labelCountWriteMu.LockContext(ctx);err==nil{return nil};effect();return nil}
 func released(b *BadgerEngine,ctx context.Context)error{if err:=b.labelCountWriteMu.LockContext(ctx);err!=nil{return err};b.labelCountWriteMu.Unlock();effect();return nil}
 func otherField(b *BadgerEngine,ctx context.Context)error{if err:=b.edgeTypeCountWriteMu.LockContext(ctx);err!=nil{return err};defer b.edgeTypeCountWriteMu.Unlock();effect();return nil}
 func wrongType(b *BadgerEngine,ctx context.Context)error{if err:=b.other.LockContext(ctx);err!=nil{return err};defer b.other.Unlock();effect();return nil}
 func sharedOnly(b *BadgerEngine,ctx context.Context)error{if err:=b.labelCountWriteMu.RLockContext(ctx);err!=nil{return err};effect();return nil}
 func bypass(b *BadgerEngine,ctx context.Context,skip bool)error{if !skip{if err:=b.labelCountWriteMu.LockContext(ctx);err!=nil{return err}};effect();return nil}
 `
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pkg/storage/fixture.go"), []byte(source), 0600))
	loaded, err := workcensus.LoadProgram(workcensus.Config{Dir: dir, Patterns: []string{"./pkg/storage"}})
	require.NoError(t, err)
	p := &program{root: loaded.Root, fset: loaded.FileSet, ssa: loaded.SSA, funcs: loaded.Functions, cg: loaded.CallGraph}
	q := &acquirer{p: p, cache: map[flowKey]*flow{}}
	for _, probe := range []struct {
		name string
		held bool
	}{{"held", true}, {"legacy", true}, {"unchecked", false}, {"failed", false}, {"released", false}, {"otherField", false}, {"wrongType", false}, {"sharedOnly", false}, {"bypass", false}} {
		t.Run(probe.name, func(t *testing.T) {
			found := false
			for _, fn := range loaded.Functions {
				if fn.Name() != probe.name {
					continue
				}
				for _, block := range fn.Blocks {
					for _, ins := range block.Instrs {
						if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() != nil && call.Common().StaticCallee().Name() == "effect" {
							found = true
							require.Equal(t, probe.held, q.covers(fn, call).has("prefixMVCCMetaLabelCount"), "only a checked exclusive hold on the same retained field covers this count effect")
						}
					}
				}
			}
			require.True(t, found)
		})
	}
}
