package storagecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Author: #1667 implementation lane (Codex). The sealed fixture is the generic
// local-mutex/once/closed lifetime used by #1548's publication decision consumer.
// It is data supplied to the same engine loader and storage effect interpreter.
const callbackFrameFixture = `package callbackfixture
import "sync"
var kept func() error
func effect() {}
func Guarded(route func(func() error) error, action func() error) error {
 var mu sync.Mutex
 called, closed := false, false
 _ = closed; _ = called
 var outcome error
 guarded := func() error {
  mu.Lock()
  defer mu.Unlock()
  if closed || called { return nil }
  called = true
  effect()
  outcome = action()
  return outcome
 }
 defer func() {
  mu.Lock()
  closed = true
  mu.Unlock()
 }()
 err := route(guarded)
 mu.Lock()
 defer mu.Unlock()
 closed = true
 if called { return outcome }
 return err
}
`

func callbackFrameProgram(t *testing.T, source string) (*analysis, *ssa.MakeClosure) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module census.callbackfixture\n\ngo 1.26\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(source), 0600))
	loaded, err := workcensus.LoadProgram(workcensus.Config{Dir: dir, Patterns: []string{"./..."}})
	require.NoError(t, err)
	p := &program{root: loaded.Root, fset: loaded.FileSet, ssa: loaded.SSA, funcs: loaded.Functions, cg: loaded.CallGraph}
	a := &analysis{p: p, sums: map[*ssa.Function]*summary{}}
	var callback *ssa.MakeClosure
	for _, fn := range loaded.Functions {
		if fn.Name() == "effect" {
			a.sums[fn] = &summary{reqs: map[string]req{"effect": {space: "prefixNode", origin: "fixture storage effect"}}}
		}
		if fn.Name() != "Guarded" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				mc, ok := ins.(*ssa.MakeClosure)
				if !ok {
					continue
				}
				for _, free := range mc.Fn.(*ssa.Function).FreeVars {
					if free.Name() == "action" {
						callback = mc
					}
				}
			}
		}
	}
	require.NotNil(t, callback)
	return a, callback
}

func TestCallbackFrameLifetime(t *testing.T) {
	seal := "defer func() {\n  mu.Lock()\n  closed = true\n  mu.Unlock()\n }()"
	for _, tc := range []struct {
		name, source string
		want         bool
	}{
		{"sealed", callbackFrameFixture, true},
		{"unsealed", strings.Replace(callbackFrameFixture, seal, "", 1), false},
		{"no_once_guard", strings.Replace(callbackFrameFixture, "closed || called", "closed", 1), false},
		{"no_closed_guard", strings.Replace(callbackFrameFixture, "closed || called", "called", 1), false},
		{"unlocked_action", strings.Replace(callbackFrameFixture, "defer mu.Unlock()", "mu.Unlock()", 1), false},
		{"reopened", strings.Replace(callbackFrameFixture, "if called { return outcome }", "closed = false\n if called { return outcome }", 1), false},
		{"late_seal", strings.Replace(strings.Replace(callbackFrameFixture, seal, "", 1), "err := route(guarded)", "err := route(guarded)\n "+seal, 1), false},
		{"async_work", strings.Replace(callbackFrameFixture, "effect()\n  outcome", "go effect()\n  outcome", 1), false},
		{"effect_outside_guard", strings.Replace(callbackFrameFixture, "guarded := func() error {", "guarded := func() error {\n effect()", 1), false},
		{"stored", `package callbackfixture
var kept func() error
func Guarded(route func(func() error) error, action func() error) error {
 guarded := func() error { return action() }; kept = guarded; return route(guarded)
}`, false},
		{"async", `package callbackfixture
func Guarded(route func(func() error) error, action func() error) error {
 guarded := func() error { go action(); return nil }; return route(guarded)
}`, false},
		{"deferred_store", `package callbackfixture
var kept func() error
func save(callback func() error) { kept = callback }
func Guarded(route func(func() error) error, action func() error) error {
 guarded := func() error { return action() }; defer save(guarded); return nil
}`, false},
		{"copied", `package callbackfixture
var kept [1]func() error
func Guarded(route func(func() error) error, action func() error) error {
 values:=[]func() error{func() error{return action()}};copy(kept[:],values);return nil
}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, callback := callbackFrameProgram(t, tc.source)
			require.Equal(t, tc.want, a.synchronous(callback))
		})
	}
}

func TestCallbackCoverageUsesInvocation(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type BadgerEngine struct{}
 type lifecycleWrite struct{}
 func (lifecycleWrite) finish() {}
 func (*BadgerEngine) beginLifecycleWrite() (lifecycleWrite,error) {return lifecycleWrite{},nil}
 func effect(*BadgerEngine) {}
 func run(units []func()){for _,unit:=range units{unit()}}
 func bad(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err}
  callback:=func(){effect(b)}
  hold.finish()
  callback()
  return nil
 }
 func good(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err};defer hold.finish()
  callback:=func(){effect(b)};callback();return nil
 }
 func containers_good(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err};defer hold.finish()
  units:=[]func(){};units=append(units,func(){effect(b)});run(units);return nil
 }
 func containers_bad(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err}
  units:=[]func(){};units=append(units,func(){effect(b)});hold.finish();run(units);return nil
 }
 func recursive_good(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err};defer hold.finish()
  var callback func();callback=func(){if b==nil{callback();return};effect(b)};callback();return nil
 }
 func recursive_bad(b *BadgerEngine)error{
  hold,err:=b.beginLifecycleWrite();if err!=nil{return err}
  var callback func();callback=func(){if b==nil{callback();return};effect(b)};hold.finish();callback();return nil
 }
 `)
	a := &analysis{p: p, acq: &acquirer{p: p, cache: map[flowKey]*flow{}}, sums: map[*ssa.Function]*summary{
		pkg.Func("effect"): {reqs: map[string]req{"effect": {space: "prefixNode", origin: "fixture storage effect"}}},
	}}
	for _, tc := range []struct {
		name string
		held bool
	}{{"bad", false}, {"good", true}, {"containers_good", true}, {"containers_bad", false}, {"recursive_good", true}, {"recursive_bad", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			for _, closure := range fn.AnonFuncs {
				a.sums[closure] = a.summarize(closure)
			}
			summary := a.summarize(fn)
			require.Equal(t, tc.held, len(summary.reqs) == 0, "constructing a callback under a hold cannot cover invocation after release")
		})
	}
}

func TestContextualOrderAcquisitionProof(t *testing.T) {
	p, pkg := coverageProgram(t, `package storage
 type BadgerEngine struct{}
 type bufferedOrderResources struct{}
 type writeRelease[T any] struct{}
 func (*writeRelease[T]) release() {}
 type orderHold = writeRelease[bufferedOrderResources]
 type writtenEntities struct{}
 func(b *BadgerEngine) orderAfterBufferedCommits(h *orderHold,e writtenEntities)(bool,error){return b.orderAfterBufferedCommitsContext(nil,h,e)}
 func(b *BadgerEngine) orderAfterBufferedCommitsContext(ctx any,h *orderHold,e writtenEntities)(bool,error){return false,nil}
 func effect() {}
 func good(b *BadgerEngine)error{var h orderHold;_,err:=b.orderAfterBufferedCommitsContext(nil,&h,writtenEntities{});if err!=nil{return err};defer h.release();effect();return nil}
 func ignored(b *BadgerEngine){var h orderHold;b.orderAfterBufferedCommitsContext(nil,&h,writtenEntities{});effect()}
 func failed(b *BadgerEngine)error{var h orderHold;_,err:=b.orderAfterBufferedCommitsContext(nil,&h,writtenEntities{});if err==nil{return nil};effect();return err}
 func released(b *BadgerEngine)error{var h orderHold;_,err:=b.orderAfterBufferedCommitsContext(nil,&h,writtenEntities{});if err!=nil{return err};h.release();effect();return nil}
 `)
	q := &acquirer{p: p, cache: map[flowKey]*flow{}}
	for _, tc := range []struct {
		name string
		held bool
	}{{"good", true}, {"ignored", false}, {"failed", false}, {"released", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			found := false
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() == pkg.Func("effect") {
						found = true
						require.Equal(t, tc.held, q.covers(fn, call).has("prefixNode"))
					}
				}
			}
			require.True(t, found)
		})
	}
}
