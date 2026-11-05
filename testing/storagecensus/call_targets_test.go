package storagecensus

import (
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// dispatchProgram builds its fixture the way load does: typed SSA with
// instantiated generics, then VTA over CHA, then the one call-target index.
func dispatchProgram(t *testing.T, source string) (*program, *ssa.Package) {
	t.Helper()
	p, pkg := ssaFixture(t, source, "pkg/storage", ssa.InstantiateGenerics|ssa.SanityCheckFunctions)
	p.cg = vta.CallGraph(ssautil.AllFunctions(p.ssa), cha.CallGraph(p.ssa))
	p.indexCallTargets()
	return p, pkg
}

// lockCall is the one call of a method named Lock in fn.
func lockCall(t *testing.T, fn *ssa.Function) *ssa.Call {
	t.Helper()
	var found *ssa.Call
	for _, block := range fn.Blocks {
		for _, instruction := range block.Instrs {
			call, ok := instruction.(*ssa.Call)
			if !ok {
				continue
			}
			name := ""
			if method := call.Common().Method; method != nil {
				name = method.Name()
			} else if callee := call.Common().StaticCallee(); callee != nil {
				name = callee.Name()
			}
			if name == "Lock" {
				require.Nil(t, found, "the fixture function has one Lock call")
				found = call
			}
		}
	}
	require.NotNil(t, found)
	return found
}

const dispatchFixture = `package storage
 type trackedRWMutex struct{}
 func (m *trackedRWMutex) Lock(){}
 func (m *trackedRWMutex) Unlock(){}
 type otherMutex struct{}
 func (m *otherMutex) Lock(){}
 type locker interface{ Lock() }
 type tracking interface{ Lock(); Unlock() }
 func disagreement(flag bool){
  var mu locker
  if flag { mu = &trackedRWMutex{} } else { mu = &otherMutex{} }
  mu.Lock()
 }
 func agreement(){ var mu tracking = &trackedRWMutex{}; mu.Lock() }
 func direct(mu *trackedRWMutex){ mu.Lock() }
`

// A call site whose possible implementations disagree on the effect grants
// nothing: dispatch unanimity is part of the proof, not a resolution detail.
func TestCallTargetsRefuseDisagreeingDispatch(t *testing.T) {
	p, pkg := dispatchProgram(t, dispatchFixture)
	q := &acquirer{p: p}
	site := lockCall(t, pkg.Func("disagreement"))
	targets := p.callTargets(site)
	require.Len(t, targets, 2, "the fixture must reach both implementations")
	require.Equal(t,
		[]string{"pkg/storage.otherMutex.Lock", "pkg/storage.trackedRWMutex.Lock"},
		[]string{p.funcName(targets[0]), p.funcName(targets[1])},
		"the index gives one stable order whatever order the graph was built in")

	effects := map[string]bool{}
	for _, target := range targets {
		method, ok := target.Object().(*types.Func)
		require.True(t, ok)
		action, exclusive, fallible := mutexMethodEffect(method)
		if action == "lock" {
			effects["lock"] = effects["lock"] || exclusive && !fallible
			continue
		}
		require.Empty(t, action)
		effects["unknown"] = true
	}
	require.Equal(t, map[string]bool{"lock": true, "unknown": true}, effects,
		"the fixture's two implementations must differ in effect")

	receiver, action, exclusive, fallible := q.mutexCallEffect(site)
	require.Nil(t, receiver)
	require.Empty(t, action)
	require.False(t, exclusive)
	require.False(t, fallible)

	// The analysis resolves the same site through the same owner.
	require.Equal(t, targets, (&analysis{p: p}).callees(pkg.Func("disagreement"), site))

	// One recognised implementation is still resolved through the index.
	agreed := lockCall(t, pkg.Func("agreement"))
	require.Len(t, p.callTargets(agreed), 1)
	receiver, action, exclusive, fallible = q.mutexCallEffect(agreed)
	require.NotNil(t, receiver)
	require.Equal(t, "lock", action)
	require.True(t, exclusive)
	require.False(t, fallible)

	// A direct call stays a static resolution and is unaffected by the index.
	direct := lockCall(t, pkg.Func("direct"))
	receiver, action, exclusive, fallible = q.mutexCallEffect(direct)
	require.NotNil(t, receiver)
	require.Equal(t, "lock", action)
	require.True(t, exclusive)
	require.False(t, fallible)
}

const unanimousFixture = `package storage
 type trackedRWMutex struct{}
 func (m *trackedRWMutex) Lock(){}
 func (m *trackedRWMutex) Unlock(){}
 type promoted struct{ trackedRWMutex }
 type tracking interface{ Lock(); Unlock() }
 func two(flag bool){
  var mu tracking
  if flag { mu = &trackedRWMutex{} } else { mu = &promoted{} }
  mu.Lock()
 }
`

// Two dispatch targets that agree on the effect keep the hold: unanimity
// rejects disagreement, not every virtual call.
func TestCallTargetsAcceptUnanimousDispatch(t *testing.T) {
	p, pkg := dispatchProgram(t, unanimousFixture)
	site := lockCall(t, pkg.Func("two"))
	targets := p.callTargets(site)
	require.Len(t, targets, 2, "the fixture must reach both implementations")
	for _, target := range targets {
		method, ok := target.Object().(*types.Func)
		require.True(t, ok)
		action, exclusive, fallible := mutexMethodEffect(method)
		require.Equal(t, "lock", action)
		require.True(t, exclusive)
		require.False(t, fallible)
	}
	receiver, action, exclusive, fallible := (&acquirer{p: p}).mutexCallEffect(site)
	require.NotNil(t, receiver)
	require.Equal(t, "lock", action)
	require.True(t, exclusive)
	require.False(t, fallible)
}

// A program with no call graph has no known target at an invoke site, and an
// unknown target grants nothing.
func TestCallTargetsUnknownGraphGrantsNothing(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type trackedRWMutex struct{}
 func (m *trackedRWMutex) Lock(){}
 func invoke(mu interface{ Lock() }){ mu.Lock() }
 `, "pkg/storage", ssa.SanityCheckFunctions)
	site := lockCall(t, pkg.Func("invoke"))
	require.Empty(t, p.callTargets(site))
	receiver, action, exclusive, fallible := (&acquirer{p: p}).mutexCallEffect(site)
	require.Nil(t, receiver)
	require.Empty(t, action)
	require.False(t, exclusive)
	require.False(t, fallible)
}
