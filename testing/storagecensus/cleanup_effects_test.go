package storagecensus

import (
	"go/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Exercise the complete deferred effect body, not merely its treatment of the
// returned error. The old composition-only proof accepted these constructors
// even when the deferred failure arm changed unrelated state.
func TestDeferredCleanupRejectsUnrelatedEffects(t *testing.T) {
	for _, tc := range []struct {
		name, effect string
		want         bool
	}{
		{"pure_helper", "pureCleanupHelper()", true},
		{"direct_body_write", "body()", false},
		{"indirect_body_write", "writeCleanupHelper()", false},
		{"global_write", "armState++", false},
		{"field_write", "cleanupShared.value = 1", false},
		{"map_update", "cleanupMap[1] = 1", false},
		{"goroutine", "go body()", false},
		{"channel_send", "cleanupChannel <- 1", false},
		{"unknown_callback", "callback()", false},
		{"same_named_close", "new(cleanupImposter).Close()", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := cleanupFixtureSource + `
    type cleanupObject struct { value int }
    var cleanupShared cleanupObject
    var cleanupMap map[int]int
    var cleanupChannel chan int
    type cleanupImposter struct{}
    func (*cleanupImposter) Close() error { body(); return nil }
    func pureCleanupHelper() {}
    func writeCleanupHelper() { body() }
    func effectCtor(callback func()) (result *int, openErr error) {
     defer func() {
      if openErr != nil {
       EFFECT
       openErr = compose.Outcomes(openErr, cleanup())
      }
     }()
     err := source()
     if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
     body()
     return nil,nil
    }`
			source = strings.Replace(source, "EFFECT", tc.effect, 1)
			p, pkg := cleanupFixture(t, source)
			d := fixtureBodyAnalysis(t, p, pkg)
			fn := pkg.Func("effectCtor")
			branch := fixtureBranch(t, fn)
			cell, _ := fixtureFailureCell(t, fn)
			closure, capture, ok := deferredCleanupClosure(cell, branch.Block().Succs[0])
			// The nonnil arm may be successor 1 when the compiler negates it.
			if !ok {
				closure, capture, ok = deferredCleanupClosure(cell, branch.Block().Succs[1])
			}
			require.True(t, ok)
			require.True(t, cleanupComposes(closure, capture), "the previous error-only proof accepts every effect variant")
			// Seed the same interprocedural source fact decisions() uses for a
			// helper returning a stored-read error. Its taint must reach the outer
			// guard, which must remain a decision whenever the defer has extra effects.
			d.tainted = map[*ssa.Function]bool{pkg.Func("source"): true}
			require.True(t, d.taint(fn)[branch.Cond], "source error must taint the constructor guard")
			require.NotEmpty(t, d.gatedWrites(fn, branch.Block()), "the guard controls entity-body work")
			require.Equal(t, tc.want, d.propagatesError(branch))
		})
	}
}

func TestDeferredCleanupPureLocalRelease(t *testing.T) {
	// An unknown callback parameter is not a lifecycle capability. A local
	// release stub with a completely proved pure body is safe for this fixture.
	source := strings.Replace(cleanupFixtureSource,
		"func build(releaseConstruction func()) (result *int, newErr error) {",
		"func build() (result *int, newErr error) { releaseConstruction := release", 1)
	p, pkg := cleanupFixture(t, source)
	d := fixtureBodyAnalysis(t, p, pkg)
	require.True(t, d.propagatesError(fixtureBranch(t, pkg.Func("build"))))
}

func TestDeferredCleanupUnknownReleaseIsRejected(t *testing.T) {
	source := cleanupFixtureSource + `
 func unknownRelease(release func()) (result *int,openErr error) {
  defer func(){release();if openErr!=nil{openErr=compose.Outcomes(openErr,cleanup())}}()
  err:=source();if err!=nil{return nil,fmt.Errorf("catalog: %w",err)}
  body();return nil,nil
 }`
	p, pkg := cleanupFixture(t, source)
	d := fixtureBodyAnalysis(t, p, pkg)
	require.False(t, d.propagatesError(fixtureBranch(t, pkg.Func("unknownRelease"))), "a caller-supplied function is not a completion capability")
}

func TestDeferredCleanupRequiresAcquiredRelease(t *testing.T) {
	const source = `package lifecycle
 import "context"
 type WorkLifetime struct{}
 func (*WorkLifetime) Acquire(ctx context.Context) (context.Context,func(),bool) { return ctx,func(){},true }
 func (*WorkLifetime) Other(ctx context.Context) (context.Context,func(),bool) { return ctx,func(){},true }
 type Impostor struct{}
 func (*Impostor) Acquire(ctx context.Context) (context.Context,func(),bool) { return ctx,func(){},true }
 func pureRelease() {}
 func admitted() { var w WorkLifetime; _,release,_:=w.Acquire(context.Background()); defer func(){release()}() }
 func overwritten() { var w WorkLifetime; _,release,_:=w.Acquire(context.Background()); release=pureRelease; defer func(){release()}() }
 func wrongReceiver() { var w Impostor; _,release,_:=w.Acquire(context.Background()); defer func(){release()}() }
 func wrongMethod() { var w WorkLifetime; _,release,_:=w.Other(context.Background()); defer func(){release()}() }
 func parameter(release func()) { defer func(){release()}() }
 `
	_, pkg := ssaFixture(t, source, "pkg/lifecycle", ssa.SanityCheckFunctions, standaloneFixture)
	for _, tc := range []struct {
		name string
		want bool
	}{{"admitted", true}, {"overwritten", false}, {"wrongReceiver", false}, {"wrongMethod", false}, {"parameter", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			var binding *ssa.MakeClosure
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					if mc, ok := ins.(*ssa.MakeClosure); ok {
						binding = mc
					}
				}
			}
			require.NotNil(t, binding)
			closure := binding.Fn.(*ssa.Function)
			proof := cleanupEffectProof{root: closure, binding: binding, visiting: map[*ssa.Function]bool{}}
			var invocation *ssa.Call
			for _, block := range closure.Blocks {
				for _, ins := range block.Instrs {
					if call, ok := ins.(*ssa.Call); ok {
						invocation = call
					}
				}
			}
			require.NotNil(t, invocation)
			require.Equal(t, tc.want, proof.call(invocation.Common(), true))
		})
	}
}

func TestDeferredCleanupTerminalOwnerIdentity(t *testing.T) {
	const source = `package owner
 import "context"
 var state int
 type DB struct{}
 func (*DB) Close() error { state++;return nil }
 func (*DB) LifetimeContext() context.Context { return context.Background() }
 type Server struct{}
 func (*Server) beginShutdown(context.Context) bool {state++;return true}
 func (*Server) WaitStopped(context.Context) error {state++;return nil}
 type Imposter struct{}
 func (*Imposter) Close() error {state++;return nil}
 `
	for _, path := range []string{"pkg/nornicdb", "pkg/server", "pkg/storage"} {
		t.Run(path, func(t *testing.T) {
			_, pkg := ssaFixture(t, source, path, ssa.SanityCheckFunctions, standaloneFixture)
			wanted := map[string]bool{}
			if path == "pkg/nornicdb" {
				wanted["DB.Close"] = true
				wanted["DB.LifetimeContext"] = true
			}
			if path == "pkg/server" {
				wanted["Server.beginShutdown"] = true
				wanted["Server.WaitStopped"] = true
			}
			checked := 0
			for _, typ := range []string{"DB", "Server", "Imposter"} {
				named := pkg.Pkg.Scope().Lookup(typ).Type()
				methods := pkg.Prog.MethodSets.MethodSet(types.NewPointer(named))
				for i := 0; i < methods.Len(); i++ {
					method := pkg.Prog.MethodValue(methods.At(i))
					require.Equal(t, wanted[typ+"."+method.Name()], cleanupTerminal(method), "%s.%s", typ, method.Name())
					checked++
				}
			}
			require.Equal(t, 5, checked)
		})
	}
}
