package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"path/filepath"
	"strings"
	"testing"
)

const assertionFixture = `package fixture
import "context"
func sink(ctx context.Context)error{return ctx.Err()}
func require(ctx context.Context){if ctx==nil{panic("nil")}}
func lenient(ctx context.Context){if ctx==nil{return}}
func recovered(ctx context.Context){defer func(){_ = recover()}();if ctx==nil{panic("nil")}}
func other(a,b context.Context){if b==nil{panic("nil")}}
var unknown func(context.Context)
func checked(ctx context.Context)bool{return ctx!=nil}
func ignoredCheck(ctx context.Context){_ = checked(ctx);_ = sink(ctx)}
func deferredCheck(ctx context.Context){defer require(ctx);_ = sink(ctx)}
func reassigned(ctx context.Context){require(ctx);ctx=nil;_ = sink(ctx)}
type checker interface{check(context.Context)}
type strict struct{}
func(strict)check(ctx context.Context){if ctx==nil{panic("nil")}}
type permissive struct{}
func(permissive)check(context.Context){}
func mixedTargets(c checker,ctx context.Context){c.check(ctx);_ = sink(ctx)}
func beforeAfter(ctx context.Context){_ = sink(ctx);require(ctx);_ = sink(ctx)}
func normalResult(ctx context.Context)context.Context{require(ctx);return ctx}
func useResult(){_ = sink(normalResult(nil))}
func nilReturn(ctx context.Context){lenient(ctx);_ = sink(ctx)}
func helperRecover(ctx context.Context){recovered(ctx);_ = sink(ctx)}
func unknownCall(ctx context.Context){unknown(ctx);_ = sink(ctx)}
func wrongArgument(ctx context.Context){other(ctx,context.Background());_ = sink(ctx)}
func branch(ctx context.Context,ok bool){if ok{require(ctx)};_ = sink(ctx)}
func callerRecover(ctx context.Context)(out context.Context){defer func(){_ = recover()}();require(ctx);return ctx}
func recoveredUse(){_ = sink(callerRecover(nil))}
type holder struct{ctx context.Context}
func mutate(h *holder){h.ctx=nil}
func aliasMutation(h *holder){require(h.ctx);mutate(h);_ = sink(h.ctx)}
func Use(){ignoredCheck(nil);deferredCheck(nil);reassigned(context.Background());mixedTargets(strict{},nil);mixedTargets(permissive{},nil);beforeAfter(nil);useResult();nilReturn(nil);helperRecover(nil);unknownCall(nil);wrongArgument(nil);branch(nil,false);recoveredUse();aliasMutation(&holder{});aliasMutation(&holder{ctx:context.Background()})}
`

func TestAssertionPostconditionIsUseSiteDependent(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "assertion.go"): []byte(assertionFixture)}
	inv := mustExtract(t, c)
	var calls []ssa.CallInstruction
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if ok && call.Common().StaticCallee() != nil && call.Common().StaticCallee().Name() == "sink" {
					calls = append(calls, call)
				}
			}
		}
	}
	for _, reverse := range []bool{false, true} {
		inv.Program.nilFlow = nil
		got := map[string][]bool{}
		for i := range calls {
			j := i
			if reverse {
				j = len(calls) - 1 - i
			}
			call := calls[j]
			w, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
			got[call.Parent().Name()] = append(got[call.Parent().Name()], w != nil)
			t.Logf("reverse=%v %s at%d nil=%v witness=%v reasons=%v", reverse, call.Parent().Name(), call.Pos(), w != nil, w, why)
		}
		vals := got["beforeAfter"]
		if len(vals) != 2 {
			t.Fatalf("missing before/after calls: %v", vals)
		}
		if reverse {
			vals[0], vals[1] = vals[1], vals[0]
		}
		if !vals[0] || vals[1] {
			t.Errorf("before/after assertion =%v, want [true false]", vals)
		}
		if len(got["useResult"]) != 1 || got["useResult"][0] {
			t.Errorf("normal result remains nil: %v", got["useResult"])
		}
		for _, name := range []string{"nilReturn", "helperRecover", "unknownCall", "wrongArgument", "branch", "recoveredUse", "aliasMutation", "ignoredCheck", "deferredCheck", "reassigned", "mixedTargets"} {
			if len(got[name]) != 1 || !got[name][0] {
				t.Errorf("lost %s negative: %v", name, got[name])
			}
		}
	}
}

func TestActualLifecycleAcquireAssertionPostcondition(t *testing.T) {
	root := filepath.Join("..", "..")
	c := Config{Dir: root, Patterns: []string{"./pkg/lifecycle"}, IncludeProgram: true, Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}}
	abs, err := filepath.Abs(filepath.Join(root, "pkg/lifecycle/census_assertion_fixture.go"))
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{abs: []byte(`package lifecycle
import "context"
func censusSink(ctx context.Context)error{return ctx.Err()}
func CensusAcquireNil(){var w WorkLifetime;ctx,release,_:=w.Acquire(nil);defer release();_ = censusSink(ctx)}
`)}
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, fn := range inv.Program.Functions {
		if fn.Name() == "requireContext" || fn.Name() == "acquire" {
			var b strings.Builder
			fn.WriteTo(&b)
			t.Logf("actual %s SSA\n%s", fn.String(), b.String())
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "censusSink" {
					continue
				}
				found = true
				w, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				if w != nil {
					t.Errorf("actual Acquire normal result nil: %v", w)
				}
				if !strings.Contains(strings.Join(why, " "), "assertion-call normal-return") {
					t.Errorf("actual result lacks assertion proof: %v", why)
				}
				t.Logf("actual result witness=%v proof=%v", w, why)
			}
		}
	}
	if !found {
		t.Fatal("missing actual lifecycle consumer")
	}
}
