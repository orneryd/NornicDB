package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const repeatedGuardFixture = `package fixture
import "context"
func sink(ctx context.Context)error{return ctx.Err()}
func stable(owner *int){var ctx context.Context;if owner!=nil{ctx=context.Background()};if owner!=nil{_ = sink(ctx)}}
func renamed(selected *int){var ctx context.Context;if selected!=nil{ctx=context.Background()};if selected!=nil{_ = sink(ctx)}}
func phiOwner(ok bool){var owner *int;if ok{x:=1;owner=&x};var ctx context.Context;if owner!=nil{ctx=context.Background()};if owner!=nil{_ = sink(ctx)}}
func capturedRead(owner *int){var ctx context.Context;defer func(){_ = ctx}();if owner!=nil{ctx=context.Background()};if owner!=nil{_ = sink(ctx)}}
func wrong(a,b *int){var ctx context.Context;if a!=nil{ctx=context.Background()};if b!=nil{_ = sink(ctx)}}
type holder struct{owner *int}
func change(h *holder){x:=1;h.owner=&x}
func reload(h *holder){var ctx context.Context;if h.owner!=nil{ctx=context.Background()};change(h);if h.owner!=nil{_ = sink(ctx)}}
func bypass(owner *int, skip bool){var ctx context.Context;if owner!=nil&&!skip{ctx=context.Background()};if owner!=nil{_ = sink(ctx)}}
func unmatched(owner *int){var ctx context.Context;if owner==nil{ctx=context.Background()};if owner!=nil{_ = sink(ctx)}}
func callbackOverwrite(owner *int){var ctx context.Context;if owner!=nil{ctx=context.Background()};func(){ctx=nil}();if owner!=nil{_ = sink(ctx)}}
func loop(owners []*int){ctx:=context.Background();for _,owner:=range owners{if owner!=nil{_ = sink(ctx)};ctx=nil;if owner!=nil{ctx=context.Background()}}}
func none(owner *int){var ctx context.Context;if owner!=nil{ctx=context.Background()};if owner==nil{if owner!=nil{_ = sink(ctx)}}}
func guardedThenPlain(owner *int){var ctx context.Context;if owner!=nil{ctx=context.Background()};if owner!=nil{_ = sink(ctx)};_ = sink(ctx)}
var unknownCtx func(*context.Context)
func escaped(owner *int){var ctx context.Context;if owner!=nil{ctx=context.Background()};unknownCtx(&ctx);if owner!=nil{_ = sink(ctx)}}
func loopPhi(owner *int,n int){ctx:=context.Background();for i:=0;i<n;i++{if owner!=nil{_ = sink(ctx)};ctx=nil;if owner!=nil{ctx=context.Background()};if owner==nil{owner=new(int)}else{owner=nil}}}
func Use(){escaped(new(int));loopPhi(nil,2);none(nil);guardedThenPlain(nil);x:=1;stable(&x);stable(nil);renamed(&x);phiOwner(true);capturedRead(&x);wrong(nil,&x);reload(&holder{});bypass(&x,true);unmatched(&x);callbackOverwrite(&x);loop([]*int{nil,&x})}
`

func TestRepeatedGuardKeepsExactStablePredicate(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "repeated_guard.go"): []byte(repeatedGuardFixture)}
	inv := mustExtract(t, c)
	got := map[string]bool{}
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		if fn.Name() == "stable" || fn.Name() == "capturedRead" || fn.Name() == "loop" {
			var b strings.Builder
			fn.WriteTo(&b)
			t.Logf("%s SSA\n%s", fn.Name(), b.String())
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
					continue
				}
				w, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				got[fn.Name()] = w != nil
				t.Logf("%s witness=%v proof=%v", fn.Name(), w, why)
			}
		}
	}
	for _, name := range []string{"stable", "renamed", "phiOwner", "capturedRead"} {
		if got[name] {
			t.Errorf("stable repeated guard %s falsely nil", name)
		}
	}
	for _, name := range []string{"wrong", "reload", "bypass", "unmatched", "callbackOverwrite", "loop", "loopPhi", "escaped", "none"} {
		if !got[name] {
			t.Errorf("lost %s negative", name)
		}
	}
	var ordered []*ssa.Call
	for _, fn := range inv.Program.Functions {
		if fn.Name() != "guardedThenPlain" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if call, ok := ins.(*ssa.Call); ok && call.Common().StaticCallee() != nil && call.Common().StaticCallee().Name() == "sink" {
					ordered = append(ordered, call)
				}
			}
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Pos() < ordered[j].Pos() })
	if len(ordered) != 2 || ordered[0].Common().Args[0] != ordered[1].Common().Args[0] {
		t.Fatal("want two uses of exact same phi")
	}
	for _, reverse := range []bool{false, true} {
		inv.Program.nilFlow = nil
		for i := range ordered {
			j := i
			if reverse {
				j = 1 - i
			}
			call := ordered[j]
			w, _ := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
			if (w != nil) != (j == 1) {
				t.Errorf("reverse=%v use%d witness=%v", reverse, j, w)
			}
		}
	}

}
func TestActualTLSRepeatedOwnerGuard(t *testing.T) {
	c := Config{Dir: filepath.Join("..", ".."), Patterns: []string{"./pkg/bolt", "./pkg/lifecycle"}, BuildFlags: []string{"-tags=noui,nolocalllm"}, IncludeProgram: true, Selectors: []Selector{{Node: "declaration", Kind: "declaration", Reachability: "all"}}}
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	var calls []*ssa.Call
	for _, fn := range inv.Program.Functions {
		if fn.Name() != "loadTLSConfigWithRotation" {
			continue
		}
		var b strings.Builder
		fn.WriteTo(&b)
		t.Logf("actual TLS SSA\n%s", b.String())
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(*ssa.Call)
				if ok && call.Common().StaticCallee() != nil && call.Common().StaticCallee().Name() == "Acquire" {
					calls = append(calls, call)
				}
			}
		}
	}
	sort.Slice(calls, func(i, j int) bool { return calls[i].Pos() < calls[j].Pos() })
	if len(calls) != 2 {
		t.Fatalf("want2actual Acquire calls got%d", len(calls))
	}
	call := calls[1]
	w, why := inv.Program.MayBeNilWhy(call.Common().Args[len(call.Common().Args)-1], call)
	t.Logf("actual second TLS Acquire witness=%v proof=%v", w, why)
	if !strings.Contains(strings.Join(why, " "), "phi edges contradicting the same stable predicate excluded at use") {
		t.Errorf("actual TLS lacks exact use-local proof: %v", why)
	}
	if w != nil {
		t.Errorf("actual TLS stable owner guard falsely nil: %v", w)
	}
}
