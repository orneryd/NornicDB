package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"path/filepath"
	"testing"
)

func TestConditionalErrorPreservationSourceControls(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "preserve.go"): []byte(`package fixture
import "errors"
func identity(x error)error{return x}
func wrapped(x error)error{return identity(x)}
func discard(x error)error{return nil}
func branch(x error,b bool)error{if b{return nil};return x}
func normalize(x error,b bool)error{if b{return x};return &fault{}}
type fault struct{};func(*fault)Error()string{return "fault"}
func bag(x error,ys ...error)error{a:=make([]error,0,len(ys)+1);if x!=nil{a=append(a,x)};for _,y:=range ys{if y!=nil{a=append(a,y)}};switch len(a){case 0:return nil;case 1:return a[0]};return errors.Join(a...)}
func resliceAppend(x error)error{a:=[]error{x};b:=a[:0];b=append(b,nil);_ = b;return a[0]}
func shifted(flag bool,x error)error{return identity(x)}
func directMethod(x error)error{return good{}.apply(x)}
func methodValue(x error)error{f:=good{}.apply;return f(x)}
func capturedOverwrite(x error)error{f:=func(){x=nil};f();return x}
func deferredSwallow(x error)(out error){out=x;defer func(){out=nil}();return out}
func recursive(x error)error{return recursive(x)}
func overwritten(x error)error{a:=[]error{x};a[0]=nil;return a[0]}
func truncated(x error)error{a:=[]error{x};a=a[:0];return errors.Join(a...)}
func cleared(x error)error{a:=[]error{x};clear(a);return errors.Join(a...)}
func change(a []error){a[0]=nil}
func aliasWrite(x error)error{a:=[]error{x};change(a);return a[0]}
func nilAppend(x error)error{a:=make([]error,0);a=append(a,nil);return a[0]}
func sameLength(x error)error{a:=[]error{x};a=[]error{nil};return a[0]}
func unrelated(x error)error{a:=[]error{nil};return a[0]}
var external func(error)error
func opaque(x error)error{return external(x)}
func joinRenamed(xs ...error)error{number:=0;for _,element:=range xs{if element!=nil{number++}};if number==0{return nil};return &fault{}}
func joined(x error)error{a:=[]error{x};return joinRenamed(a...)}
func joinErases(xs ...error)error{number:=0;for _,element:=range xs{if element!=nil{number++}};if number==0{return nil};return nil}
func joinedErases(x error)error{a:=[]error{x};return joinErases(a...)}
func lengthOnly(x error)error{a:=[]error{nil};return joinRenamed(a...)}
func changedCounter(xs ...error)error{number:=0;for _,element:=range xs{if element!=nil{number++}};number=0;if number==0{return nil};return &fault{}}
func counterLost(x error)error{a:=[]error{x};return changedCounter(a...)}
type composer interface{apply(error)error};type good struct{};func(good)apply(x error)error{return x};type bad struct{};func(bad)apply(x error)error{return nil}
func dispatch(c composer,x error)error{return c.apply(x)}
func candidates(x error){_=dispatch(good{},x);_=dispatch(bad{},x)}
func mixedTargets(x error)error{return dispatch(good{},x)}
var global error
func mutableGlobal(x error)error{return global}
`)}
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	wants := map[string]bool{"identity": true, "wrapped": true, "discard": false, "branch": false, "normalize": true, "bag": true, "resliceAppend": false, "shifted": true, "directMethod": true, "methodValue": true, "capturedOverwrite": false, "deferredSwallow": false, "recursive": false, "overwritten": false, "truncated": false, "cleared": false, "aliasWrite": false, "nilAppend": false, "sameLength": false, "unrelated": false, "opaque": false, "joined": true, "joinedErases": false, "lengthOnly": false, "counterLost": false, "mixedTargets": false, "mutableGlobal": false}
	found := map[string]bool{}
	for _, fn := range inv.Program.Functions {
		want, ok := wants[fn.Name()]
		if !ok || fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		found[fn.Name()] = true
		// Both query orders prove conditional facts cannot poison the global memo.
		for _, sequence := range [][]bool{{false, true}, {true, false}} {
			flow := newNilFlow(inv.Program)
			for _, order := range sequence {
				p := newErrorProof(flow)
				if order {
					for _, param := range fn.Params {
						if isError(param.Type()) {
							p.params[param] = errorFact{nonnil: true}
							break
						}
					}
				}
				p.setReachable(fn)
				got := strictNormalReturns(p, fn)
				if order && got != want {
					t.Errorf("%s bound nonnil=%t want %t", fn.Name(), got, want)
				}
				if !order && got && fn.Name() != "normalize" {
					t.Errorf("%s unbound unexpectedly proved", fn.Name())
				}
			}
		}
	}
	for name := range wants {
		if !found[name] {
			t.Errorf("missing %s", name)
		}
	}
}
func strictNormalReturns(p *errorProof, fn *ssa.Function) bool {
	found := false
	for _, b := range fn.Blocks {
		if !p.reachable[b] {
			continue
		}
		r, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		found = true
		if len(r.Results) != 1 || !p.fact(r.Results[0], r).nonnil {
			return false
		}
	}
	return found
}

func TestDeferredErrorPreservationActualOwnerControls(t *testing.T) {
	root := filepath.Join("..", "..")
	path, err := filepath.Abs(filepath.Join(root, "pkg/txphase/census_deferred_preservation_fixture.go"))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		setup, cleanup, branch string
		wantNil                bool
	}{
		"actual":               {"", `err=nerr.CombineTerminalOutcomes(err,nil)`, "", false},
		"requiredUnknown":      {"", `err=nerr.CombineTerminalOutcomes(err,unknownCleanup(flag))`, "", false},
		"conditionalPreserver": {"", `if flag {err=nerr.CombineTerminalOutcomes(err,nil)}`, "", false},
		"erases":               {"", `err=nil`, "", true},
		"conditionalErases":    {"", `if flag {err=nil}`, "", true},
		"wrongCell":            {"var other error", `err=nerr.CombineTerminalOutcomes(other,nil)`, "", true},
		"registrationSnapshot": {"snapshot:=err", `err=nerr.CombineTerminalOutcomes(snapshot,nil)`, "", true},
		"escape":               {"", `escapeError(&err)`, "", true},
		"async":                {"", `go func(){err=nil}()`, "", true},
		"overwriteAfterCheck":  {"", `err=nerr.CombineTerminalOutcomes(err,nil)`, `err=nil`, true},
	}
	code := `package txphase
import("context";"errors";nerr "github.com/orneryd/nornicdb/pkg/errors")
func preservationSink(ctx context.Context)error{return ctx.Err()}
func unknownCleanup(flag bool)error{if flag{return errors.New("cleanup")};return nil}
func escapeError(p *error){*p=nil}
`
	for name, c := range cases {
		code += `func proofOwner_` + name + `(ctx context.Context,flag bool)(completion context.Context,err error){completion=ctx;` + c.setup + `;defer func(){cause:=recover();` + c.cleanup + `;if cause!=nil{panic(cause)}}();completion,err=CommitContext(ctx);if err!=nil{` + c.branch + `;return completion,err};return completion,nil}` + "\n"
		code += `func proofUse_` + name + `(flag bool){ctx,err:=proofOwner_` + name + `(context.Background(),flag);if err!=nil{return};_=preservationSink(ctx)}` + "\n"
	}
	code += `func proofOwner_recovered(ctx context.Context)(completion context.Context,err error){defer func(){recover();err=nerr.CombineTerminalOutcomes(err,nil)}();panic("recovered")}
func proofUse_recovered(){ctx,err:=proofOwner_recovered(context.Background());if err!=nil{return};_=preservationSink(ctx)}
func proofOwner_lifo(ctx context.Context)(completion context.Context,err error){completion=ctx;defer func(){err=nil}();defer func(){err=nerr.CombineTerminalOutcomes(err,nil)}();completion,err=CommitContext(ctx);if err!=nil{return completion,err};return completion,nil}
func proofUse_lifo(){ctx,err:=proofOwner_lifo(context.Background());if err!=nil{return};_=preservationSink(ctx)}
`
	cfg := Config{Dir: root, Patterns: []string{"./pkg/txphase", "./pkg/errors", "./pkg/errors/compose"}, IncludeProgram: true, Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}, Overlay: map[string][]byte{path: []byte(code)}}
	inv, err := Extract(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, fn := range inv.Program.Functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				c, ok := ins.(*ssa.Call)
				if !ok || c.Common().StaticCallee() == nil || c.Common().StaticCallee().Name() != "preservationSink" {
					continue
				}
				name := fn.Name()
				const prefix = "proofUse_"
				if len(name) < len(prefix) || name[:len(prefix)] != prefix {
					continue
				}
				name = name[len(prefix):]
				seen[name] = true
				inv.Program.nilFlow = nil
				w, why := inv.Program.MayBeNilWhy(c.Common().Args[0], c)
				want := name == "recovered" || name == "lifo" || cases[name].wantNil
				if (w != nil) != want {
					t.Errorf("%s nil=%t want %t witness=%v reasons=%v", name, w != nil, want, w, why)
				}
			}
		}
	}
	if len(seen) != len(cases)+2 {
		t.Fatalf("matched %d owner controls want %d", len(seen), len(cases)+2)
	}
}

// Poison the exact SSA interval the proof promises to validate. These are
// analysis mutation controls, not claims about a public runtime regression.
func TestDeferredPreservationReadsFinalCellAndRecoverySuccessors(t *testing.T) {
	cfg := genericConfig(t)
	cfg.IncludeProgram = true
	cfg.Overlay = map[string][]byte{filepath.Join(cfg.Dir, "preservation_sequence.go"): []byte(`package fixture
func sequenceIdentity(x error)error{return x}
func sequenceOwner(x error)(out error){out=x;defer func(){out=sequenceIdentity(out)}();if out!=nil{return sequenceIdentity(out)};return out}
var controlRecoveryFlag bool
func sequenceRecovery(x error)(out error){defer func(){recover()}();if x==nil{if controlRecoveryFlag{return nil};return nil};return x}
`)}
	inv, err := Extract(cfg)
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	recovery := false
	for _, fn := range inv.Program.Functions {
		if fn.Name() == "sequenceOwner" {
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					load, ok := ins.(*ssa.UnOp)
					if !ok || !isError(load.Type()) {
						continue
					}
					flow := newNilFlow(inv.Program)
					if !flow.deferredErrorNonNil(load) {
						continue
					}
					checked = true
					original := append([]ssa.Instruction(nil), block.Instrs...)
					index := instructionIndex(load)
					var store *ssa.Store
					var call *ssa.Call
					for _, candidate := range original {
						if s, ok := candidate.(*ssa.Store); ok {
							store = s
						}
						if c, ok := candidate.(*ssa.Call); ok {
							call = c
						}
					}
					if store == nil || call == nil {
						t.Fatal("missing own store/call sequence controls")
					}
					erased := *store
					erased.Val = ssa.NewConst(nil, store.Val.Type())
					for _, poison := range []ssa.Instruction{&erased, call} {
						block.Instrs = append(append(append([]ssa.Instruction(nil), original[:index]...), poison), original[index:]...)
						if newNilFlow(inv.Program).deferredErrorNonNil(load) {
							t.Errorf("intervening %T incorrectly preserved cell", poison)
						}
						block.Instrs = original
					}
				}
			}
		}
		if fn.Name() == "sequenceRecovery" {
			old := fn.Recover
			for _, b := range fn.Blocks {
				if b == fn.Blocks[0] || len(b.Succs) == 0 {
					continue
				}
				if _, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); !ok {
					continue
				}
				fn.Recover = b
				p := newErrorProof(newNilFlow(inv.Program))
				p.params[fn.Params[0]] = errorFact{nonnil: true}
				p.setReachable(fn)
				for _, succ := range b.Succs {
					if !p.reachable[succ] || !p.edges[proofEdge{b, succ}] {
						t.Error("reachable recovery entry did not propagate to successor")
					}
				}
				if strictNormalReturns(p, fn) {
					t.Error("recovery successor nil return incorrectly ignored")
				}
				recovery = true
				break
			}
			fn.Recover = old
		}
	}
	if !checked || !recovery {
		t.Fatalf("sequence=%t recovery=%t", checked, recovery)
	}
}
