package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"os"
	"path/filepath"
	"testing"
)

func TestNilFlowSamePackageInitializerOrder(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, definition, suffix string
		wantNil                          bool
	}{
		{name: "Go orders direct dependency before textual caller", prefix: `var earlyCtx,earlyErr=phaseProducer(true)`, definition: `var stableClosed=errors.New("closed")`},
		{name: "finite numeric CFG loop", prefix: `var early=loop();func loop()int{n:=0;for i:=0;i<4;i++{n+=i};return n}`, definition: `var stableClosed=errors.New("closed")`},
		{name: "private method-expression struct literal", prefix: `type action func(worker)int;type declaration struct{run action};type worker struct{};func(worker)Run()int{return 1};var callback=declaration{run:worker.Run}`, definition: `var stableClosed=errors.New("closed")`},
		{name: "early private callback invocation", prefix: `type action func()int;type declaration struct{run action};func work()int{return 1};var callback=declaration{run:work};var early=callback.run()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "exported callback container", prefix: `type action func()int;type declaration struct{run action};func work()int{return 1};var Callback=declaration{run:work}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "private callback address escape", prefix: `type action func()int;type declaration struct{run action};func work()int{return 1};var callback=declaration{run:work};var escapedCallback=&callback`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "callback alias read", prefix: `type action func()int;type declaration struct{run action};func work()int{return 1};var alias action=work;var callback=declaration{run:alias}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "captured closure", prefix: `type action func()int;type declaration struct{run action};func capture()action{n:=1;return func()int{return n}};var callback=declaration{run:capture()}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "unavailable static body", prefix: `func unavailable()int;var early=unavailable()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "imported goroutine receiver channel", prefix: `type receiver struct{};func(receiver)Run(){phaseProducer(true)};func publish()int{dep.Queue<-receiver{};return 1};var early=publish()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "imported receiver global", prefix: `type receiver struct{};func(receiver)Run(){phaseProducer(true)};func publish()int{dep.Held=receiver{};return 1};var early=publish()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "imported receiver map", prefix: `type receiver struct{};func(receiver)Run(){phaseProducer(true)};func publish()int{dep.Registry["reader"]=receiver{};return 1};var early=publish()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "function field copied outward", prefix: `type action func()int;type declaration struct{run action};func work()int{return 1};var callback=declaration{run:work};func publish()int{dep.Any=callback;return 1};var early=publish()`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "local map initializer", prefix: `var simple=map[string]int{"one":1}`, definition: `var stableClosed=errors.New("closed")`},
		{name: "errors.New", definition: `var stableClosed=errors.New("closed")`},
		{name: "localized allocation chain", definition: `type localized struct{};func(*localized)Error()string{return "closed"};func newLocalized()*localized{return &localized{}};func localize()error{return newLocalized()};var stableClosed=localize()`},
		{name: "early direct call", prefix: `type earlyReader interface{Read()int};type earlyThunk struct{};var starter earlyReader=earlyThunk{};var early=starter.Read();func(earlyThunk)Read()int{phaseProducer(true);return 1}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "early helper call", prefix: `type earlyReader interface{Read()int};type earlyThunk struct{};var starter earlyReader=earlyThunk{};var early=starter.Read();func(earlyThunk)Read()int{return helper()};func helper()int{phaseProducer(true);return 1}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "early callback", prefix: `type earlyReader interface{Read()int};type earlyThunk struct{};var starter earlyReader=earlyThunk{};var early=starter.Read();func(earlyThunk)Read()int{return callback(phaseProducer)};func callback(fn func(bool)(context.Context,error))int{fn(true);return 1}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "early recursive call", prefix: `var early=recursive(false);func recursive(stop bool)int{if stop{return 0};return recursive(true)}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "early goroutine", prefix: `type earlyReader interface{Read()int};type earlyThunk struct{};var starter earlyReader=earlyThunk{};var early=starter.Read();func(earlyThunk)Read()int{return spawn()};func spawn()int{go phaseProducer(true);return 1}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "early deferred call", prefix: `type earlyReader interface{Read()int};type earlyThunk struct{};var starter earlyReader=earlyThunk{};var early=starter.Read();func(earlyThunk)Read()int{return deferred()};func deferred()int{defer phaseProducer(true);return 1}`, definition: `var stableClosed=errors.New("closed")`, wantNil: true},
		{name: "nil initializer", definition: `var stableClosed error=nil`, wantNil: true},
		{name: "later overwrite", definition: `var stableClosed=errors.New("closed")`, suffix: `func overwrite(){stableClosed=nil}`, wantNil: true},
		{name: "address escape", definition: `var stableClosed=errors.New("closed");var escaped=&stableClosed`, wantNil: true},
		{name: "after initializer call", definition: `var stableClosed=errors.New("closed")`, suffix: `var laterCtx,laterErr=phaseProducer(true)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := genericConfig(t)
			c.Dir = t.TempDir()
			if err := os.WriteFile(filepath.Join(c.Dir, "external.s"), []byte("// unavailable native body\n"), 0600); err != nil {
				t.Fatal(err)
			}
			c.Patterns = []string{"./..."}
			c.Roots = []Root{{Package: "census.phasefixture", Name: "phaseConsumer", Required: true}}
			if err := os.WriteFile(filepath.Join(c.Dir, "go.mod"), []byte("module census.phasefixture\n\ngo 1.24\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(c.Dir, "dep"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(c.Dir, "dep", "dep.go"), []byte(`package dep
 type Runner interface{Run()}
 var Held Runner
 var Any any
 var Registry=map[string]Runner{}
 var Queue=make(chan Runner,1)
 func init(){go func(){runner:=<-Queue;runner.Run()}()}
 `), 0600); err != nil {
				t.Fatal(err)
			}
			c.IncludeProgram = true
			c.Overlay = map[string][]byte{filepath.Join(c.Dir, "same_package_order.go"): []byte(`package fixture
import("context";"errors";"census.phasefixture/dep")
var _=dep.Queue
var _=errors.New("keep-import")
` + tc.prefix + "\n" + tc.definition + "\n" + tc.suffix + `
func phaseProducer(fail bool)(context.Context,error){if fail{return nil,stableClosed};return context.Background(),nil}
func phaseSink(ctx context.Context){_=ctx.Err()}
func phaseConsumer(fail bool){ctx,err:=phaseProducer(fail);if err!=nil{return};phaseSink(ctx)}
`)}
			inv := mustExtract(t, c)
			found := false
			for _, fn := range inv.Program.Functions {
				if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.phasefixture" || fn.Name() != "phaseConsumer" {
					continue
				}
				for _, b := range fn.Blocks {
					for _, ins := range b.Instrs {
						call, ok := ins.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "phaseSink" {
							continue
						}
						found = true
						w, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
						if (w != nil) != tc.wantNil {
							t.Errorf("MayBeNil=%v want%v witness%v why%v", w != nil, tc.wantNil, w, why)
						}
					}
				}
			}
			if !found {
				t.Fatal("missing actual sink")
			}
		})
	}
}
