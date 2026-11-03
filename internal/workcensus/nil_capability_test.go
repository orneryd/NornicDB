package workcensus

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func capabilityFixture(tb testing.TB, prefix string, extra ...map[string]string) (*Program, *ssa.Global, *ssa.Function) {
	tb.Helper()
	dir := tb.TempDir()
	files := map[string]string{
		"go.mod":    "module census.capability\n\ngo 1.24\n",
		"dep/dep.s": "// unavailable static native body\n",
		"owner.s":   "// unavailable owned body\n",
		"dep/dep.go": `package dep
type Runner interface{Run()}
var Held Runner
var Any any
var Registry=map[string]Runner{}
var Queue=make(chan Runner,1)
var Data=[]string{"foreign"}
var DataMap=map[string][]string{"a":Data}
var DataQueue=make(chan []string,1)
type Stage struct{Hash func(int)int}
var Current=&Stage{}
func init(){go func(){runner:=<-Queue;runner.Run()}()}
func Primitive(int)int
func Accept(any)int{return 1}
func Register(func())int{return 1}
func Generic[T any]()int{return 1}
func Foreign()any{return 1}
func ForeignCallable()func(){return func(){}}
func ForeignHash()func(int)int{return func(int)int{return 1}}
func ForeignMap()map[string]func(){return nil}
func ForeignSlice()[]string{return Data}
`,
		"owner.go": `package owner
import("context";"errors";"fmt";"reflect";"unsafe";"census.capability/dep")
var _=errors.New
var _=fmt.Errorf
var _=reflect.TypeOf
var _ unsafe.Pointer
var _=dep.Queue
type receiver struct{}
func(receiver)Run(){reader(true)}
func work(receiver){}
func construct(){var x dep.Runner=receiver{};x.Run()}
` + prefix + `
var marker=dep.Primitive(1)
var stable=errors.New("closed")
func reader(fail bool)(context.Context,error){if fail{return nil,stable};return context.Background(),nil}
func sink(ctx context.Context){_=ctx.Err()}
func consumer(fail bool){ctx,err:=reader(fail);if err!=nil{return};sink(ctx)}
`,
	}
	for _, additions := range extra {
		for name, raw := range additions {
			files[name] = raw
		}
	}
	for name, raw := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			tb.Fatal(err)
		}
	}
	p, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		tb.Fatal(err)
	}
	for _, pkg := range p.SSA.AllPackages() {
		if pkg.Pkg.Path() == "census.capability" {
			return p, pkg.Members["stable"].(*ssa.Global), pkg.Func("reader")
		}
	}
	tb.Fatal("fixture owner absent")
	return nil, nil, nil
}

func BenchmarkInitializerCapabilityPrefix(b *testing.B) {
	for _, tc := range []struct{ name, prefix string }{
		{"primitive", `var early=dep.Primitive(1)`},
		{"slice-loop", `type record struct{s string};func build()[]record{var out []record;for i:=0;i<16;i++{out=append(out,record{s:"data"})};return out};var early=build()`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p, g, reader := capabilityFixture(b, tc.prefix)
			f := newNilFlow(p)
			fact := f.globalInitializer(g)
			if fact == nil || !fact.nonNil {
				b.Fatal("source fact absent")
			}
			b.Run("cold", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					f.initializationOrder = nil
					f.initializerPrecedesFunction(fact, reader)
				}
			})
			b.Run("cached", func(b *testing.B) {
				f.initializerPrecedesFunction(fact, reader)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f.initializerPrecedesFunction(fact, reader)
				}
			})
		})
	}
}

func TestNilFlowCapabilityPrimitive(t *testing.T) {
	p, g, reader := capabilityFixture(t, `var early=dep.Primitive(1)`)
	f := newNilFlow(p)
	if !f.initializerPrecedesFunction(f.globalInitializer(g), reader) {
		t.Fatal("complete capability-free prefix unproved")
	}
}

const capabilityCell = `
	type cell[T any]struct{hash func(T)int;shards [2]map[string]int}
	func hash(int)int{return 1}
	`

func TestNilFlowCapabilityFrontier(t *testing.T) {
	tests := []struct {
		name, source string
		positive     bool
	}{
		{"primitive opaque", `var early=dep.Primitive(1)`, true},
		{"prior foreign error", `var prior=errors.New("prior");var early=fmt.Errorf("wrapped: %w",prior)`, true},
		{"prior foreign error helper", `var prior=errors.New("prior");func wrap()error{return fmt.Errorf("wrapped: %w",prior)};var early=wrap()`, true},
		{"prior foreign interface", `var prior=dep.Foreign();var early=dep.Accept(prior)`, true},
		{"owner interface receiver", `var prior dep.Runner=receiver{};var early=dep.Accept(prior)`, false},
		{"owner error receiver", `type ownerError struct{};func(ownerError)Error()string{reader(true);return "early"};var prior error=ownerError{};var early=fmt.Errorf("wrapped: %w",prior)`, false},
		{"prior error overwrite", `var prior=errors.New("prior");func wrap()error{prior=errors.New("changed");return fmt.Errorf("wrapped: %w",prior)};var early=wrap()`, false},
		{"prior error address escape", `var prior=errors.New("prior");var escaped=&prior;var early=dep.Accept(escaped)`, false},
		{"local append loop", `type record struct{s string};func build()[]record{var out []record;for i:=0;i<3;i++{out=append(out,record{"data"})};return out};var early=build()`, true},
		{"owned literal plus owned return", `type record struct{s string};func build()[]record{return []record{{"b"}}};var early=append([]record{{"a"}},build()...)`, true},
		{"exact private source", `type record struct{s string};var source=[]record{{"a"}};var early=append([]record{{"b"}},source...)`, true},
		{"borrowed nested read only", `type record struct{s []string};func build()[]record{var out []record;out=append(out,record{dep.Data});return out};var early=build()`, true},
		{"metadata projection consumer", `type record struct{s string};var source=[]record{{"a"}};func read()int{n:=0;for _,r:=range source{n+=dep.Primitive(len(r.s))};return n};var early=read()`, true},
		{"sealed callable map", `var early=map[string]func(){"a":construct,"b":construct}`, true},
		{"foreign typed nil chain", `var early=reflect.TypeOf((*context.Context)(nil)).Elem()`, true},
		{"sealed generic constructors", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};for i:=range c.shards{c.shards[i]=make(map[string]int)};return c};var early=newCell(hash);var second=newCell(hash)`, true},
		{"callable slice hidden construction", `var early=append([]func(){},construct)`, false},
		{"function field slice", `type record struct{f func()};var early=append([]record{},record{construct})`, false},
		{"nested callable slice", `type record struct{f []func()};var early=append([]record{},record{[]func(){construct}})`, false},
		{"owned receiver interface slice", `var early=append([]any{},receiver{})`, false},
		{"nested receiver interface", `type record struct{x any};var early=append([]record{},record{receiver{}})`, false},
		{"distinct constructing bridge", `type bridge struct{};func(bridge)Run(){var x dep.Runner=receiver{};x.Run()};var early=dep.Accept(bridge{})`, false},
		{"value method revokes data", `type record struct{s string};func(record)Run(){var x dep.Runner=receiver{};x.Run()};var early=append([]record{},record{})`, false},
		{"pointer method revokes data", `type record struct{s string};func(*record)Run(){var x dep.Runner=receiver{};x.Run()};var early=append([]record{},record{})`, false},
		{"owned typed nil reflection", `var early=reflect.TypeOf((*receiver)(nil))`, false},
		{"owned TypeFor", `var early=reflect.TypeFor[receiver]()`, false},
		{"nested generic owner", `type box[T any]struct{v T};var early=dep.Generic[[]box[receiver]]()`, false},
		{"reflection ValueOf", `var early=reflect.ValueOf(receiver{})`, false},
		{"unsafe primitive laundering", `var value receiver;var early=dep.Primitive(int(uintptr(unsafe.Pointer(&value))))`, false},
		{"foreign destination", `var early=append(dep.Data,"x")`, false},
		{"unknown destination", `func unavailable()[]string;var early=append(unavailable(),"x")`, false},
		{"borrowed nested destination", `type record struct{s []string};var source=[]record{{dep.Data}};var early=append(source[0].s,"x")`, false},
		{"private overwritten header", `var source=[]string{"a"};func build()[]string{source=[]string{"b"};return append([]string{},source...)};var early=build()`, false},
		{"private header address escape", `var source=[]string{"a"};var escaped=&source;var early=append([]string{},source...)`, false},
		{"private nondominating definition", `var source []string;func build(b bool)[]string{if b{source=[]string{"a"}};return append([]string{},source...)};var early=build(false)`, false},
		{"metadata helper invokes reader", `type record struct{s string};func build()[]record{var r=[]record{{"a"}};construct();return r};var early=build()`, false},
		{"metadata helper publishes receiver", `type record struct{s string};func build()[]record{dep.Held=receiver{};return []record{{"a"}}};var early=build()`, false},
		{"data copy remains rejected", `func build()int{a:=[]string{"a"};return copy(a,[]string{"b"})};var early=build()`, false},
		{"recursive data shape", `type record struct{next []record};var early=append([]record{},record{})`, false},
		{"receiver global escape", `func publish()int{dep.Held=receiver{};return dep.Primitive(1)};var early=publish()`, false},
		{"receiver map escape", `func publish()int{dep.Registry["r"]=receiver{};return dep.Primitive(1)};var early=publish()`, false},
		{"receiver channel escape", `func publish()int{dep.Queue<-receiver{};return dep.Primitive(1)};var early=publish()`, false},
		{"receiver interface escape", `func publish()int{dep.Any=receiver{};return dep.Primitive(1)};var early=publish()`, false},
		{"callable argument escape", `var early=dep.Register(construct)`, false},
		{"sealed map lookup", `var callbacks=map[string]func(){"a":construct};var early=callbacks["a"]`, false},
		{"sealed map invoke", `var callbacks=map[string]func(){"a":construct};func invoke()int{callbacks["a"]();return 1};var early=invoke()`, false},
		{"sealed map len reload", `var callbacks=map[string]func(){"a":construct};var early=len(callbacks)`, false},
		{"sealed map range", `var callbacks=map[string]func(){"a":construct};func count()int{n:=0;for range callbacks{n++};return n};var early=count()`, false},
		{"sealed map pass", `var callbacks=map[string]func(){"a":construct};var early=dep.Accept(callbacks)`, false},
		{"sealed map address", `var callbacks=map[string]func(){"a":construct};var early=&callbacks`, false},
		{"exported callable map", `var Callbacks=map[string]func(){"a":construct}`, false},
		{"callable map foreign alias", `func build()int{m:=map[string]func(){"a":construct};dep.Any=m;return 1};var early=build()`, false},
		{"constructor invokes callback", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{var zero T;h(zero);return &cell[T]{hash:h}};var early=newCell(hash)`, false},
		{"constructor defers callback", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{var zero T;defer h(zero);return &cell[T]{hash:h}};var early=newCell(hash)`, false},
		{"constructor async callback", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{var zero T;go h(zero);return &cell[T]{hash:h}};var early=newCell(hash)`, false},
		{"constructor boxes callback", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{dep.Any=h;return &cell[T]{hash:h}};var early=newCell(hash)`, false},
		{"constructor publishes callback", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{dep.Accept(h);return &cell[T]{hash:h}};var early=newCell(hash)`, false},
		{"constructor extra escaped result", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{return &cell[T]{hash:h}};var early=newCell(hash);var escaped=dep.Accept(early)`, false},
		{"constructor second result escapes", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{return &cell[T]{hash:h}};var early=newCell(hash);var second=dep.Accept(newCell(hash))`, false},
		{"constructor staging alias escapes", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};dep.Accept(&c.shards);return c};var early=newCell(hash)`, false},
		{"consumed array load external", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};a:=c.shards;dep.Accept(a);return c};var early=newCell(hash)`, false},
		{"consumed array load owned", capabilityCell + `func consume([2]map[string]int){};func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};consume(c.shards);return c};var early=newCell(hash)`, false},
		{"hash field read", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};dep.Accept(c.hash);return c};var early=newCell(hash)`, false},
		{"foreign interface owned wrapper", `type wrapper struct{reflect.Type};func(wrapper)Elem()reflect.Type{construct();return nil};var early=dep.Accept(wrapper{})`, false},
		{"foreign receiver mixed phi", `type wrapper struct{reflect.Type};func(wrapper)Elem()reflect.Type{construct();return nil};func choose(b bool)reflect.Type{if b{return reflect.TypeOf(0)};return wrapper{}};var early=choose(false).Elem()`, false},
		{"foreign global contaminated", `type wrapper struct{reflect.Type};func(wrapper)Elem()reflect.Type{construct();return nil};func publish()int{dep.Any=wrapper{};return dep.Primitive(1)};var early=publish()`, false},
		{"unknown dynamic function", `func unavailable()func();func invoke()int{unavailable()();return 1};var early=invoke()`, false},
		{"owned source gap", `func unavailable()int;var early=unavailable()`, false},
		{"owned static recursion", `func recurse(n int)int{if n==0{return 1};return recurse(n-1)};var early=recurse(3)`, false},
		{"unmodelled channel allocation", `var early=make(chan int)`, false},
		{"local backing exported before append", `func build()[]string{a:=make([]string,1,4);dep.Accept(a);return append(a,"b")};var early=build()`, false},
		{"escaped subslice even forced growth", `func build()[]string{a:=[]string{"a"};dep.Accept(a[:]);return append(a,"b","c","d")};var early=build()`, false},
		{"escaped element address", `func build()[]string{a:=make([]string,1,4);dep.Accept(&a[0]);return append(a,"b")};var early=build()`, false},
		{"escaped phi alternative", `func build(b bool)[]string{a:=make([]string,1,4);if b{dep.Accept(a)};var d []string;if b{d=a};return append(d,"b")};var early=build(false)`, false},
		{"foreign phi alternative", `func build(b bool)[]string{a:=[]string{"a"};if b{a=dep.Data};return append(a,"b")};var early=build(false)`, false},
		{"backing write after outward argument", `func build()int{a:=[]string{"a"};dep.Accept(a);a[0]="b";return 1};var early=build()`, false},
		{"borrowed nested element write", `type record struct{s []string};func build()int{a:=record{dep.Data};a.s[0]="b";return 1};var early=build()`, false},
		{"owned pointer slice", `var early=append([]*receiver{},&receiver{})`, false},
		{"data helper returns address outward", `type record struct{s string};func build()*[]record{a:=[]record{{"a"}};return &a};var early=dep.Accept(build())`, false},
		{"local protected map length", `func build()int{a:=map[string]func(){"a":construct};return len(a)};var early=build()`, false},
		{"foreign map phi", `func build(b bool)map[string]func(){a:=map[string]func(){"a":construct};if b{return nil};return a};var early=dep.Accept(build(false))`, false},
		{"consumed array load returned", capabilityCell + `func newCell[T any](h func(T)int)[2]map[string]int{c:=&cell[T]{hash:h};return c.shards};var early=newCell(hash)`, false},
		{"consumed array load private global", capabilityCell + `var aggregate [2]map[string]int;func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};aggregate=c.shards;return c};var early=newCell(hash)`, false},
		{"consumed array load boxed", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};var a any=c.shards;dep.Accept(a);return c};var early=newCell(hash)`, false},
		{"consumed array load indexed branch", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};a:=c.shards;if a[0]!=nil{return nil};return c};var early=newCell(hash)`, false},
		{"unknown callable map entry", `var early=map[string]func(){"a":dep.ForeignCallable()}`, false},
		{"unknown callable constructor parameter", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{return &cell[T]{hash:h}};var early=newCell(dep.ForeignHash())`, false},
		{"foreign callable map phi", `func build(b bool)map[string]func(){m:=map[string]func(){"a":construct};if b{m=dep.ForeignMap()};return m};var early=build(false)`, false},
		{"local callable map range", `func count()int{m:=map[string]func(){"a":construct};n:=0;for range m{n++};return n};var early=count()`, false},
		{"discarded hash load", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};_ = c.hash;return c};var early=newCell(hash)`, false},
		{"foreign map slice destination", `var early=append(dep.DataMap["a"],"b")`, false},
		{"foreign channel slice destination", `var early=append(<-dep.DataQueue,"b")`, false},
		{"opaque foreign slice destination", `var early=append(dep.ForeignSlice(),"b")`, false},
		{"constructor writes foreign field", capabilityCell + `func newCell(h func(int)int)*cell[int]{dep.Current.Hash=h;return &cell[int]{hash:h}};var early=newCell(hash)`, false},
		{"second callsite poisons callback union", capabilityCell + `func newCell[T any](h func(T)int)*cell[T]{return &cell[T]{hash:h}};var early=newCell(hash);var second=newCell(dep.ForeignHash())`, false},
		{"aggregate consumed by method", `type shards [2]map[string]int;func(shards)Check(){};type cell[T any]struct{hash func(T)int;shards shards};func hash(int)int{return 1};func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};c.shards.Check();return c};var early=newCell(hash)`, false},
		{"owned recover builtin", `func call()int{recover();return 1};var early=call()`, false},
		{"owner state address argument", `var state=1;var early=dep.Accept(&state)`, false},
		{"private map address helper", `var callbacks map[string]func();func initialize(out *map[string]func())int{*out=map[string]func(){"a":construct};return 1};var early=initialize(&callbacks)`, false},
		{"global array destination", `var state [2]string;var early=append(state[:],"a")`, false},
		{"anonymous interface method argument type", `var early=reflect.TypeOf((*interface{M(receiver)})(nil))`, false},
		{"anonymous interface method result type", `var early=reflect.TypeOf((*interface{M()receiver})(nil))`, false},
		{"anonymous interface generic type", `var early=dep.Generic[interface{M(receiver)}]()`, false},
		{"anonymous owned interface declaration", `var early=reflect.TypeOf((*interface{Read()})(nil))`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, g, reader := capabilityFixture(t, tc.source)
			f := newNilFlow(p)
			fact := f.globalInitializer(g)
			if fact == nil || !fact.nonNil {
				t.Fatal("fixture lost single source-proven sentinel")
			}
			var marker *ssa.Store
			for _, b := range fact.owner.Func("init").Blocks {
				for _, ins := range b.Instrs {
					if store, ok := ins.(*ssa.Store); ok {
						if global, ok := store.Addr.(*ssa.Global); ok && global.Name() == "marker" {
							marker = store
						}
					}
				}
			}
			if marker == nil || !marker.Block().Dominates(fact.store.Block()) || (marker.Block() == fact.store.Block() && instructionIndex(marker) >= instructionIndex(fact.store)) {
				t.Fatal("mutant not in real sentinel prefix")
			}
			if got := f.initializerPrecedesFunction(fact, reader); got != tc.positive {
				t.Errorf("prefix proved=%v want%v", got, tc.positive)
			}
			found := false
			for _, fn := range p.Functions {
				if fn.Name() != "consumer" {
					continue
				}
				for _, b := range fn.Blocks {
					for _, ins := range b.Instrs {
						call, ok := ins.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
							continue
						}
						found = true
						if w := newNilFlow(p).use(call.Common().Args[0], call); (w == nil) != tc.positive {
							t.Errorf("canonical discharge=%v want%v witness=%v", w == nil, tc.positive, w)
						}
					}
				}
			}
			if !found {
				t.Fatal("canonical context use absent")
			}
		})
	}
}
