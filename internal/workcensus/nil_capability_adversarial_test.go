package workcensus

import (
	"go/types"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestNilFlowCapabilityPhaseIsolation(t *testing.T) {
	p, g, reader := capabilityFixture(t, `
type entry interface{Read()int}
type start struct{}
func(start)Read()int{reader(true);return 1}
var e entry=start{}
var early=e.Read()
func Reader(fail bool)(context.Context,error){return reader(fail)}
`, map[string]string{
		"importer/importer.go": `package importer
import("context";owner "census.capability")
func Importer()(context.Context,error){return owner.Reader(true)}
`,
		"opaque/opaque.go": `package opaque
import "context"
func Opaque(fn func(bool)(context.Context,error))(context.Context,error){return fn(true)}
`,
	})
	var load *ssa.UnOp
	for _, b := range reader.Blocks {
		for _, ins := range b.Instrs {
			if u, ok := ins.(*ssa.UnOp); ok && sameGlobal(u.X, g) {
				load = u
			}
		}
	}
	sites := map[string]*ssa.Call{}
	for _, fn := range p.Functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				if fn.Name() == "Importer" || fn.Name() == "Opaque" {
					sites[fn.Name()] = call
				} else if call.Common().StaticCallee() == reader && fn.Name() == "Read" {
					sites["earlyOwn"] = call
				}
			}
		}
	}
	if load == nil || len(sites) != 3 {
		t.Fatal("phase population incomplete")
	}
	for _, order := range [][]string{{"Opaque", "earlyOwn", "Importer", "earlyOwn", "Opaque"}, {"Importer", "Opaque", "earlyOwn", "Importer"}} {
		f := newNilFlow(p)
		for _, name := range order {
			if got := f.nonNilGlobalErrorAtCall(load, sites[name]); got != (name == "Importer") {
				t.Fatalf("order%v site%s phase%v", order, name, got)
			}
			if f.use(load, load) == nil {
				t.Fatalf("importer answer polluted own early load: order%v after%s", order, name)
			}
			if fact := f.globalInitializer(g); fact == nil || !fact.nonNil {
				t.Fatal("phase answer polluted source fact")
			}
		}
	}
}

func TestNilFlowCapabilityUnseededSSA(t *testing.T) {
	p, g, reader := capabilityFixture(t, `func build()[]string{var a []string;for i:=0;i<2;i++{a=append(a,"x")};return a};var early=build()`)
	if f := newNilFlow(p); !f.initializerPrecedesFunction(f.globalInitializer(g), reader) {
		t.Fatal("seeded loop not proved")
	}
	changed := 0
	for _, fn := range p.Functions {
		if fn.Name() != "build" {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				phi, ok := ins.(*ssa.Phi)
				if !ok {
					continue
				}
				if _, ok := phi.Type().Underlying().(*types.Slice); !ok {
					continue
				}
				for i := range phi.Edges {
					phi.Edges[i] = phi
				}
				changed++
			}
		}
	}
	if changed != 1 {
		t.Fatalf("unseeded SSA mutation population=%d", changed)
	}
	f := newNilFlow(p)
	if f.initializerPrecedesFunction(f.globalInitializer(g), reader) {
		t.Fatal("unseeded cyclic origin proved itself")
	}
}

func TestNilFlowCapabilityCompleteUsesWithoutReferrers(t *testing.T) {
	for _, consumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "discarded", true: "semantic consumer"}[consumed], func(t *testing.T) {
			body := `for i:=range c.shards{c.shards[i]=make(map[string]int)}`
			if consumed {
				body = `consume(c.shards)`
			}
			p, g, reader := capabilityFixture(t, capabilityCell+`
func consume([2]map[string]int){}
func newCell[T any](h func(T)int)*cell[T]{c:=&cell[T]{hash:h};`+body+`;return c}
var early=newCell(hash)
`)
			changed := 0
			for _, fn := range p.Functions {
				if origin := fn.Origin(); origin == nil || origin.Name() != "newCell" {
					continue
				}
				for _, b := range fn.Blocks {
					for _, ins := range b.Instrs {
						load, ok := ins.(*ssa.UnOp)
						if !ok {
							continue
						}
						if _, ok := load.Type().Underlying().(*types.Array); !ok {
							continue
						}
						if refs := load.Referrers(); refs != nil {
							*refs = nil
							changed++
						}
					}
				}
			}
			if changed != 1 {
				t.Fatalf("missing-referrer mutation population=%d", changed)
			}
			f := newNilFlow(p)
			if got := f.initializerPrecedesFunction(f.globalInitializer(g), reader); got == consumed {
				t.Fatalf("proved=%v consumer=%v; complete operand scan must decide", got, consumed)
			}
		})
	}
}
