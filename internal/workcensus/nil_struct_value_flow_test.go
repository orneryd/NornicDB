package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

const structValueFlowFixture = `package fixture
import "context"
type host struct { Context context.Context; other int }
var stored host
func sink(ctx context.Context) error { return ctx.Err() }
func save(c host) { stored = c }
func load(c host) error { x := c.Context; c.Context = context.Background(); return sink(x) }
func otherValue() int { return 1 }
func safe() error { save(host{}); return load(host{Context: context.Background(), other: otherValue()}) }
func nilValue() error { return loadNil(host{}) }
func loadNil(c host) error { return sink(c.Context) }
func copied() error { a := host{Context: context.Background()}; b := a; return sink(b.Context) }
func overwrite() error { a := host{Context: context.Background()}; a.Context = nil; return sink(a.Context) }
func aliasMutation() error { a := host{Context: context.Background()}; b := &a; b.Context = nil; return sink(a.Context) }
func globalValue() error { return sink(stored.Context) }
var unknown func(*host)
func unknownMutation() error { a := host{Context: context.Background()}; unknown(&a); return sink(a.Context) }
func mixed(c host) error { return sink(c.Context) }
func mixedCallers() { _ = mixed(host{Context: context.Background()}); _ = mixed(host{}) }
func Use() { mixedCallers(); _ = safe(); _ = nilValue(); _ = copied(); _ = overwrite(); _ = aliasMutation(); _ = globalValue(); _ = unknownMutation() }
`

func TestNilStructValueFlowKeepsObjectIdentity(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "struct_value.go"): []byte(structValueFlowFixture)}
	inv := mustExtract(t, c)
	got := map[string]string{}
	why := map[string][]string{}
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(ssa.CallInstruction)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
					continue
				}
				witness, reasons := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				name := fn.Name()
				why[name] = reasons
				if witness != nil {
					got[name] = strings.Join(witness, " ")
				}
				t.Logf("%s witness=%v reasons=%v", name, witness, reasons)
				var b strings.Builder
				fn.WriteTo(&b)
				t.Logf("SSA %s\n%s", name, b.String())
			}
		}
	}
	for _, name := range []string{"load", "copied"} {
		if !strings.Contains(strings.Join(why[name], " "), "proof: field projected from exact whole-base store") {
			t.Errorf("%s missing exact-store proof: %v", name, why[name])
		}
		if w, ok := got[name]; ok {
			t.Errorf("%s false nil from another host value: %s", name, w)
		}
	}
	for _, name := range []string{"loadNil", "overwrite", "aliasMutation", "globalValue", "unknownMutation", "mixed"} {
		if _, ok := got[name]; !ok {
			t.Errorf("%s lost nil/unknown control", name)
		}
	}
}
