package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

const stableFieldReloadFixture = `package fixture
import "context"
type engine struct { origin context.Context }
type statement struct { executor *engine }
type member struct { statement *statement }
func sink(c context.Context) error { return c.Err() }
var unknown func(*statement)
func stable(m member) error { if m.statement.executor.origin != nil { return sink(m.statement.executor.origin) }; return nil }
func renamed(m member) error { a:=m.statement; if a.executor.origin != nil { b:=a; return sink(b.executor.origin) }; return nil }
func reassigned(m member) error { if m.statement.executor.origin != nil { m.statement.executor=&engine{}; return sink(m.statement.executor.origin) }; return nil }
func different(m,n member) error { if m.statement.executor.origin != nil { return sink(n.statement.executor.origin) }; return nil }
func called(m member) error { if m.statement.executor.origin != nil { unknown(m.statement); return sink(m.statement.executor.origin) }; return nil }
func overwritten(m member) error { if m.statement.executor.origin != nil { *m.statement=statement{executor:&engine{}}; return sink(m.statement.executor.origin) }; return nil }
func closure(m member) error { if m.statement.executor.origin != nil { f:=func(){m.statement.executor=&engine{}}; f(); return sink(m.statement.executor.origin) }; return nil }
func loop(m member,n int) error { if m.statement.executor.origin != nil { for i:=0;i<n;i++ { m.statement.executor=&engine{} }; return sink(m.statement.executor.origin) }; return nil }
func predicate(m,n member) error { if m.statement.executor.origin != nil { if n.statement.executor.origin==nil { return sink(n.statement.executor.origin) } }; return nil }
func receive(m member,ch <-chan struct{}) error { if m.statement.executor.origin != nil { <-ch; return sink(m.statement.executor.origin) }; return nil }
func selectReceive(m member,ch <-chan struct{}) error { if m.statement.executor.origin != nil { select { case <-ch: }; return sink(m.statement.executor.origin) }; return nil }
var escaped map[int]*statement
func mapEscape(m member) error { if m.statement.executor.origin != nil { escaped[0]=m.statement; return sink(m.statement.executor.origin) }; return nil }
var engines,replacements []engine
func builtinCopy(m member) error { if m.statement.executor.origin != nil { copy(engines,replacements); return sink(m.statement.executor.origin) }; return nil }
func Use(){ a:=member{&statement{&engine{}}}; _=stable(a); _=renamed(a); _=reassigned(a); _=different(a,a); _=called(a); _=overwritten(a); _=closure(a); _=loop(a,1); _=predicate(a,a) }
`

func TestStableFieldReloadSourceGuard(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "stable_reload.go"): []byte(stableFieldReloadFixture)}
	inv := mustExtract(t, c)
	got := map[string]bool{}
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
				got[fn.Name()] = len(witness) > 0
				t.Logf("%s nil=%v reasons=%v witness=%v", fn.Name(), got[fn.Name()], reasons, witness)
				var s strings.Builder
				fn.WriteTo(&s)
				t.Log(s.String())
			}
		}
	}
	for _, name := range []string{"stable", "renamed"} {
		if nilValue, seen := got[name]; !seen || nilValue {
			t.Errorf("%s guard not proven", name)
		}
	}
	for _, name := range []string{"reassigned", "different", "called", "overwritten", "closure", "loop", "predicate", "receive", "selectReceive", "mapEscape", "builtinCopy"} {
		if nilValue, seen := got[name]; !seen || !nilValue {
			t.Errorf("%s lost negative", name)
		}
	}
}

// Keep the real public execution call and its SSA rather than substituting a
// fixture with the same field spelling. Other zero-origin constructors are
// deliberately still possible and are not accepted by this proof.
func TestActualNativeOriginGuardedReload(t *testing.T) {
	c := Config{Dir: filepath.Join("..", ".."), Patterns: []string{"./pkg/cypher"}, IncludeProgram: true, Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}}
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, fn := range inv.Program.Functions {
		if (fn.Name() != "execute" && fn.Name() != "bindNativeStatementLifetime") || fn.Pkg == nil || fn.Pkg.Pkg.Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok || call.Common().StaticCallee() == nil {
					continue
				}
				target := call.Common().StaticCallee().Name()
				index := 1
				if fn.Name() == "execute" {
					if target != "bindNativeStatementLifetime" {
						continue
					}
				} else {
					if target != "AfterFuncJoined" {
						continue
					}
					index = 0
				}
				count++
				w, why := inv.Program.MayBeNilWhy(call.Common().Args[index], call)
				if len(w) > 0 {
					t.Errorf("guarded actual origin nil: %v reasons %v", w, why)
				}
				var out strings.Builder
				fn.WriteTo(&out)
				t.Logf("source position %s reasons=%v\n%s", fn.Prog.Fset.Position(call.Pos()), why, out.String())
			}
		}
	}
	if count != 2 {
		t.Fatalf("real bind call count=%d", count)
	}
}
