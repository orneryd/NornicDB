package workcensus

// Author: #1667 implementation lane (Codex). Immutable captured binding facts
// are checked against source mutations, never inferred from a variable name.

import (
	"golang.org/x/tools/go/ssa"
	"os"
	"path/filepath"
	"testing"
)

func TestImmutableCapturedCellValue(t *testing.T) {
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod": "module census.cell\n\ngo 1.26\n",
		"source.go": `package cell
func opaque(*interface{}){}
func consume(interface{}){}
func held(v interface{}){x:=v;f:=func(){consume(x)};f()}
func changed(v interface{}){x:=v;f:=func(){consume(x)};x=7;f()}
func escaped(v interface{}){x:=v;opaque(&x);f:=func(){consume(x)};f()}
func writing(v interface{}){x:=v;f:=func(){x=7;consume(x)};f()}
func before(v interface{}){var x interface{};f:=func(){consume(x)};x=v;f()}
`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		name string
		want bool
	}{{"held", true}, {"changed", false}, {"escaped", false}, {"writing", false}, {"before", false}} {
		t.Run(probe.name, func(t *testing.T) {
			found := false
			for _, fn := range p.Functions {
				if fn.Name() != probe.name {
					continue
				}
				for _, b := range fn.Blocks {
					for _, ins := range b.Instrs {
						mc, ok := ins.(*ssa.MakeClosure)
						if !ok || len(mc.Bindings) != 1 {
							continue
						}
						found = true
						value, known := ImmutableCellValueAt(mc.Bindings[0], mc)
						if known != probe.want {
							t.Fatalf("known=%t want=%t", known, probe.want)
						}
						if known && value != fn.Params[0] {
							t.Fatal("wrong immutable binding")
						}
					}
				}
			}
			if !found {
				t.Fatal("compiled captured cell missing")
			}
		})
	}
}
