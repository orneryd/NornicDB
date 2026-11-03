package workcensus

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// A successor can dominate a use because it is a join, even when another
// predecessor bypasses the nil test. The test and its selected successor must
// both dominate the use before either nil fact is established.
func TestNilFactsRequireDominatingTestAndSuccessor(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"go.mod": "module census.nildominance\n\ngo 1.26\n",
		"fixture.go": `package nildominance
func sink(*int) {}
func nonNil(v *int) { if v == nil { return }; sink(v) }
func nilValue(v *int) { if v != nil { return }; sink(v) }
func nonNilReversed(v *int) { if nil == v { return }; sink(v) }
func nilReversed(v *int) { if nil != v { return }; sink(v) }
func bypassNonNil(v *int, choice bool) { if choice { if v == nil { return } }; sink(v) }
func bypassNil(v *int, choice bool) { if choice { if v != nil { return } }; sink(v) }
func nestedNonNil(v *int, choice bool) { if choice { if v == nil { return }; sink(v) } }
func joinNil(v *int) { if v != nil { sink(v) }; sink(v) }
func joinNonNil(v *int) { if v == nil { sink(v) }; sink(v) }
func nestedNil(v *int, choice bool) { if choice { if v != nil { return }; sink(v) } }
`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		nilValue, nonNil bool
	}{
		{"nonNil", false, true}, {"nilValue", true, false},
		{"nonNilReversed", false, true}, {"nilReversed", true, false},
		{"bypassNonNil", false, false}, {"bypassNil", false, false},
		{"nestedNonNil", false, true}, {"nestedNil", true, false},
		{"joinNil", false, false}, {"joinNonNil", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			found := false
			for _, fn := range loaded.Functions {
				if fn.Name() != tc.name {
					continue
				}
				for _, block := range fn.Blocks {
					for _, ins := range block.Instrs {
						call, ok := ins.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
							continue
						}
						if (tc.name == "joinNil" || tc.name == "joinNonNil") && len(block.Preds) < 2 {
							continue
						}
						found = true
						value := call.Common().Args[0]
						if got := NilAt(value, block); got != tc.nilValue {
							t.Errorf("NilAt=%v, want %v", got, tc.nilValue)
						}
						if got := NonNilAt(value, block); got != tc.nonNil {
							t.Errorf("NonNilAt=%v, want %v", got, tc.nonNil)
						}
					}
				}
			}
			if !found {
				t.Fatal("fixture sink missing")
			}
		})
	}
}
