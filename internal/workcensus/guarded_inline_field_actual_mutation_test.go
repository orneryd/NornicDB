package workcensus

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestGuardedInlineFieldActualMutations(t *testing.T) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		t.Skip("opt-in actual field-use mutants")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "pkg/search/build_write_queue.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	guard := `if write.ctx == nil {
			return false, false, errors.New("search: queued node admission requires a context")
		}`
	tests := []struct {
		name, from, to, extra string
		proven                bool
	}{
		{"current", "", "", "", true},
		{"no context guard", guard, "", "", false},
		{"inverted context guard", "if write.ctx == nil {", "if write.ctx != nil {", "", false},
		{"no guard return", guard, `if write.ctx == nil { _ = errors.New("not returned") }`, "", false},
		{"inverted node guard", "case write.node != nil:\n\t\tif write.ctx == nil", "case write.node == nil:\n\t\tif write.ctx == nil", "", false},
		{"call before guard", guard, `_, _, _ = s.admitQueuedNode(write.ctx, write.node)
		` + guard, "", false},
		{"clobber after guard", guard, guard + "\nwrite.ctx = nil", "", false},
		{"different context checked", "if write.ctx == nil {", "if s.LifetimeContext() == nil {", "", false},
		{"unguarded replay reader", "", "", `
func(w deferredIndexWrite) fieldProofReplay()error{return w.ctx.Err()}
`, false},
		{"aggregate reflection escape", "", "", `
func(w deferredIndexWrite) fieldProofBox()any{return w}
`, false},
		{"unknown alias writer", guard, guard + "\nfieldProofAlias(&write)", `
var fieldProofAlias func(*deferredIndexWrite)
`, false},
		{"unrelated retained copy", "write.ctx = nil", "write.ctx = nil\n_ = write", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := string(original)
			if tt.from != "" {
				if strings.Count(raw, tt.from) != 1 {
					t.Fatal("actual field mutation anchor changed")
				}
				raw = strings.Replace(raw, tt.from, tt.to, 1)
			}
			raw += tt.extra
			p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/search"}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Overlay: map[string][]byte{path: []byte(raw)}})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := p.LoadedSource(path)
			if err != nil || !bytes.Equal(parsed, []byte(raw)) {
				t.Fatal("mutant was not the actual compiler input")
			}
			found := false
			for _, fn := range p.Functions {
				if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "github.com/orneryd/nornicdb/pkg/search" || fn.Name() != "IndexEdge" {
					continue
				}
				for _, block := range fn.Blocks {
					for _, instruction := range block.Instrs {
						call, ok := instruction.(*ssa.Call)
						if !ok {
							continue
						}
						for _, arg := range call.Common().Args {
							if arg.Type().String() != "github.com/orneryd/nornicdb/pkg/search.deferredIndexWrite" {
								continue
							}
							found = true
							projection := p.ProjectFieldAt(arg, []int{0}, call)
							if projection.Witness == nil {
								t.Fatal("mutant lost exact context-field finding")
							}
							if got := p.GuardedInlineFieldUses(arg, []int{0}, projection); got != tt.proven {
								t.Errorf("actual field proof=%v want%v", got, tt.proven)
							}
						}
					}
				}
			}
			if !found {
				t.Fatal("actual IndexEdge context-field argument missing")
			}
		})
	}
}
