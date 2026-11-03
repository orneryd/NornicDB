package workcensus

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestGuardedInlineFieldUses(t *testing.T) {
	tests := []struct {
		name, body, extra string
		proven            bool
	}{
		{"guarded", `if w.node!=nil{if w.ctx==nil{return};sink(w.ctx)};saved=append(saved,w)`, "", true},
		{"no context guard", `if w.node!=nil{sink(w.ctx)}`, "", false},
		{"inverted context guard", `if w.node!=nil{if w.ctx!=nil{return};sink(w.ctx)}`, "", false},
		{"no sibling guard", `if w.ctx==nil{return};sink(w.ctx)`, "", false},
		{"call before guard", `if w.node!=nil{sink(w.ctx);if w.ctx==nil{return}}`, "", false},
		{"clobbered field", `if w.node!=nil{if w.ctx==nil{return};w.ctx=nil;sink(w.ctx)}`, "", false},
		{"different aggregate", `if w.node!=nil{if w.ctx==nil{return};sink(other.ctx)}`, `var other write`, false},
		{"guarded replay", `if w.node!=nil{if w.ctx==nil{return};sink(w.ctx)};saved=append(saved,w)`, `func replay(w write){if w.node!=nil{if w.ctx==nil{return};sink(w.ctx)}}`, true},
		{"unguarded replay", `if w.node!=nil{if w.ctx==nil{return};sink(w.ctx)};saved=append(saved,w)`, `func replay(w write){sink(w.ctx)}`, false},
		{"address escape", `if w.node!=nil{if w.ctx==nil{return};escapeWrite(&w);sink(w.ctx)}`, `var escapeWrite func(*write)`, false},
		{"reflection escape", `if w.node!=nil{if w.ctx==nil{return};sink(w.ctx)};boxed=w`, `var boxed any`, false},
		{"callback escape", `if w.node!=nil{if w.ctx==nil{return};later=func(){sink(w.ctx)}}`, `var later func()`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := genericConfig(t)
			config.IncludeProgram = true
			config.Overlay = map[string][]byte{filepath.Join(config.Dir, "guarded.go"): []byte(`package fixture
import "context"
type write struct{ctx context.Context;node *int}
var saved []write
func sink(ctx context.Context){_=ctx.Err()}
func queue(w write){` + tt.body + `}
func entry(){queue(write{})}
` + tt.extra)}
			p := mustExtract(t, config).Program
			found := false
			for _, fn := range p.Functions {
				if fn.Name() != "entry" {
					continue
				}
				for _, block := range fn.Blocks {
					for _, instruction := range block.Instrs {
						call, ok := instruction.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "queue" {
							continue
						}
						found = true
						arg := call.Common().Args[0]
						projection := p.ProjectFieldAt(arg, []int{0}, call)
						if projection.Witness == nil {
							t.Fatal("negative context input missing")
						}
						if got := p.GuardedInlineFieldUses(arg, []int{0}, projection); got != tt.proven {
							t.Errorf("guarded=%v want%v", got, tt.proven)
						}
					}
				}
			}
			if !found {
				t.Fatal("fixture call missing")
			}
		})
	}
}

func TestGuardedInlineFieldActual(t *testing.T) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		t.Skip("opt-in actual field-use population")
	}
	p, err := LoadProgram(Config{Dir: filepath.Join("..", ".."), Patterns: []string{"./pkg/search"}, BuildFlags: []string{"-tags=noui,nolocalllm"}})
	if err != nil {
		t.Fatal(err)
	}
	findings, unknown := 0, 0
	for _, fn := range p.Functions {
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
					projection := p.ProjectFieldAt(arg, []int{0}, call)
					if projection.Witness == nil {
						continue
					}
					if projection.Unknown {
						unknown++
						if p.GuardedInlineFieldUses(arg, []int{0}, projection) {
							t.Error("unknown retained payload was accepted")
						}
						continue
					}
					findings++
					if !p.GuardedInlineFieldUses(arg, []int{0}, projection) {
						t.Errorf("actual field use unproved %s %s unknown=%v", fn, p.FileSet.Position(call.Pos()), projection.Unknown)
					}
				}
			}
		}
	}
	if findings != 17 || unknown != 2 {
		t.Fatalf("actual field-flow population=%d unknown=%d want17/2 (including one outside-boundary use)", findings, unknown)
	}
}
