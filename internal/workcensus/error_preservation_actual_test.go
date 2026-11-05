package workcensus

import (
	"golang.org/x/tools/go/ssa"
	"path/filepath"
	"strings"
	"testing"
)

// This is the preserved real CommitContext/compose/normalizer reproduction,
// not a synthetic replacement for the source implementations being proved.
func TestActualCommitErrorCorrelationWithComposition(t *testing.T) {
	root := filepath.Join("..", "..")
	path, err := filepath.Abs(filepath.Join(root, "pkg/txphase/census_composition_fixture.go"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: root, Patterns: []string{"./pkg/txphase", "./pkg/errors", "./pkg/errors/compose"}, IncludeProgram: true, Selectors: []Selector{{Kind: "declaration", Node: "declaration", Reachability: "all"}}, Overlay: map[string][]byte{path: []byte(`package txphase
import("context";nerr "github.com/orneryd/nornicdb/pkg/errors")
func censusConsume(ctx context.Context)error{return ctx.Err()}
func censusPlain(ctx context.Context)(completion context.Context,err error){completion,err=CommitContext(ctx);if err!=nil{return completion,err};return completion,nil}
func censusComposed(ctx context.Context)(completion context.Context,err error){completion=ctx;defer func(){cause:=recover();err=nerr.CombineTerminalOutcomes(err,nil);if cause!=nil{panic(cause)}}();completion,err=CommitContext(ctx);if err!=nil{return completion,err};return completion,nil}
func CensusPlain(){ctx,err:=censusPlain(context.Background());if err!=nil{return};_=censusConsume(ctx)}
func CensusComposed(){ctx,err:=censusComposed(context.Background());if err!=nil{return};_=censusConsume(ctx)}
`)}}
	inv, err := Extract(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, fn := range inv.Program.Functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "censusConsume" {
					continue
				}
				seen[fn.Name()] = true
				inv.Program.nilFlow = nil
				w, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				if w != nil {
					t.Errorf("%s checked commit context witness=%v", fn.Name(), w)
				}
				if fn.Name() == "CensusComposed" {
					text := strings.Join(why, "\n")
					for _, proof := range []string{"source-verified conditional error preservation through exact deferred cell", "call-bound source error result errors.Join", "call-bound source error result github.com/orneryd/nornicdb/pkg/errors/compose.Outcomes", "call-bound source error result github.com/orneryd/nornicdb/pkg/errors.NormalizeTerminalOutcome"} {
						if !strings.Contains(text, proof) {
							t.Errorf("missing actual source proof %q: %s", proof, text)
						}
					}
				}
			}
		}
	}
	if !seen["CensusPlain"] || !seen["CensusComposed"] || len(seen) != 2 {
		t.Fatalf("actual source caller population %v", seen)
	}
}
