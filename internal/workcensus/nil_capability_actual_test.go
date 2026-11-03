package workcensus

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/tools/go/ssa"
)

type capabilityActualSite struct {
	Caller, Site, Root             string
	CallsitePhase, RefusalExcluded bool
	Witness                        []string
}

func TestNilFlowCapabilityActualWAL(t *testing.T) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		t.Skip("opt-in full current-source qualification")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/storage"}, BuildFlags: []string{"-tags=noui,nolocalllm"}})
	if err != nil {
		t.Fatal(err)
	}
	f := newNilFlow(p)
	var reader *ssa.Function
	var sentinel *ssa.Global
	for _, fn := range p.Functions {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == "github.com/orneryd/nornicdb/pkg/storage" && fn.Name() == "beginNativeMutation" {
			reader = fn
			sentinel, _ = fn.Pkg.Members["ErrInvalidData"].(*ssa.Global)
			break
		}
	}
	if reader == nil || sentinel == nil {
		t.Fatal("native WAL admission source population missing")
	}
	fact := f.globalInitializer(sentinel)
	if fact == nil || !fact.nonNil || !f.initializerPrecedesFunction(fact, reader) {
		t.Fatal("complete native WAL refusal initializer prefix unproved")
	}
	calls := 0
	for _, fn := range p.Functions {
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				call, ok := instruction.(*ssa.Call)
				if !ok || call.Common().StaticCallee() != reader {
					continue
				}
				calls++
				if witness := f.resultOf(call, 0, true); witness != nil {
					t.Errorf("checked native WAL refusal remains possible: %s: %v", fn, witness)
				}
			}
		}
	}
	if calls != 10 {
		t.Fatalf("native WAL caller population=%d want10", calls)
	}
}

func TestNilFlowCapabilityActual(t *testing.T) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		t.Skip("opt-in full current-source qualification")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/cypher", "./pkg/bolt", "./pkg/nornicdb", "./pkg/mcp", "./pkg/server"}})
	if err != nil {
		t.Fatal(err)
	}
	f := newNilFlow(p)
	refusals := map[*ssa.Function]*ssa.UnOp{}
	for _, fn := range p.Functions {
		if fn.Pkg == nil {
			continue
		}
		path := fn.Pkg.Pkg.Path()
		if !((path == "github.com/orneryd/nornicdb/pkg/cypher" && fn.Name() == "BeginUse") ||
			(path == "github.com/orneryd/nornicdb/pkg/bolt" && fn.Name() == "beginServe") ||
			(path == "github.com/orneryd/nornicdb/pkg/nornicdb" && fn.Name() == "beginProviderUse")) {
			continue
		}
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				ret, ok := ins.(*ssa.Return)
				if !ok || len(ret.Results) != 3 {
					continue
				}
				load, ok := ret.Results[2].(*ssa.UnOp)
				if !ok {
					continue
				}
				if g, ok := load.X.(*ssa.Global); ok && (g.Name() == "ErrQueryEmbedderClosed" || g.Name() == "errServerClosed" || g.Name() == "ErrClosed") {
					refusals[fn] = load
				}
			}
		}
	}
	if len(refusals) != 3 {
		t.Fatalf("root population=%d want3", len(refusals))
	}
	for fn, load := range refusals {
		path := fn.Pkg.Pkg.Path()
		if path != "github.com/orneryd/nornicdb/pkg/cypher" && path != "github.com/orneryd/nornicdb/pkg/bolt" {
			continue
		}
		fact := f.globalInitializer(load.X.(*ssa.Global))
		if fact == nil {
			t.Fatal("actual sentinel source fact absent")
		}
		c := newInitializerCapabilities(f, fact, fn)
		if !c.collect() {
			t.Fatal("complete actual owned source closure absent")
		}
		c.solve()
		if !c.validate() {
			t.Error("actual complete typed invariant failed")
		}
		appends, copies, callables, maps, ctors, dead := 0, 0, 0, 0, 0, 0
		bodies := map[*ssa.Function]bool{}
		for _, ins := range c.instructions {
			for _, op := range ins.Operands(nil) {
				if op != nil && *op != nil {
					if _, ok := (*op).(*ssa.Function); ok {
						if call, ok := ins.(*ssa.Call); !ok || call.Common().Value != *op {
							callables++
						}
					}
				}
			}
			switch x := ins.(type) {
			case *ssa.Call:
				if b, ok := x.Common().Value.(*ssa.Builtin); ok {
					if b.Name() == "append" {
						appends++
					}
					if b.Name() == "copy" {
						copies++
					}
				}
				if target := c.calls[x]; target != nil {
					origin := target.Origin()
					if origin == nil {
						origin = target
					}
					if origin.Name() == "newBoundedCache" {
						ctors++
						bodies[target] = true
					}
				}
			case *ssa.MapUpdate:
				if _, ok := x.Value.Type().Underlying().(*types.Signature); ok {
					maps++
				}
			case *ssa.UnOp:
				if _, ok := x.Type().Underlying().(*types.Array); ok {
					semantic := 0
					for _, use := range c.uses[x] {
						if _, debug := use.(*ssa.DebugRef); !debug {
							semantic++
						}
					}
					if semantic == 0 {
						dead++
					}
				}
			}
		}
		t.Logf("%s prefix instructions=%d appends=%d copies=%d callable carriers=%d callable map entries=%d cache callsites=%d cache bodies=%d discarded array loads=%d",
			path, len(c.instructions), appends, copies, callables, maps, ctors, len(bodies), dead)
		if path == "github.com/orneryd/nornicdb/pkg/cypher" {
			if appends != 4 || copies != 0 || callables != 42 || maps != 6 || ctors != 22 || len(bodies) != 19 || dead != 19 {
				t.Error("complete actual typed frontier population changed")
			}
		} else if len(c.instructions) != 80 || appends != 0 || copies != 0 {
			t.Error("Bolt complete prefix population changed")
		}
	}
	want := map[string]int{
		"runServing": 1, "applyInlineEmbeddingMutations": 1, "callDbIndexVectorEmbed": 1, "embedVectorQueryText": 1, "runSearchRequest": 1,
		"handleDiscover": 1, "BeginProviderUse": 1, "FindSimilarInDatabase": 1, "HybridSearchWithOptions": 1, "SearchDatabase": 1,
		"beginProviderUse$bound": 2, "chunkQueryWithEmbedder": 1, "embedQueryChunksWithEmbedder": 1,
	}
	five := map[string]bool{"runServing": true, "applyInlineEmbeddingMutations": true, "callDbIndexVectorEmbed": true, "embedVectorQueryText": true, "runSearchRequest": true}
	counts := map[string]int{}
	var records []capabilityActualSite
	for _, fn := range p.Functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok {
					continue
				}
				for _, target := range p.CallTargets(call) {
					load := refusals[target]
					if load == nil {
						continue
					}
					counts[fn.Name()]++
					phase := f.nonNilGlobalErrorAtCall(load, call)
					site := capabilityActualSite{Caller: fn.String(), Site: p.FileSet.Position(call.Pos()).String(), Root: target.String(), CallsitePhase: phase, RefusalExcluded: phase || f.use(load, load) == nil, Witness: f.resultOf(call, 0, true)}
					records = append(records, site)
					t.Logf("%+v", site)
					if !site.RefusalExcluded || site.Witness != nil {
						t.Errorf("unproved occurrence: %+v", site)
					}
					if five[fn.Name()] {
						fact := f.globalInitializer(load.X.(*ssa.Global))
						if fact == nil || !fact.nonNil || !f.initializerPrecedesFunction(fact, load.Parent()) {
							t.Errorf("unproved complete prefix: %s", fn)
						}
					}
				}
			}
		}
	}
	if !reflect.DeepEqual(counts, want) {
		t.Errorf("population=%v want%v", counts, want)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Caller+records[i].Site < records[j].Caller+records[j].Site })
	pins := map[string]string{}
	p.sources.mu.Lock()
	for path, raw := range p.sources.files {
		pins[path] = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	p.sources.mu.Unlock()
	// Pins attest parser input, never a post-hoc substitute for it. A separate
	// currentness check rejects concurrent disk drift during qualification.
	for path, pin := range pins {
		raw, err := os.ReadFile(path)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin {
			t.Errorf("parsed source no longer current: %s (%v)", path, err)
		}
	}
	for _, path := range []string{"pkg/cypher/query_embedder.go", "pkg/bolt/server_lifecycle.go", "pkg/nornicdb/db_embedder_owner.go"} {
		if pins[filepath.Join(root, path)] == "" {
			t.Errorf("missing parser pin: %s", path)
		}
	}
	t.Logf("complete admission population=%d; parser-input pins=%d", len(records), len(pins))
	if dir := os.Getenv("WORKCENSUS_CAPABILITY_EVIDENCE"); dir != "" {
		for name, value := range map[string]any{"typed-actual-sites.json": records, "typed-parser-pins.json": pins} {
			raw, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
