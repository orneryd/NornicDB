package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestNilFlowCheckedErrorRequiresExclusiveSuccessEdge(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "nil_error_soundness.go"): []byte(`package fixture
import("context";"errors")
func soundnessSink(ctx context.Context)error{return ctx.Err()}
func checkedContext(fail bool)(context.Context,error){if fail{return nil,errors.New("closed")};return context.Background(),nil}
func otherError(fail bool)error{if fail{return errors.New("other")};return nil}
func observeError(err error){_=err}
func guardedError(fail bool){ctx,err:=checkedContext(fail);if err!=nil{return};_=soundnessSink(ctx)}
func unrelatedMerge(fail bool){ctx,err:=checkedContext(fail);if err!=nil{observeError(err)};if otherError(false)!=nil{return};_=soundnessSink(ctx)}
var maybeSet error
func setErr(){maybeSet=errors.New("later")}
func unprovenCtx(fail bool)(context.Context,error){if fail{return nil,maybeSet};return context.Background(),nil}
func unprovenCtxUse(fail bool){ctx,err:=unprovenCtx(fail);if err!=nil{return};_=soundnessSink(ctx)}
`)}
	inv := mustExtract(t, c)
	found := map[string]bool{}
	for _, fn := range inv.Program.Functions {
		if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "soundnessSink" {
					continue
				}
				witness, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				found[fn.Name()] = witness != nil
				t.Logf("%s witness=%v why=%s", fn.Name(), witness, strings.Join(why, " | "))
			}
		}
	}
	for name, want := range map[string]bool{"guardedError": false, "unrelatedMerge": true, "unprovenCtxUse": true} {
		got, exists := found[name]
		if !exists {
			t.Errorf("missing actual sink consumer %s", name)
		} else if got != want {
			t.Errorf("%s MayBeNil=%v want %v", name, got, want)
		}
	}
}

func TestNilFlowGlobalInitializationNeedsLoadedSource(t *testing.T) {
	const prelude = `package dep
import("context";"errors";"time")
type literalError struct{}
func(literalError)Error()string{return "literal"}
type pointerError struct{}
func(*pointerError)Error()string{return "pointer"}
func fresh()error{return errors.New("fresh")}
type pointerContext struct{}
func(*pointerContext)Deadline()(time.Time,bool){return time.Time{},false}
func(*pointerContext)Done()<-chan struct{}{return nil}
func(*pointerContext)Err()error{return nil}
func(*pointerContext)Value(any)any{return nil}
var _ context.Context=(*pointerContext)(nil)
`
	for _, tc := range []struct {
		name, definition, consumer string
		overlay                    string
		nilPossible, unload        bool
	}{
		{name: "literal", definition: `var Stable error=literalError{}`},
		{name: "errors.New actual body", definition: `var Stable error=errors.New("initial")`},
		{name: "nil initializer", definition: `var Stable error=nil`, nilPossible: true},
		{name: "overlay initializer mutation", definition: `var Stable error=literalError{}`, overlay: `var Stable error=nil`, nilPossible: true},
		{name: "uninitialized setter", definition: `var Stable error;func Set(){Stable=literalError{}}`, nilPossible: true},
		{name: "later overwrite", definition: `var Stable error=literalError{};func Set(){Stable=nil}`, nilPossible: true},
		{name: "conditional init", definition: `var Stable error;func init(){if time.Now().IsZero(){Stable=literalError{}}}`, nilPossible: true},
		{name: "escaped address", definition: `var Stable error=literalError{};var Alias=&Stable`, nilPossible: true},
		{name: "original importing writer", definition: `var Stable error=literalError{}`, consumer: `func overwrite(){dep.Stable=nil}`, nilPossible: true},
		{name: "original address escape", definition: `var Stable error=literalError{}`, consumer: `func escape(*error){};func expose(){escape(&dep.Stable)}`, nilPossible: true},
		{name: "early initialization read", definition: `type earlyReader interface{Read()error};type earlyThunk struct{};var reader earlyReader=earlyThunk{};var Stable=reader.Read();func(earlyThunk)Read()error{return before()};func before()error{return Later};var Later error=literalError{}`, nilPossible: true},
		{name: "typed nil error interface", definition: `var Stable error=(*pointerError)(nil)`},
		{name: "typed nil context", definition: `var Held context.Context=(*pointerContext)(nil);var Stable error=literalError{}`, consumer: `func useContext(){contextSink(dep.Held)}`, nilPossible: true},
		{name: "unloadable owner source", definition: `var Stable error=literalError{}`, nilPossible: true, unload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, sub := range []string{"dep", "consumer"} {
				if err := os.MkdirAll(filepath.Join(dir, sub), 0700); err != nil {
					t.Fatal(err)
				}
			}
			depFile := filepath.Join(dir, "dep", "dep.go")
			depSource := []byte(prelude + tc.definition + "\n")
			consumer := `package consumer
import("context";"census.globalfixture/dep")
func contextSink(ctx context.Context){_=ctx.Err()}
func errorContext(fail bool)(context.Context,error){if fail{return nil,dep.Stable};return context.Background(),nil}
func Execute(fail bool){ctx,err:=errorContext(fail);if err!=nil{return};contextSink(ctx)}
` + tc.consumer
			for file, raw := range map[string]string{"go.mod": "module census.globalfixture\n\ngo 1.24\n", "dep/dep.go": string(depSource), "consumer/consumer.go": consumer} {
				if err := os.WriteFile(filepath.Join(dir, file), []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			c := genericConfig(t)
			c.Dir = dir
			c.Patterns = []string{"./consumer"}
			c.Roots = []Root{{Package: "census.globalfixture/consumer", Name: "Execute", Required: true}}
			c.IncludeProgram = true
			if tc.overlay != "" {
				depSource = []byte(prelude + tc.overlay + "\n")
				c.Overlay = map[string][]byte{depFile: depSource}
			}
			inv := mustExtract(t, c)
			if tc.unload {
				if err := os.Remove(depFile); err != nil {
					t.Fatal(err)
				}
			}
			found := false
			for _, fn := range inv.Program.Functions {
				if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.globalfixture/consumer" {
					continue
				}
				for _, block := range fn.Blocks {
					for _, ins := range block.Instrs {
						call, ok := ins.(*ssa.Call)
						if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "contextSink" {
							continue
						}
						if tc.name == "typed nil context" && fn.Name() != "useContext" {
							continue
						}
						if tc.name != "typed nil context" && fn.Name() != "Execute" {
							continue
						}
						found = true
						witness, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
						if (witness != nil) != tc.nilPossible {
							t.Errorf("nilPossible=%v want%v witness=%v why=%v", witness != nil, tc.nilPossible, witness, why)
						}
						if !tc.nilPossible {
							hasSourceProof := false
							for _, reason := range why {
								hasSourceProof = hasSourceProof || strings.Contains(reason, "source-verified single package initializer")
							}
							if !hasSourceProof {
								t.Errorf("global discharge has no recorded source proof: %v", why)
							}
							loaded, err := inv.Program.LoadedSource(depFile)
							if err != nil || string(loaded) != string(depSource) {
								t.Errorf("initializer must use captured parsed bytes, err=%v", err)
							}
						}
					}
				}
			}
			if !found {
				t.Fatal("actual context consumer not found")
			}
			if !tc.unload {
				loaded, err := inv.Program.LoadedSource(depFile)
				if err != nil || string(loaded) != string(depSource) {
					t.Errorf("actual initializer parsed bytes differ from expected overlay/source: %v", err)
				}
			}
		})
	}
}

func TestNilFlowStandardContextErrorGlobalHasSourceProof(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "global_standard.go"): []byte(`package fixture
import "context"
func standardContext(fail bool)(context.Context,error){if fail{return nil,context.DeadlineExceeded};return context.Background(),nil}
func standardUse(fail bool){ctx,err:=standardContext(fail);if err!=nil{return};_=ctx.Err()}
`)}
	inv := mustExtract(t, c)
	found := false
	for _, fn := range inv.Program.Functions {
		if fn.Name() != "standardUse" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				call, ok := ins.(*ssa.Call)
				if !ok || call.Common().Method == nil || call.Common().Method.Name() != "Err" {
					continue
				}
				found = true
				witness, why := inv.Program.MayBeNilWhy(call.Common().Value, call)
				if witness != nil {
					t.Fatalf("source-defined DeadlineExceeded refusal must not be a success return: %v", witness)
				}
				matched := false
				for _, reason := range why {
					matched = matched || strings.Contains(reason, "source-verified single package initializer")
				}
				if !matched {
					t.Fatalf("missing source fact: %v", why)
				}
			}
		}
	}
	if !found {
		t.Fatal("context method consumer not found")
	}
}

func TestNilFlowGlobalInitializationOrderIsPerLoad(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "global_order.go"): []byte(`package fixture
 type initialError struct{}
 func(initialError)Error()string{return "initial"}
 type errorReader interface{Read()error}
 type readThunk struct{}
 func(readThunk)Read()error{return readBeforeGlobal()}
 var reader errorReader=readThunk{}
 var Early=reader.Read()
 var SourceErr error=initialError{}
 var After=SourceErr
 func readBeforeGlobal()error{return SourceErr}
 `)}
	inv := mustExtract(t, c)
	seen := map[string]bool{}
	// Query the initialized read first so caching must not erase the per-load
	// ordering requirement on the earlier callback read of the very same global.
	for _, name := range []string{"init", "readBeforeGlobal"} {
		for _, fn := range inv.Program.Functions {
			if fn.Name() != name || fn.Pkg == nil || fn.Pkg.Pkg.Path() != "census.fixture" {
				continue
			}
			for _, block := range fn.Blocks {
				for _, ins := range block.Instrs {
					load, ok := ins.(*ssa.UnOp)
					if !ok {
						continue
					}
					global, ok := load.X.(*ssa.Global)
					if !ok || global.Name() != "SourceErr" {
						continue
					}
					witness, _ := inv.Program.MayBeNilWhy(load, load)
					seen[name] = true
					want := name == "readBeforeGlobal"
					if (witness != nil) != want {
						t.Errorf("%s nil=%v want%v witness=%v", name, witness != nil, want, witness)
					}
				}
			}
		}
	}
	for _, name := range []string{"init", "readBeforeGlobal"} {
		if !seen[name] {
			t.Errorf("missing actual global load in %s", name)
		}
	}
}
