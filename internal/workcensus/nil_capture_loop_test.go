package workcensus

// One SSA Store is not one dynamic write. A cell declared outside a loop keeps
// one store that runs again on every iteration, so the iteration that captures
// an accepted context can be followed by an iteration that stores nil into the
// same allocated cell before the goroutine reads it. The capture is silent only
// when the store cannot run again on the cell that was captured: a cell
// allocated inside the loop is a different object at each store.

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

const nilCaptureLoopFixture = `package fixture

import (
	"context"
	"errors"
)

func loopSink(ctx context.Context) error { return ctx.Err() }

func loopCtx(fail bool) (context.Context, error) {
	if fail {
		return nil, errors.New("closed")
	}
	return context.Background(), nil
}

func loopPin(p *context.Context) { *p = nil }

// repeatedWriteCapture is the soundness case: ctx is declared once, outside the
// loop, so the loop's one Store writes the same cell again after an accepted
// iteration captured it. The second iteration stores nil, and the goroutine
// that captured the accepted context may read the cell after that store.
func repeatedWriteCapture(fails []bool) {
	var ctx context.Context
	for _, fail := range fails {
		var err error
		ctx, err = loopCtx(fail)
		if err != nil {
			continue
		}
		go func() { _ = loopSink(ctx) }()
	}
}

func UseRepeatedWriteCapture() { repeatedWriteCapture([]bool{false, true}) }

// repeatedWriteBypassedCapture checks the error on one path only, over the same
// re-stored cell: neither the check nor the cell is a proof.
func repeatedWriteBypassedCapture(fails []bool, skip bool) {
	var ctx context.Context
	for _, fail := range fails {
		var err error
		ctx, err = loopCtx(fail)
		if !skip {
			if err != nil {
				continue
			}
		}
		go func() { _ = loopSink(ctx) }()
	}
}

func UseRepeatedWriteBypassedCapture() { repeatedWriteBypassedCapture([]bool{false, true}, true) }

// repeatedWriteMutatedCapture lets the closure overwrite the captured cell.
func repeatedWriteMutatedCapture(fails []bool) {
	var ctx context.Context
	for _, fail := range fails {
		var err error
		ctx, err = loopCtx(fail)
		if err != nil {
			continue
		}
		go func() {
			ctx = nil
			_ = loopSink(ctx)
		}()
	}
}

func UseRepeatedWriteMutatedCapture() { repeatedWriteMutatedCapture([]bool{false, true}) }

// repeatedWriteEscapedCapture hands the cell's address to a call that may write
// it.
func repeatedWriteEscapedCapture(fails []bool) {
	var ctx context.Context
	for _, fail := range fails {
		var err error
		ctx, err = loopCtx(fail)
		if err != nil {
			continue
		}
		loopPin(&ctx)
		go func() { _ = loopSink(ctx) }()
	}
}

func UseRepeatedWriteEscapedCapture() { repeatedWriteEscapedCapture([]bool{false, true}) }

// perIterationCapture declares the cell inside the loop. Each iteration
// allocates its own cell, so the store the next iteration runs writes a
// different object and the accepted value the closure bound stays the one it
// read. This is the loop positive control: it must stay silent.
func perIterationCapture(fails []bool) {
	for _, fail := range fails {
		ctx, err := loopCtx(fail)
		if err != nil {
			continue
		}
		go func() { _ = loopSink(ctx) }()
	}
}

func UsePerIterationCapture() { perIterationCapture([]bool{false}) }
`

// TestMayBeNilCaptureAcrossRepeatedStore reports a captured context whose cell a
// later iteration of the same loop re-stores, and keeps the per-iteration cell
// silent: the rejection is the cell's reuse, not the loop.
func TestMayBeNilCaptureAcrossRepeatedStore(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "nil_capture_loop.go"): []byte(nilCaptureLoopFixture)}
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
				if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "loopSink" {
					continue
				}
				name := strings.TrimPrefix(OutermostFunction(fn).String(), "census.fixture.")
				witness, reasons := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
				if witness == nil {
					why[name] = reasons
					continue
				}
				got[name] = strings.Join(witness, " ")
			}
		}
	}
	// Every negative names where its nil comes from, so a report is never the
	// result of a blanket loop rejection.
	for name, want := range map[string]string{
		"repeatedWriteCapture":         "result of census.fixture.loopCtx",
		"repeatedWriteBypassedCapture": "result of census.fixture.loopCtx",
		"repeatedWriteMutatedCapture":  "nil context.Context -> stored in captured ctx",
		"repeatedWriteEscapedCapture":  "the address of captured ctx escapes",
	} {
		w, reported := got[name]
		if !reported {
			t.Errorf("%s: nil not reported; silent reasons %v", name, why[name])
			continue
		}
		if !strings.Contains(w, want) {
			t.Errorf("%s: witness does not name its source; want %q in %q", name, want, w)
		}
		delete(got, name)
	}
	// The per-iteration cell is a different object at each store, so the
	// accepted capture still holds the value it bound.
	if w, reported := got["perIterationCapture"]; reported {
		t.Errorf("perIterationCapture: reported as nil: %s", w)
	} else {
		reasons := strings.Join(why["perIterationCapture"], " ")
		for _, want := range []string{"proof: immutable captured cell ctx of census.fixture.perIterationCapture", "proof: return with a non-nil error excluded by the caller's check"} {
			if !strings.Contains(reasons, want) {
				t.Errorf("perIterationCapture: silent for the wrong reason, want %q in %v", want, why["perIterationCapture"])
			}
		}
	}
	if len(got) != 0 {
		t.Errorf("captures reported as nil: %v", got)
	}
}
