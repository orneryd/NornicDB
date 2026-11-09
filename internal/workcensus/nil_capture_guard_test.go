package workcensus

// A closure that captures the result of a checked call reads it in another
// function, after the capture. The capture is where the value leaves its
// function, so the checked-success and immutable-cell facts that hold there
// bound the value: the check is not lost because the read happens later, in a
// goroutine, or through a cell. Every plant below hands a context to sink; only
// the plants where a nil can actually arrive may be reported.

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

const nilCaptureFixture = `package fixture

import (
	"context"
	"errors"
)

func sink(ctx context.Context) error { return ctx.Err() }

func maybeCtx(fail bool) (context.Context, error) {
	if fail {
		return nil, errors.New("closed")
	}
	return context.Background(), nil
}

// acquire rejects a request by returning it unchanged, with its error: the
// rejection result may be nil only where the error is not.
func acquire(ctx context.Context, ok bool) (context.Context, error) {
	if !ok {
		return ctx, errors.New("rejected")
	}
	return context.Background(), nil
}

func pin(p *context.Context) { *p = nil }

// guardedCapture is the reviewed shape: a nil context is returned only with an
// error, the caller returns on that error, and the accepted context is captured
// by a goroutine. No nil can reach sink, so it must stay silent.
func guardedCapture(fail bool) {
	ctx, err := maybeCtx(fail)
	if err != nil {
		return
	}
	go func() { _ = sink(ctx) }()
}

func UseGuardedCapture() { guardedCapture(false) }

// guardedRejectionCapture is the same shape over the rejection contract: the
// rejected result may be nil, but the caller uses only the accepted one.
func guardedRejectionCapture(ctx context.Context, ok bool) {
	got, err := acquire(ctx, ok)
	if err != nil {
		return
	}
	go func() { _ = sink(got) }()
}

func UseGuardedRejectionCapture() { guardedRejectionCapture(nil, true) }

// reassignedCapture is the same accepted shape when the result replaces the
// request it came from: the cell now holds two stored values, and the capture,
// not either store, is where the accepted one leaves the function.
func reassignedCapture(ctx context.Context, fail bool) {
	ctx, err := maybeCtx(fail)
	if err != nil {
		return
	}
	go func() { _ = sink(ctx) }()
}

func UseReassignedCapture() { reassignedCapture(context.Background(), false) }

// ignoredCapture captures the result without its error: the nil error return
// still reaches sink.
func ignoredCapture(fail bool) {
	ctx, _ := maybeCtx(fail)
	go func() { _ = sink(ctx) }()
}

func UseIgnoredCapture() { ignoredCapture(true) }

// bypassedCapture checks the error on one path only, so the capture is not
// dominated by the check and the nil error return still reaches sink.
func bypassedCapture(fail, skip bool) {
	ctx, err := maybeCtx(fail)
	if !skip {
		if err != nil {
			return
		}
	}
	go func() { _ = sink(ctx) }()
}

func UseBypassedCapture() { bypassedCapture(true, true) }

// mutatedCapture lets the closure overwrite the captured cell, so the value read
// there is not the accepted one.
func mutatedCapture(fail bool) {
	ctx, err := maybeCtx(fail)
	if err != nil {
		return
	}
	go func() {
		ctx = nil
		_ = sink(ctx)
	}()
}

func UseMutatedCapture() { mutatedCapture(false) }

// escapedCapture hands the cell's address to a call that may write it, so the
// captured value is not the accepted one.
func escapedCapture(fail bool) {
	ctx, err := maybeCtx(fail)
	if err != nil {
		return
	}
	pin(&ctx)
	go func() { _ = sink(ctx) }()
}

func UseEscapedCapture() { escapedCapture(false) }

// rejectionUse reads the rejected result on the error path.
func rejectionUse(ctx context.Context, ok bool) {
	got, err := acquire(ctx, ok)
	if err != nil {
		_ = sink(got)
		return
	}
}

func UseRejectionUse() { rejectionUse(nil, false) }

// dynamicCapture checks the error of a dynamic call whose targets include one
// that returns a nil context with a nil error: the check cannot remove it.
type nullableAcquire interface{ acquire(bool) (context.Context, error) }

type nilAcquirer struct{}

func (nilAcquirer) acquire(bool) (context.Context, error) { return nil, nil }

func dynamicCapture(a nullableAcquire, fail bool) {
	ctx, err := a.acquire(fail)
	if err != nil {
		return
	}
	go func() { _ = sink(ctx) }()
}

func UseDynamicCapture() { dynamicCapture(nilAcquirer{}, true) }
`

// TestMayBeNilGuardedCapture plants the guarded capture shape beside the
// controls that must keep reporting: the check is a path fact, not a function
// name, a nil-result convention or an exemption.
func TestMayBeNilGuardedCapture(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "nil_capture.go"): []byte(nilCaptureFixture)}
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
	// The accepted context of a checked call is the value the capture binds:
	// the nil error return is excluded by the caller's check at the capture,
	// and the cell that carries it is immutable there.
	for _, silent := range []string{"guardedCapture", "guardedRejectionCapture"} {
		if w, reported := got[silent]; reported {
			t.Errorf("%s: guarded capture reported as nil: %s", silent, w)
			continue
		}
		reasons := strings.Join(why[silent], " ")
		for _, want := range []string{"proof: immutable captured cell", "proof: return with a non-nil error excluded by the caller's check"} {
			if !strings.Contains(reasons, want) {
				t.Errorf("%s: silent for the wrong reason, want %q in %v", silent, want, why[silent])
			}
		}
	}
	// The accepted context that replaced its own request keeps its checked
	// result: two stores reach the cell, and the one that reaches the capture
	// is judged with the nil tests and checked errors that hold there.
	if w, reported := got["reassignedCapture"]; reported {
		t.Errorf("reassignedCapture: guarded capture reported as nil: %s", w)
	} else {
		reasons := strings.Join(why["reassignedCapture"], " ")
		for _, want := range []string{"proof: reaching store before the capture of ctx of census.fixture.reassignedCapture", "proof: return with a non-nil error excluded by the caller's check"} {
			if !strings.Contains(reasons, want) {
				t.Errorf("reassignedCapture: silent for the wrong reason, want %q in %v", want, why["reassignedCapture"])
			}
		}
	}
	// Each control names where its nil comes from, so a passing positive is
	// never the result of a blanket exemption.
	for name, want := range map[string]string{
		"ignoredCapture":  "result of census.fixture.maybeCtx",
		"bypassedCapture": "result of census.fixture.maybeCtx",
		// The closure rewrites the captured cell, so the nil read there is
		// its own store, not the accepted result.
		"mutatedCapture": "nil context.Context -> stored in captured ctx",
		// The cell's address escapes, so no value is proven to stay in it.
		"escapedCapture": "the address of captured ctx escapes",
		"rejectionUse":   "parameter ctx of census.fixture.acquire",
		"dynamicCapture": "result of (census.fixture.nilAcquirer).acquire",
	} {
		w, reported := got[name]
		if !reported {
			t.Errorf("%s: nil not reported; got %v", name, got)
			continue
		}
		if !strings.Contains(w, want) {
			t.Errorf("%s: witness does not name its source; want %q in %q", name, want, w)
		}
		delete(got, name)
	}
	if len(got) != 0 {
		t.Errorf("captures reported as nil: %v", got)
	}
}
