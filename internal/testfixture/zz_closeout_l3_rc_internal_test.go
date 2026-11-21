// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Root-cause proofs for P-NGEN-22 (NOR-363) and P-NGEN-24 (NOR-365).
package testfixture

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// P-NGEN-22. Owner: StoreBoundary.Check, store_boundary.go:41, which calls
// fmt.Fprintf(output, ...) and discards its error; the next line returns 1
// either way.
// (a) Diagnostic: the exact Fprintf call shape Check makes returns a non-nil
// error on the failing writer (the value discarded at :41) and nil on the control.
// (b) Bypass: a test-side copy of Check that keeps that error reports it; the
// control (working writer) is unchanged.
func TestCloseoutL3_RC_NGEN22_CheckDiscardsFprintfError(t *testing.T) {
	checkCopy := func(boundary StoreBoundary, code int, output io.Writer, name string) (int, error) {
		count, stacks := OpenStores()
		if count != boundary.start {
			_, err := fmt.Fprintf(output, "%s: %d Badger store(s) left open after tests (before=%d after=%d)\n%s", name, count-boundary.start, boundary.start, count, stacks)
			return 1, err
		}
		return code, nil
	}
	leaked := StoreBoundary{start: -1}
	failing := &closeoutL3FailingWriter{err: errors.New("closeout-l3 diagnostic writer failed")}
	_, discarded := fmt.Fprintf(failing, "%s: %d Badger store(s) left open after tests (before=%d after=%d)\n%s", "x", 1, -1, 0, "")
	var good bytes.Buffer
	_, controlErr := fmt.Fprintf(&good, "%s: %d Badger store(s) left open after tests (before=%d after=%d)\n%s", "x", 1, -1, 0, "")
	t.Logf("CLOSEOUT_L3_RC NGEN22 Fprintf at :41 returns failing=%v control=%v", discarded, controlErr)

	pinCode := leaked.Check(0, &closeoutL3FailingWriter{err: errors.New("closeout-l3 diagnostic writer failed")}, "pin")
	copyCode, copyErr := checkCopy(leaked, 0, &closeoutL3FailingWriter{err: errors.New("closeout-l3 diagnostic writer failed")}, "bypass")
	var copyGood bytes.Buffer
	controlCode, controlCopyErr := checkCopy(leaked, 0, &copyGood, "bypass-control")
	t.Logf("CLOSEOUT_L3_RC NGEN22 pinCheck code=%d (error not observable) | bypass code=%d err=%v | bypass control code=%d err=%v wrote=%v",
		pinCode, copyCode, copyErr, controlCode, controlCopyErr, strings.Contains(copyGood.String(), "left open after tests"))

	if controlCopyErr != nil || controlCode != 1 || !strings.Contains(copyGood.String(), "left open after tests") {
		t.Fatalf("control: bypass copy must behave like Check on a working writer")
	}
	if copyErr == nil {
		t.Fatalf("BYPASS: keeping the Fprintf error must surface the writer failure")
	}
	if discarded == nil || controlErr != nil {
		t.Fatalf("MECHANISM: the Fprintf at :41 must fail on the failing writer only (failing=%v control=%v)", discarded, controlErr)
	}
}

// P-NGEN-24. Owner: disposeInvalidOwnerResource, lifetime.go:117-121, which
// runs `_ = resource.Close()` (:119) and returns nothing, so its two callers
// (:76 TrackProcess, :108 requireTestingOwner) panic with a fixed message.
// (c) Minimal reproduction at that step: the function closes the failing
// resource once and has no way to return the error.
// (b) Bypass: a copy that returns the Close error lets the refusal message
// carry it; a resource whose Close succeeds keeps the plain message (control).
func TestCloseoutL3_RC_NGEN24_DisposalErrorDiscarded(t *testing.T) {
	failing := &closeoutL3RCCloser{err: errors.New("closeout-l3 disposal failed")}
	disposeInvalidOwnerResource(failing)
	t.Logf("CLOSEOUT_L3_RC NGEN24 pin disposeInvalidOwnerResource: hasLifetimeResource=%v closeCalls=%d closeErr=%v (returned to caller: nothing)", hasLifetimeResource(failing), failing.closed, failing.err)

	disposeCopy := func(resource Closer) error {
		if hasLifetimeResource(resource) {
			return resource.Close()
		}
		return nil
	}
	refusal := func(resource Closer) string {
		message := "test fixture requires its actual testing.T, testing.B or testing.F owner"
		if err := disposeCopy(resource); err != nil {
			message += "; disposal failed: " + err.Error()
		}
		return message
	}
	bypassFailing := refusal(&closeoutL3RCCloser{err: errors.New("closeout-l3 disposal failed")})
	bypassControl := refusal(&closeoutL3RCCloser{})
	t.Logf("CLOSEOUT_L3_RC NGEN24 bypass failing=%q control=%q", bypassFailing, bypassControl)

	if failing.closed != 1 {
		t.Fatalf("MECHANISM: the pin function must close the resource once, got %d", failing.closed)
	}
	if !strings.Contains(bypassFailing, "closeout-l3 disposal failed") || strings.Contains(bypassControl, "disposal failed") {
		t.Fatalf("BYPASS/control: returned disposal error must appear only when Close fails")
	}
}

type closeoutL3RCCloser struct {
	err    error
	closed int
}

func (c *closeoutL3RCCloser) Close() error {
	c.closed++
	return c.err
}
