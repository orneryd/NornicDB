package testfixture

import (
	"archive/zip"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestOwnWithCloseClosesOnceThroughOwner(t *testing.T) {
	var boundary Boundary
	var closed []string
	engine := &recordingCloser{name: "engine", closed: &closed}
	wal := &failingCloser{recordingCloser: recordingCloser{name: "wal", closed: &closed}, failure: errors.New("WAL close failed")}
	resource, closeNow := OwnWithClose(&boundary, wal, engine)
	if resource != wal {
		t.Fatal("owner changed the resource")
	}
	closeNow()
	if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
		t.Fatalf("early close order %v", closed)
	}
	closeNow()
	err := boundary.Close()
	if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
		t.Fatalf("fixture closed again after its early close: %v", closed)
	}
	if err == nil || strings.Count(err.Error(), "closing test fixture: WAL close failed") != 1 {
		t.Fatalf("early close failure reported %v, want once", err)
	}
}

// The audit of a tracked fixture trusts Own's failed close: Own has reported
// it, and the audit neither closes the resource again nor reports it twice.
func TestTrackedOwnedCloseFailureIsReportedOnce(t *testing.T) {
	var boundary Boundary
	state := &lifetimeState{closeErr: errors.New("engine close failed")}
	track(&boundary, Closer(state), lifetimeClosed, "constructor.go:37")
	Own(&boundary, state)
	err := boundary.Close()
	if state.closes != 1 {
		t.Fatalf("closed %d times, want once", state.closes)
	}
	if err == nil || err.Error() != "closing test fixture: engine close failed" {
		t.Fatalf("close failure reported as %v", err)
	}
	if _, ok := releaseOutcomes.Load(Owner(&boundary)); ok {
		t.Fatal("release outcome outlived the owner's cleanups")
	}
}

type probedLifetime struct {
	probes, closes int
}

func (resource *probedLifetime) Close() error   { resource.closes++; return nil }
func (resource *probedLifetime) IsClosed() bool { resource.probes++; return false }

// While Own's close of a tracked fixture is unfinished, the audit leaves the
// resource alone: probing or closing it again could block on the state the
// unfinished close holds.
func TestAuditLeavesUnfinishedOwnedCloseAlone(t *testing.T) {
	var boundary Boundary
	resource := &probedLifetime{}
	track(&boundary, Closer(resource), lifetimeClosed, "constructor.go:37")
	outcomeOf(&boundary).started = 1
	if err := boundary.Close(); err != nil {
		t.Fatalf("audit reported an unfinished owned close: %v", err)
	}
	if resource.probes != 0 || resource.closes != 0 {
		t.Fatalf("audit probed %d and closed %d times during an unfinished owned close", resource.probes, resource.closes)
	}
}

// A tracked lifetime inherits the failed release that owns it when the owner
// holds a wrapper or a release function, not the tracked resource itself.
func TestAuditInheritsFailedReleaseOfOwningShape(t *testing.T) {
	failure := errors.New("wrapper close failed")
	for name, register := range map[string]func(owner Owner, inner *probedLifetime){
		"wrapper": func(owner Owner, inner *probedLifetime) {
			Own(owner, &failingCloser{recordingCloser: recordingCloser{name: "manager", closed: &[]string{}}, failure: failure})
		},
		"release function": func(owner Owner, inner *probedLifetime) {
			Own(owner, CloseFunc(func() error { return failure }))
		},
		"early release": func(owner Owner, inner *probedLifetime) {
			_, closeNow := OwnWithClose(owner, CloseFunc(func() error { return failure }))
			closeNow()
		},
	} {
		t.Run(name, func(t *testing.T) {
			var boundary Boundary
			inner := &probedLifetime{}
			track(&boundary, Closer(inner), lifetimeClosed, "constructor.go:37")
			register(&boundary, inner)
			err := boundary.Close()
			if inner.probes != 0 || inner.closes != 0 {
				t.Fatalf("audit probed %d and closed %d times after the owning release failed", inner.probes, inner.closes)
			}
			if err == nil || strings.Count(err.Error(), "wrapper close failed") != 1 {
				t.Fatalf("release failure reported as %v, want once", err)
			}
		})
	}
}

// While a wrapper's release is unfinished, the inner lifetime's audit leaves it
// alone: its probe and close could block on what that release holds.
func TestAuditLeavesInnerLifetimeOfUnfinishedRelease(t *testing.T) {
	var boundary Boundary
	inner := &probedLifetime{}
	track(&boundary, Closer(inner), lifetimeClosed, "constructor.go:37")
	outcomeOf(&boundary).started = 1
	if err := boundary.Close(); err != nil {
		t.Fatalf("audit reported an unfinished release: %v", err)
	}
	if inner.probes != 0 || inner.closes != 0 {
		t.Fatalf("audit probed %d and closed %d times during an unfinished release", inner.probes, inner.closes)
	}
}

// After a wrapper's successful release, the inner lifetime is still checked
// against its actual state.
func TestAuditChecksInnerLifetimeAfterSuccessfulRelease(t *testing.T) {
	var boundary Boundary
	inner := &probedLifetime{}
	track(&boundary, Closer(inner), lifetimeClosed, "constructor.go:37")
	Own(&boundary, CloseFunc(func() error { return nil }))
	err := boundary.Close()
	if inner.probes != 1 {
		t.Fatalf("probed %d times after a successful release, want once", inner.probes)
	}
	if err == nil || !strings.Contains(err.Error(), "test fixture still open at cleanup") {
		t.Fatalf("open inner lifetime reported as %v", err)
	}
}

type namedBoundary struct {
	Boundary
	name string
}

func (boundary *namedBoundary) Name() string { return boundary.name }

// A lifetime tracked on a parent test inherits the failed or unfinished release
// of a subtest, and of no sibling or unrelated test.
func TestAuditInheritsReleaseOutcomeOfSubtest(t *testing.T) {
	for name, tc := range map[string]struct {
		child     string
		unfinish  bool
		wantProbe int
	}{
		"failed subtest":     {child: "Parent/sub"},
		"failed nested":      {child: "Parent/sub/deep"},
		"unfinished subtest": {child: "Parent/sub", unfinish: true},
		"unrelated test":     {child: "ParentOther/sub", wantProbe: 1},
	} {
		t.Run(name, func(t *testing.T) {
			parent := &namedBoundary{name: "Parent"}
			inner := &probedLifetime{}
			track(parent, Closer(inner), lifetimeClosed, "constructor.go:37")
			child := &namedBoundary{name: tc.child}
			if tc.unfinish {
				outcomeOf(child).started = 1
			} else {
				Own(child, CloseFunc(func() error { return errors.New("release failed") }))
			}
			_ = child.Close()
			_ = parent.Close()
			if inner.probes != tc.wantProbe {
				t.Fatalf("parent audit probed %d times, want %d", inner.probes, tc.wantProbe)
			}
			if tc.wantProbe == 0 && inner.closes != 0 {
				t.Fatalf("parent audit closed %d times after the subtest's release", inner.closes)
			}
		})
	}
}

// A successful owned close is still checked against the resource's actual
// state, so a Close that returns nil without closing is reported.
func TestAuditChecksStateAfterSuccessfulOwnedClose(t *testing.T) {
	var boundary Boundary
	resource := &probedLifetime{}
	track(&boundary, Closer(resource), lifetimeClosed, "constructor.go:37")
	Own(&boundary, resource)
	err := boundary.Close()
	if resource.probes != 1 || resource.closes != 2 {
		t.Fatalf("probed %d and closed %d times, want 1 and 2", resource.probes, resource.closes)
	}
	if err == nil || !strings.Contains(err.Error(), "test fixture still open at cleanup: constructed at constructor.go:37") {
		t.Fatalf("open resource after a successful close reported as %v", err)
	}
}

// sliceCloser is not comparable; Own still closes and reports it.
type sliceCloser struct {
	closes  *int
	failure []error
}

func (resource sliceCloser) Close() error {
	*resource.closes++
	return errors.Join(resource.failure...)
}

func TestOwnReportsUnrecordableResource(t *testing.T) {
	var boundary Boundary
	closes := 0
	Own(&boundary, sliceCloser{closes: &closes, failure: []error{errors.New("close failed")}})
	if err := boundary.Close(); closes != 1 || err == nil || err.Error() != "closing test fixture: close failed" {
		t.Fatalf("closed %d times, reported %v", closes, err)
	}
}

func TestExtractZipTrimsPrefixIntoTestDirectory(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "fixture.zip")
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	entry, err := writer.Create("layout/data/value.txt")
	if err == nil {
		_, err = entry.Write([]byte("value"))
	}
	if err = errors.Join(err, writer.Close(), file.Close()); err != nil {
		t.Fatal(err)
	}
	root := ExtractZip(t, archivePath, "layout/")
	content, err := os.ReadFile(filepath.Join(root, "data", "value.txt"))
	if err != nil || string(content) != "value" {
		t.Fatalf("extracted %q, %v", content, err)
	}
}

// A release that is not a plain Close goes through the same owner: it closes
// in dependency order, once, and its failure is reported.
func TestOwnClosesReleaseFunctionsInOrder(t *testing.T) {
	var boundary Boundary
	var closed []string
	release := func(name string, failure error) CloseFunc {
		return func() error { closed = append(closed, name); return failure }
	}
	Own(&boundary, release("peer", nil), release("driver", errors.New("driver close failed")))
	err := boundary.Close()
	if !reflect.DeepEqual(closed, []string{"driver", "peer"}) {
		t.Fatalf("close order %v", closed)
	}
	if err == nil || err.Error() != "closing test fixture: driver close failed" {
		t.Fatalf("release failure reported as %v", err)
	}
}

// A redacted fixture reports that its close failed without the error text.
func TestRedactedCloseWithholdsErrorDetails(t *testing.T) {
	var boundary Boundary
	secret := errors.New("password=hunter2")
	Own(&boundary, Redacted("coordinator", CloseFunc(func() error { return secret })))
	err := boundary.Close()
	if err == nil || err.Error() != "closing test fixture: coordinator close failed; details withheld" || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("redacted close reported as %v", err)
	}
	var clean Boundary
	Own(&clean, Redacted("coordinator", CloseFunc(func() error { return nil })))
	if err := clean.Close(); err != nil {
		t.Fatalf("successful redacted close reported %v", err)
	}
}
