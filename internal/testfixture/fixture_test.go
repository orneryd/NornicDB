package testfixture

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type recordingCloser struct {
	name   string
	closed *[]string
}

type checkedBoundary struct {
	Boundary
	failures []string
}

func (boundary *checkedBoundary) Errorf(format string, arguments ...any) {
	boundary.failures = append(boundary.failures, fmt.Sprintf(format, arguments...))
}

type failingCloser struct {
	recordingCloser
	failure error
}

func (resource *failingCloser) Close() error {
	_ = resource.recordingCloser.Close()
	return resource.failure
}

func TestOwnCheckedPreservesCloseAssertionsAndDependencyOrder(t *testing.T) {
	for _, failures := range []struct {
		name         string
		engine, wal  error
		wantFailures int
	}{
		{"success", nil, nil, 0},
		{"engine_error", errors.New("engine close failed"), nil, 1},
		{"wal_error", nil, errors.New("WAL close failed"), 1},
		{"both_errors", errors.New("engine close failed"), errors.New("WAL close failed"), 2},
	} {
		t.Run(failures.name, func(t *testing.T) {
			var boundary checkedBoundary
			var closed []string
			engine := &failingCloser{recordingCloser: recordingCloser{name: "engine", closed: &closed}, failure: failures.engine}
			wal := &failingCloser{recordingCloser: recordingCloser{name: "wal", closed: &closed}, failure: failures.wal}
			if OwnChecked(&boundary, wal, engine) != wal {
				t.Fatal("checked owner changed the resource")
			}
			wal = &failingCloser{recordingCloser: recordingCloser{name: "replacement", closed: &closed}}
			if len(closed) != 0 || len(boundary.failures) != 0 {
				t.Fatal("fixture closed before its boundary")
			}
			boundary.Close()
			boundary.Close()
			if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
				t.Fatalf("close order %v", closed)
			}
			if len(boundary.failures) != failures.wantFailures {
				t.Fatalf("close failures %v, want %d", boundary.failures, failures.wantFailures)
			}
		})
	}
}

func TestOwnWithCloseAssertionAcceptsExpectedAndReportsUnexpectedErrors(t *testing.T) {
	expected := errors.New("expected terminal close failure")
	for _, test := range []struct {
		name        string
		failure     error
		wantReports int
	}{
		{name: "expected", failure: expected},
		{name: "unexpected", failure: errors.New("unexpected close failure"), wantReports: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var boundary checkedBoundary
			var closed []string
			resource := &failingCloser{
				recordingCloser: recordingCloser{name: "resource", closed: &closed},
				failure:         test.failure,
			}
			OwnWithCloseAssertion(&boundary, resource, func(err error) bool {
				return errors.Is(err, expected)
			})

			boundary.Close()

			if len(boundary.failures) != test.wantReports {
				t.Fatalf("close failures %v, want %d", boundary.failures, test.wantReports)
			}
			if !reflect.DeepEqual(closed, []string{"resource"}) {
				t.Fatalf("closed %v", closed)
			}
		})
	}
}

func (resource *recordingCloser) Close() error {
	*resource.closed = append(*resource.closed, resource.name)
	return nil
}

func TestOwnClosesCapturedResourceAtTestBoundary(t *testing.T) {
	var closed []string
	resource := &recordingCloser{name: "original", closed: &closed}
	t.Run("fixture", func(t *testing.T) {
		if Own(t, resource) != resource {
			t.Fatal("owner changed the resource")
		}
		resource = &recordingCloser{name: "replacement", closed: &closed}
		if len(closed) != 0 {
			t.Fatal("resource closed before its boundary")
		}
	})
	if !reflect.DeepEqual(closed, []string{"original"}) {
		t.Fatalf("closed %v", closed)
	}
}

func TestOwnPreservesDependencyCloseOrder(t *testing.T) {
	var closed []string
	t.Run("fixture", func(t *testing.T) {
		engine := &recordingCloser{name: "engine", closed: &closed}
		wal := &recordingCloser{name: "wal", closed: &closed}
		Own(t, wal, engine)
	})
	if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
		t.Fatalf("closed %v", closed)
	}
}

func TestOwnKeepsFixtureUntilExplicitBoundary(t *testing.T) {
	var boundary Boundary
	var closed []string
	t.Run("caller", func(t *testing.T) {
		Own(&boundary, &recordingCloser{name: "process", closed: &closed})
	})
	if len(closed) != 0 {
		t.Fatal("fixture closed at the caller's test boundary")
	}
	boundary.Close()
	boundary.Close()
	if !reflect.DeepEqual(closed, []string{"process"}) {
		t.Fatalf("closed %v", closed)
	}
}

func TestBoundaryReportsAllCloseFailures(t *testing.T) {
	var boundary Boundary
	var closed []string
	engine := &failingCloser{recordingCloser: recordingCloser{name: "engine", closed: &closed}, failure: errors.New("engine close failed")}
	wal := &failingCloser{recordingCloser: recordingCloser{name: "wal", closed: &closed}, failure: errors.New("wal close failed")}
	Own(&boundary, wal, engine)
	err := boundary.Close()
	if err == nil || !strings.Contains(err.Error(), "engine close failed") || !strings.Contains(err.Error(), "wal close failed") {
		t.Fatalf("expected both close errors, got %v", err)
	}
	if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
		t.Fatalf("close order %v", closed)
	}
	if err := boundary.Close(); err != nil {
		t.Fatalf("repeated close returned stale errors: %v", err)
	}
}
