package testfixture

// Context-owner convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
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

type blockedCloser struct{ release chan struct{} }

func (resource blockedCloser) Close() error {
	<-resource.release
	return nil
}

// A fixture whose close cannot finish fails its owner instead of hanging the
// test binary (bounded fixture close: Claude, storage-format-owners lane, #1693).
func TestOwnBoundsBlockedClose(t *testing.T) {
	previous := closeTimeout
	closeTimeout = 50 * time.Millisecond
	defer func() { closeTimeout = previous }()
	resource := blockedCloser{release: make(chan struct{})}
	defer close(resource.release)
	boundary := &checkedBoundary{}
	Own(boundary, resource)
	boundary.Close()
	if len(boundary.failures) != 1 || !strings.Contains(boundary.failures[0], "did not finish within 50ms") {
		t.Fatalf("blocked close was not reported: %v", boundary.failures)
	}
}
