package testfixture

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

type recordingCloser struct {
	name   string
	closed *[]string
}

type failingCloser struct {
	recordingCloser
	failure error
}

func (resource *failingCloser) Close() error {
	_ = resource.recordingCloser.Close()
	return resource.failure
}

func TestOwnReportsEveryCloseFailureAndDependencyOrder(t *testing.T) {
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
			var boundary Boundary
			var closed []string
			engine := &failingCloser{recordingCloser: recordingCloser{name: "engine", closed: &closed}, failure: failures.engine}
			wal := &failingCloser{recordingCloser: recordingCloser{name: "wal", closed: &closed}, failure: failures.wal}
			if Own(&boundary, wal, engine) != wal {
				t.Fatal("owner changed the resource")
			}
			wal = &failingCloser{recordingCloser: recordingCloser{name: "replacement", closed: &closed}}
			if len(closed) != 0 {
				t.Fatal("fixture closed before its boundary")
			}
			err := boundary.Close()
			if again := boundary.Close(); again != nil {
				t.Fatalf("boundary reported drained failures again: %v", again)
			}
			if !reflect.DeepEqual(closed, []string{"engine", "wal"}) {
				t.Fatalf("close order %v", closed)
			}
			reported := 0
			for _, cause := range []error{failures.engine, failures.wal} {
				if cause != nil && err != nil && strings.Contains(err.Error(), "closing test fixture: "+cause.Error()) {
					reported++
				}
			}
			if reported != failures.wantFailures || (failures.wantFailures == 0) != (err == nil) {
				t.Fatalf("close failures %v, want %d", err, failures.wantFailures)
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
	if err := boundary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := boundary.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(closed, []string{"process"}) {
		t.Fatalf("closed %v", closed)
	}
}

// A testing owner fails the test that owns a fixture whose Close fails; the
// failure is observed from a child process so this test itself stays green.
func TestOwnCloseFailureFailsTestingOwner(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestOwnCloseFailureChild$", "-test.count=1")
	command.Env = append(os.Environ(), "NORNIC_FIXTURE_CLOSE_FAILURE=1")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("close failure passed its testing owner: %s", output)
	}
	for _, want := range []string{"--- FAIL: TestOwnCloseFailureChild/owner", "closing test fixture: planted close failure", "fixture_test.go:"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("missing close-failure evidence %q: %s", want, output)
		}
	}
}

func TestOwnCloseFailureChild(t *testing.T) {
	if os.Getenv("NORNIC_FIXTURE_CLOSE_FAILURE") == "" {
		t.Skip("executed by the parent subprocess contract")
	}
	var closed []string
	t.Run("owner", func(t *testing.T) {
		Own(t, &failingCloser{recordingCloser: recordingCloser{name: "engine", closed: &closed}, failure: errors.New("planted close failure")})
	})
}
