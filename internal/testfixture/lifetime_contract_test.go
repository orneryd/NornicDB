package testfixture

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

var lifetimeClosed = errors.New("fixture closed")
var lifetimeReadFailed = errors.New("fixture read failed")

type lifetimeState struct {
	closed   bool
	closes   int
	closeErr error
}

func (state *lifetimeState) Close() error   { state.closed = true; state.closes++; return state.closeErr }
func (state *lifetimeState) IsClosed() bool { return state.closed }

type countLifetimeState struct {
	lifetimeState
	readErr error
}

func (state *countLifetimeState) NodeCount() (int64, error) {
	if state.closed {
		return 0, lifetimeClosed
	}
	return 0, state.readErr
}

// Hide IsClosed to exercise the storage engine's closed-error contract.
type countOnlyLifetime struct{ state *countLifetimeState }

func (resource *countOnlyLifetime) Close() error              { return resource.state.Close() }
func (resource *countOnlyLifetime) NodeCount() (int64, error) { return resource.state.NodeCount() }

type unknownLifetime struct{ state *lifetimeState }

func (resource *unknownLifetime) Close() error { return resource.state.Close() }

func TestLifetimeAuditChecksActualStateAndDisposesLeaks(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		closed, unsupported, readFailure, closeFailure bool
	}{
		{"raw_open", false, false, false, false}, {"raw_closed", true, false, false, false},
		{"engine_read_error", false, false, true, false}, {"disposal_error", false, false, false, true},
		{"unsupported", false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var owner checkedBoundary
			state := &lifetimeState{closed: tc.closed}
			if tc.closeFailure {
				state.closeErr = errors.New("disposal failed")
			}
			var resource Closer = state
			if tc.unsupported {
				resource = &unknownLifetime{state}
			}
			if tc.readFailure {
				resource = &countOnlyLifetime{&countLifetimeState{lifetimeState: *state, readErr: lifetimeReadFailed}}
			}
			entry := lifetimeEntry{resource: resource, closedError: lifetimeClosed, site: "constructor.go:37"}
			entry.audit(&owner)
			if tc.closed {
				if len(owner.failures) != 0 || state.closes != 0 {
					t.Fatal("closed resource was rejected or closed again")
				}
				return
			}
			if len(owner.failures) != 1 {
				t.Fatalf("expected one lifetime error, got %v", owner.failures)
			}
			if !strings.Contains(owner.failures[0], "constructor.go:37") {
				t.Fatal("construction site missing")
			}
			if tc.unsupported && !strings.Contains(owner.failures[0], "no closed-state probe") {
				t.Fatal("unsupported probe was silently accepted")
			}
			if tc.closeFailure && !strings.Contains(owner.failures[0], "disposal failed") {
				t.Fatal("close error was lost")
			}
			if closed, known := entry.isClosed(); known && !closed {
				t.Fatal("leaked resource was not disposed")
			}
		})
	}
}

func TestLifetimeCountProbeRequiresClosedSentinel(t *testing.T) {
	state := &countLifetimeState{}
	entry := lifetimeEntry{resource: &countOnlyLifetime{state}, closedError: lifetimeClosed, site: "count"}
	if closed, known := entry.isClosed(); closed || !known {
		t.Fatal("open count-based resource considered closed")
	}
	state.readErr = lifetimeReadFailed
	if closed, known := entry.isClosed(); closed || !known {
		t.Fatal("unrelated read error accepted as closure")
	}
	state.closed = true
	if closed, known := entry.isClosed(); !closed || !known {
		t.Fatal("closed sentinel not observed")
	}
	entry.closedError = nil
	if _, known := entry.isClosed(); known {
		t.Fatal("missing closed sentinel was accepted")
	}
}

func TestLifetimeResultPreservesConstructorResults(t *testing.T) {
	wantErr := errors.New("constructor error")
	var nilResource *lifetimeState
	got, err := TrackResult[*lifetimeState](t, nil)(nilResource, wantErr)
	if got != nil || err != wantErr {
		t.Fatal("nil constructor result changed")
	}
	state := &lifetimeState{}
	got, err = TrackResult[*lifetimeState](t, nil)(state, wantErr)
	if got != state || err != wantErr {
		t.Fatal("non-nil error result changed")
	}
	if got.closes != 0 {
		t.Fatal("constructor resource closed before ownership")
	}
	Own(t, state)
	t.Run("result_closed", func(t *testing.T) {
		resource := Track(t, &lifetimeState{}, nil)
		if err := resource.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLifetimeRejectsSyntheticAndNilOwners(t *testing.T) {
	for _, owner := range []Owner{nil, &Boundary{}, (*testing.T)(nil), (*testing.B)(nil), (*testing.F)(nil)} {
		t.Run(fmt.Sprintf("%T", owner), func(t *testing.T) {
			state := &lifetimeState{}
			var caught any
			func() { defer func() { caught = recover() }(); Track(owner, state, nil) }()
			if caught == nil || !strings.Contains(fmt.Sprint(caught), "actual testing") {
				t.Fatal("invalid owner silently accepted")
			}
			if !state.closed || state.closes != 1 {
				t.Fatal("invalid-owner resource not disposed")
			}
		})
	}
}

func TestLifetimeProcessBoundaryAndNilResources(t *testing.T) {
	var owner Boundary
	var absent *lifetimeState
	if Track(t, absent, nil) != nil {
		t.Fatal("nil result changed")
	}
	if TrackProcess(&owner, absent, nil) != nil {
		t.Fatal("nil process result changed")
	}
	owner.Close()
	resource := TrackProcess(&owner, &lifetimeState{}, nil)
	var caught any
	func() { defer func() { caught = recover() }(); owner.Close() }()
	if caught == nil || !strings.Contains(fmt.Sprint(caught), "still open") {
		t.Fatal("process leak was not rejected")
	}
	if !resource.closed {
		t.Fatal("process leak not disposed")
	}
	owner.Close()
	var nilOwner *Boundary
	caught = nil
	resource = &lifetimeState{}
	func() { defer func() { caught = recover() }(); TrackProcess(nilOwner, resource, nil) }()
	if caught == nil || !resource.closed {
		t.Fatal("nil process boundary silently accepted")
	}
}
