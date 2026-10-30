package testfixture

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// lifetimeEntry captures the concrete constructor result. Cleanup observes
// whether that exact resource actually closed; names and closure syntax are
// never accepted as evidence of a completed lifetime.
type lifetimeEntry struct {
	resource    Closer
	closedError error
	site        string
}

// releaseOutcomes holds each owner's release outcome until its last cleanup.
var releaseOutcomes sync.Map

// releaseOutcome is the state of the releases one owner runs. A tracked
// lifetime may be released by a wrapper, a release function or the resource
// itself, and only the test knows which. The audit therefore reads the owner's
// outcome: after any failed or unfinished release, the owner has reported it,
// and probing or closing again could block on the state that release holds.
type releaseOutcome struct {
	mu                sync.Mutex
	started, finished int
	failed            bool
}

// outcomeOf returns owner's outcome, registering its removal on first use, so
// the removal runs after every cleanup registered later.
func outcomeOf(owner Owner) *releaseOutcome {
	if !reflect.TypeOf(owner).Comparable() {
		return &releaseOutcome{}
	}
	if outcome, ok := releaseOutcomes.Load(owner); ok {
		return outcome.(*releaseOutcome)
	}
	outcome, loaded := releaseOutcomes.LoadOrStore(owner, &releaseOutcome{})
	if !loaded {
		owner.Cleanup(func() {
			releaseOutcomes.Delete(owner)
			outcome.(*releaseOutcome).passToAncestors(owner)
		})
	}
	return outcome.(*releaseOutcome)
}

// passToAncestors hands a failed or unfinished release to the enclosing tests.
// A lifetime tracked on a parent test may be released on a subtest, whose
// cleanups all finish before the parent's audit runs. A subtest's failure also
// fails its parents, so this suppresses audits only on tests that have failed.
func (outcome *releaseOutcome) passToAncestors(owner Owner) {
	named, ok := owner.(interface{ Name() string })
	if !ok || !outcome.reported() {
		return
	}
	releaseOutcomes.Range(func(key, value any) bool {
		if ancestor, ok := key.(interface{ Name() string }); ok && strings.HasPrefix(named.Name(), ancestor.Name()+"/") {
			inherited := value.(*releaseOutcome)
			inherited.mu.Lock()
			inherited.failed = true
			inherited.mu.Unlock()
		}
		return true
	})
}

// closeOwned closes resource for Own and records the outcome for the audits.
func closeOwned(owner Owner, resource Closer) error {
	outcome := outcomeOf(owner)
	outcome.mu.Lock()
	outcome.started++
	outcome.mu.Unlock()
	err := resource.Close()
	outcome.mu.Lock()
	outcome.finished++
	outcome.failed = outcome.failed || err != nil
	outcome.mu.Unlock()
	return err
}

// reported is true when a release failed or has not finished.
func (outcome *releaseOutcome) reported() bool {
	outcome.mu.Lock()
	defer outcome.mu.Unlock()
	return outcome.failed || outcome.started != outcome.finished
}

// Track registers a fixture with its actual testing.T, B or F. Owner remains
// an interface so existing shared helpers can pass their testing owner through;
// a synthetic boundary cannot replace a real test boundary at runtime.
func Track[T Closer](owner Owner, resource T, closedError error) T {
	testingOwner := requireTestingOwner(owner, resource)
	testingOwner.Helper()
	return track(testingOwner, resource, closedError, lifetimeSite())
}

// TrackResult preserves an error-returning constructor's original results.
// Its curried form keeps the constructor as the only argument and registers a
// non-nil resource even when a constructor returns an error with that resource.
func TrackResult[T Closer](owner Owner, closedError error) func(T, error) (T, error) {
	testingOwner := requireTestingOwner(owner, nil)
	testingOwner.Helper()
	site := lifetimeSite()
	return func(resource T, err error) (T, error) {
		return track(testingOwner, resource, closedError, site), err
	}
}

// OwnResult registers and closes an error-returning fixture at its real test
// boundary. Constructor errors and concrete resources are returned unchanged.
func OwnResult[T Closer](owner Owner, closedError error) func(T, error) (T, error) {
	testingOwner := requireTestingOwner(owner, nil)
	testingOwner.Helper()
	site := lifetimeSite()
	return func(resource T, err error) (T, error) {
		return ownResult(testingOwner, resource, closedError, site), err
	}
}

// OwnResult3 also preserves the recovery metadata returned beside an engine.
func OwnResult3[T Closer, U any](owner Owner, closedError error) func(T, U, error) (T, U, error) {
	testingOwner := requireTestingOwner(owner, nil)
	testingOwner.Helper()
	site := lifetimeSite()
	return func(resource T, result U, err error) (T, U, error) {
		return ownResult(testingOwner, resource, closedError, site), result, err
	}
}

func ownResult[T Closer](owner testing.TB, resource T, closedError error, site string) T {
	resource = track(owner, resource, closedError, site)
	if hasLifetimeResource(resource) {
		Own(owner, resource)
	}
	return resource
}

// TrackProcess is reserved for the process leak detector's deliberate control.
// The repository constructor-entry lint lists that one entrance explicitly.
// Ordinary fixtures must use Track and their real testing owner.
func TrackProcess[T Closer](owner *Boundary, resource T, closedError error) T {
	if owner == nil {
		panic("test fixture requires a non-nil process boundary" + disposeInvalidOwnerResource(resource))
	}
	return track(owner, resource, closedError, lifetimeSite())
}

func lifetimeSite() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", file, line)
}

func requireTestingOwner(owner Owner, resource Closer) testing.TB {
	switch concrete := owner.(type) {
	case *testing.T:
		if concrete != nil {
			return concrete
		}
	case *testing.B:
		if concrete != nil {
			return concrete
		}
	case *testing.F:
		if concrete != nil {
			return concrete
		}
	}
	panic("test fixture requires its actual testing.T, testing.B or testing.F owner" + disposeInvalidOwnerResource(resource))
}

func hasLifetimeResource(resource Closer) bool {
	value := reflect.ValueOf(resource)
	return value.IsValid() && !(value.Kind() == reflect.Pointer && value.IsNil())
}

// disposeInvalidOwnerResource closes a resource refused for its owner and
// returns any close failure for the refusal message.
func disposeInvalidOwnerResource(resource Closer) string {
	if hasLifetimeResource(resource) {
		if err := resource.Close(); err != nil {
			return fmt.Sprintf("; disposal failed: %v", err)
		}
	}
	return ""
}

func track[T Closer](owner Owner, resource T, closedError error, site string) T {
	if !hasLifetimeResource(resource) {
		return resource
	}
	entry := lifetimeEntry{resource: resource, closedError: closedError, site: site}
	outcomeOf(owner)
	owner.Cleanup(func() { owner.Helper(); entry.audit(owner) })
	return resource
}

func (entry lifetimeEntry) isClosed() (bool, bool) {
	if resource, ok := entry.resource.(interface{ IsClosed() bool }); ok {
		return resource.IsClosed(), true
	}
	if resource, ok := entry.resource.(interface{ NodeCount() (int64, error) }); ok && entry.closedError != nil {
		_, err := resource.NodeCount()
		return errors.Is(err, entry.closedError), true
	}
	return false, false
}

func (entry lifetimeEntry) audit(owner Owner) {
	owner.Helper()
	if outcomeOf(owner).reported() {
		return
	}
	closed, supported := entry.isClosed()
	if closed {
		return
	}
	closeErr := entry.resource.Close()
	message := "test fixture still open at cleanup: constructed at " + entry.site
	if !supported {
		message = "test fixture has no closed-state probe: constructed at " + entry.site
	}
	if closeErr != nil {
		message += fmt.Sprintf("; disposal failed: %v", closeErr)
	}
	owner.Errorf("%s", message)
}
