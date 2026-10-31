// Author: Claude, NOR-180 composition owner, 30 Oct 2026.
package compose

import (
	"errors"
	"fmt"
	"testing"
)

func TestOutcomesKeepsPrimaryFirstAndEveryCauseOnce(t *testing.T) {
	primary, cleanup, second := errors.New("primary"), errors.New("cleanup"), errors.New("second")
	if got := Outcomes(nil); got != nil {
		t.Fatalf("no failures: got %v", got)
	}
	if got := Outcomes(nil, nil, cleanup); got != cleanup {
		t.Fatalf("one failure is returned as itself: got %v", got)
	}
	if got := Outcomes(primary, nil); got != primary {
		t.Fatalf("a lone primary is returned as itself: got %v", got)
	}
	got := Outcomes(primary, cleanup, nil, second)
	if !errors.Is(got, primary) || !errors.Is(got, cleanup) || !errors.Is(got, second) {
		t.Fatalf("every cause is retained: %v", got)
	}
	if got.Error() != "primary\ncleanup\nsecond" {
		t.Fatalf("the primary stays first: %q", got.Error())
	}
	wrapped := fmt.Errorf("context: %w", cleanup)
	if got := Outcomes(wrapped, cleanup); got != wrapped {
		t.Fatalf("a cause the primary already wraps is not repeated: %v", got)
	}
}
