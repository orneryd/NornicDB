package main

import "testing"

func TestApplySuppressionCounters(t *testing.T) {
	t.Run("counts real transitions as newly suppressed", func(t *testing.T) {
		newlySuppressed := 0
		alreadySuppressed := 0

		applySuppressionCounters(true, &newlySuppressed, &alreadySuppressed)

		if newlySuppressed != 1 {
			t.Fatalf("expected newlySuppressed=1, got %d", newlySuppressed)
		}
		if alreadySuppressed != 0 {
			t.Fatalf("expected alreadySuppressed=0, got %d", alreadySuppressed)
		}
	})

	t.Run("counts no-op rechecks as already suppressed", func(t *testing.T) {
		newlySuppressed := 0
		alreadySuppressed := 0

		applySuppressionCounters(false, &newlySuppressed, &alreadySuppressed)

		if newlySuppressed != 0 {
			t.Fatalf("expected newlySuppressed=0, got %d", newlySuppressed)
		}
		if alreadySuppressed != 1 {
			t.Fatalf("expected alreadySuppressed=1, got %d", alreadySuppressed)
		}
	})
}
