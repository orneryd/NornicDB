package search

import "testing"

func TestBuildTriageFailureDuringWarmupAndFinalDrain(t *testing.T) {
	for _, finalDrain := range []bool{false, true} {
		t.Run(map[bool]string{false: "warmup", true: "final_drain"}[finalDrain], func(t *testing.T) {
			var before func(*Service)
			if finalDrain {
				// A persistence setting is an atEnd operation. The writes after
				// it cannot run until the deferred endBuildWrites drain.
				before = func(svc *Service) { svc.SetPersistenceEnabled(true) }
			}
			testTriageLateReplayFailure(t, "starting vector pipeline warmup", before)
		})
	}
}
