package testfixture

import (
	"fmt"
	"testing"
	"time"
)

type barrierReporter struct {
	helpers int
	message string
	onFatal func()
}

func (r *barrierReporter) Helper() { r.helpers++ }
func (r *barrierReporter) Fatalf(format string, args ...any) {
	if r.onFatal != nil {
		r.onFatal()
	}
	r.message = fmt.Sprintf(format, args...)
}

func TestWaitForBarrier(t *testing.T) {
	t.Run("arrival", func(t *testing.T) {
		ready := make(chan struct{})
		close(ready)
		report := &barrierReporter{}
		released := false
		WaitForBarrier(report, ready, time.Second, func() { released = true }, "missing")
		if report.helpers != 1 || report.message != "" || released {
			t.Fatalf("unexpected arrival outcome: %+v, released=%v", report, released)
		}
	})
	t.Run("timeout releases before reporting", func(t *testing.T) {
		report := &barrierReporter{}
		released := false
		var releasedAtFailure bool
		report.onFatal = func() { releasedAtFailure = released }
		WaitForBarrier(report, nil, 0, func() { released = true }, "specific failure")
		if report.helpers != 1 || report.message != "specific failure" || !releasedAtFailure {
			t.Fatalf("unexpected timeout outcome: %+v, released=%v", report, released)
		}
	})
}
