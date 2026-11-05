package testfixture

import "time"

// WaitForBarrier blocks until ready reports that the goroutine under test
// reached its barrier. On timeout it invokes onTimeout, when supplied, before
// failing with the caller's message so a paused goroutine can be released.
func WaitForBarrier(t interface {
	Helper()
	Fatalf(string, ...any)
}, ready <-chan struct{}, timeout time.Duration, onTimeout func(), failure string) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ready:
	case <-timer.C:
		if onTimeout != nil {
			onTimeout()
		}
		t.Fatalf("%s", failure)
	}
}
