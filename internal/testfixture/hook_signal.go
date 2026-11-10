package testfixture

// ObserveHookSignal installs a test hook through install and returns the
// channel each invocation of that hook is reported on, so a test can order
// concurrent work by the hook itself instead of by timing. The channel is
// buffered with the caller's capacity: a report that arrives while the buffer
// is full is dropped rather than blocking the observed work.
//
// install must set the hook and return the function that restores the previous
// one, such as a package's SetSomeHookForTesting. That restore runs when the
// owning test finishes, so the hook's lifetime covers exactly the test.
func ObserveHookSignal(owner interface {
	Helper()
	Cleanup(func())
}, install func(func()) func(), capacity int) <-chan struct{} {
	owner.Helper()
	signals := make(chan struct{}, capacity)
	owner.Cleanup(install(func() {
		select {
		case signals <- struct{}{}:
		default:
		}
	}))
	return signals
}
