// Package cancelprobe owns the allocation-free nonblocking context Done probe.
package cancelprobe

// Ready reports whether a cached context Done channel has fired. A nil channel
// means the context cannot be canceled. Each probe is a single-channel receive;
// callers retain their own latching, error identity and publication policy.
func Ready(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}
