package replication

// checkApplyReadiness is the shared admission ordering for required Apply
// adapters. Each mode supplies its own state; a closed mode wins over startup.
func checkApplyReadiness(closed, started bool) error {
	if closed {
		return ErrClosed
	}
	if !started {
		return ErrNotReady
	}
	return nil
}
