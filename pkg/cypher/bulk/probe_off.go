//go:build !cancellationprobe

package bulk

// probeCheck counts the owner's checks for the cancellation sweep's probe
// build (cancellationprobe); it is empty otherwise.
func probeCheck(*Checkpoint) {}
