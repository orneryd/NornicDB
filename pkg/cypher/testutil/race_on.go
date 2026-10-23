//go:build race

package testutil

// raceEnabled reports the race detector, which slows every pass: the
// sweep's wall-clock bound applies only without it.
const raceEnabled = true
