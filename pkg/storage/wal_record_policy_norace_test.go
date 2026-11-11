//go:build !race

package storage

// policyTestRaceDetector reports that this test binary runs under the race
// detector, whose instrumentation allocates on its own.
const policyTestRaceDetector = false
