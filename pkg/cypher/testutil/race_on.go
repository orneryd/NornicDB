//go:build race

package testutil

// raceSlowdown scales a timing budget under the race detector.
const raceSlowdown = 5
