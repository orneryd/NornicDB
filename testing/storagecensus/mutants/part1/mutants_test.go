// Package part1 runs the first of the four partitions of the storage census
// mutant matrix. The cases live once in testing/storagecensus/internal/mutanttest.
package part1

import (
	"testing"

	"github.com/orneryd/nornicdb/testing/storagecensus/internal/mutanttest"
)

// TestCensusRejectsMutants applies partition 0 of the shared mutant matrix
// against the module root.
func TestCensusRejectsMutants(t *testing.T) {
	mutanttest.Run(t, "../../../../", 0)
}
