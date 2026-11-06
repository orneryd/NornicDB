// Package part2 runs the second of the four partitions of the storage census
// mutant matrix. The cases live once in testing/storagecensus/internal/mutanttest.
package part2

import (
	"testing"

	"github.com/orneryd/nornicdb/testing/storagecensus/internal/mutanttest"
)

// TestCensusRejectsMutants applies partition 1 of the shared mutant matrix
// against the module root.
func TestCensusRejectsMutants(t *testing.T) {
	mutanttest.Run(t, "../../../../", 1)
}
