// Package part4 runs the fourth of the four partitions of the storage census
// mutant matrix. The cases live once in testing/storagecensus/internal/mutanttest.
package part4

import (
	"testing"

	"github.com/orneryd/nornicdb/testing/storagecensus/internal/mutanttest"
)

// TestCensusRejectsMutants applies partition 3 of the shared mutant matrix
// against the module root.
func TestCensusRejectsMutants(t *testing.T) {
	mutanttest.Run(t, "../../../../", 3)
}
