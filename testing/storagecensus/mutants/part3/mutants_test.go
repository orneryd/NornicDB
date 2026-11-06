// Package part3 runs the third of the four partitions of the storage census
// mutant matrix. The cases live once in testing/storagecensus/internal/mutanttest.
package part3

import (
	"testing"

	"github.com/orneryd/nornicdb/testing/storagecensus/internal/mutanttest"
)

// TestCensusRejectsMutants applies partition 2 of the shared mutant matrix
// against the module root.
func TestCensusRejectsMutants(t *testing.T) {
	mutanttest.Run(t, "../../../../", 2)
}
