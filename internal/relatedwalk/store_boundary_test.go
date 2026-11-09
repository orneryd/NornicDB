package relatedwalk

import (
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// TestMain checks the runtime store population at the package boundary: the
// adapter tests construct real storage engines.
func TestMain(m *testing.M) { os.Exit(fixtureowner.Run(m, "relatedwalk")) }
