package interactive_reads_test

import (
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

func TestMain(m *testing.M) { os.Exit(fixtureowner.Run(m, "interactive_reads")) }
