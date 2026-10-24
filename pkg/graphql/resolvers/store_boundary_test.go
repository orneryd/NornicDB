package resolvers

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"testing"
)

func TestMain(m *testing.M) { os.Exit(fixtureowner.Run(m, "resolvers")) }
