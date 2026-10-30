package searchcontract

import (
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

func TestMain(m *testing.M) { os.Exit(fixtureowner.Run(m, "searchcontract")) }
