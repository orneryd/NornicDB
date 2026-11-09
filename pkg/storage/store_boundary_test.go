package storage

import (
	"flag"
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if bench := flag.Lookup("test.bench"); bench == nil || bench.Value.String() == "" {
		lockHoldHooks = lockHolds
	}
	os.Exit(checkStorageLockHolds(fixtureowner.Run(m, "storage")))
}
