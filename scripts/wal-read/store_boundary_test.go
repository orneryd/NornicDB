package main

import (
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// The command's fixtures open real Badger stores, so the package needs the
// runtime store boundary that observes them after every test cleanup ran.
func TestMain(m *testing.M) {
	os.Exit(fixtureowner.Run(m, "wal-read"))
}
