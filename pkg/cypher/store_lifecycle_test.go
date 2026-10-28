package cypher

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"os/exec"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Badger starts exactly one doWrites goroutine per open DB, including in-memory
// stores (which have no directory to count). Close waits for this worker to stop.
// Counting workers also covers stores opened indirectly or outside our helpers.
func openBadgerStores() (int, string) {
	return fixtureowner.OpenStores()
}

var processFixtures fixtureowner.Boundary

func TestMain(m *testing.M) {
	boundary := fixtureowner.BeginStoreBoundary()
	code := m.Run()
	code = boundary.Check(code, os.Stderr, "cypher")
	processFixtures.Close()
	os.Exit(code)
}

func TestOpenBadgerStoreCounter(t *testing.T) {
	before, _ := openBadgerStores()
	engine := newTestMemoryEngine(t)
	opened, _ := openBadgerStores()
	require.Equal(t, before+1, opened)
	require.NoError(t, engine.Close())
	closed, _ := openBadgerStores()
	require.Equal(t, before, closed)
}

// Run the real TestMain in a child process to prove it rejects a leaked store.
func TestTestMainRejectsLeakedStore(t *testing.T) {
	const leakEnv = "NORNICDB_TEST_LEAK_STORE"
	if os.Getenv(leakEnv) == "1" {
		_ = fixtureowner.Own(&processFixtures, fixtureowner.TrackProcess(&processFixtures, storage.NewMemoryEngine(), storage.ErrStorageClosed)) // kept alive through the child process leak check
		return
	}
	output, err := runCypherTestProcess(t, "TestTestMainRejectsLeakedStore", leakEnv+"=1")
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(output), "cypher: 1 Badger store(s) left open after tests")
}
