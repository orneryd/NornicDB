package cypher

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Badger starts exactly one doWrites goroutine per open DB, including in-memory
// stores (which have no directory to count). Close waits for this worker to stop.
// Counting workers also covers stores opened indirectly or outside our helpers.
func openBadgerStores() (int, string) {
	var stacks bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
		panic(err)
	}
	profile := stacks.String()
	return strings.Count(profile, "github.com/dgraph-io/badger/v4.(*DB).doWrites("), profile
}

func TestMain(m *testing.M) {
	code := m.Run()
	if count, stacks := openBadgerStores(); count != 0 {
		fmt.Fprintf(os.Stderr, "cypher: %d Badger store(s) left open after tests; close stores with t.Cleanup or newTestMemoryEngine\n%s", count, stacks)
		code = 1
	}
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
		_ = storage.NewMemoryEngine() // deliberately leaked only in the child
		return
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(binary, "-test.run=^TestTestMainRejectsLeakedStore$")
	cmd.Env = append(os.Environ(), leakEnv+"=1")
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(output), "cypher: 1 Badger store(s) left open after tests")
}
