package cypher

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// runCypherTestProcess isolates process-wide observations without rerunning
// unrelated tests. Every caller still checks the child's exit and assertions.
func runCypherTestProcess(t *testing.T, name string, environment ...string) ([]byte, error) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), binary, "-test.run=^"+regexp.QuoteMeta(name)+"$", "-test.count=1", "-test.timeout=30s", "-test.v")
	command.Env = append(os.Environ(), environment...)
	return command.CombinedOutput()
}
