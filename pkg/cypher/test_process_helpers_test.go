package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
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

// withCypherParsers runs assertions under each process-wide parser selection.
// The existing process owner bounds child lifetime and preserves its output.
func withCypherParsers(t *testing.T, body func(*testing.T)) {
	t.Helper()
	const marker = "NORNIC_CYPHER_TEST_PARSER_VARIANT"
	if expected := os.Getenv(marker); expected != "" {
		require.Equal(t, expected, config.GetParserType())
		body(t)
		return
	}
	name := t.Name()
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			output, err := runCypherTestProcess(t, name, marker+"="+parser, config.EnvParserType+"="+parser)
			require.NoError(t, err, string(output))
			t.Log(string(output))
		})
	}
}

// forEachCypherParserSetting exercises both admitted normalization settings
// under each parser and restores the process-wide settings after each case.
func forEachCypherParserSetting(t *testing.T, body func(*testing.T)) {
	t.Helper()
	forEachParser(t, func(t *testing.T) {
		for _, normalization := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
				previous := config.IsCypherQueryNormalizationEnabled()
				t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
				config.SetCypherQueryNormalizationEnabled(normalization)
				body(t)
			})
		}
	})
}
