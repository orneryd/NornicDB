package heimdall

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeneratedPluginFixtureStaysOutsideRepositoryPackages(t *testing.T) {
	pluginPath := buildCoverageHeimdallPlugin(t, "package main\nvar FixtureMarker = true\n")
	cwd, err := os.Getwd()
	require.NoError(t, err)
	command := exec.Command("go", "list", "-find", "-f", "{{.Dir}}", "./...")
	command.Dir = filepath.Join(cwd, "../..")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "repository package discovery: %s", output)
	require.NotContains(t, strings.Split(strings.TrimSpace(string(output)), "\n"), filepath.Dir(pluginPath),
		"generated plugin sources must not enter the package population scanned by concurrent tests")
}
