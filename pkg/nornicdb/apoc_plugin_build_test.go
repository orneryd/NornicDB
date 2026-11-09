package nornicdb

// The bundled APOC plugin can be loaded once per test process: the Go plugin
// runtime refuses a second copy of the same plugin, so every check that needs
// the real apoc.so shares one build. Building from the shipped plugin source
// keeps these checks on the binary the images install.

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/pluginbuild"
	"github.com/stretchr/testify/require"
)

var (
	bundledAPOCPluginOnce sync.Once
	bundledAPOCPluginPath string
	bundledAPOCPluginErr  error
)

// bundledAPOCPluginDir builds apoc.so once per test process from
// apoc/plugin-src/apoc and returns the directory holding it, which is the
// plugin directory a database opens with. The directory outlives a single test
// because the loaded plugin cannot be unloaded, so later tests must reuse it.
func bundledAPOCPluginDir(t *testing.T) string {
	t.Helper()
	bundledAPOCPluginOnce.Do(func() {
		dir, err := os.MkdirTemp("", "nornicdb-apoc-plugin-")
		if err != nil {
			bundledAPOCPluginErr = err
			return
		}
		bundledAPOCPluginPath = dir
		source, err := filepath.Abs(filepath.Join("..", "..", "apoc", "plugin-src", "apoc"))
		if err != nil {
			bundledAPOCPluginErr = err
			return
		}
		buildConfig, err := pluginbuild.Running()
		if err != nil {
			bundledAPOCPluginErr = err
			return
		}
		args := append([]string{"build"}, buildConfig.Flags...)
		args = append(args, "-buildmode=plugin", "-o", filepath.Join(dir, "apoc.so"), "apoc_plugin.go")
		build := exec.Command("go", args...)
		build.Dir = source
		build.Env = append(os.Environ(), buildConfig.Env...)
		out, err := build.CombinedOutput()
		if err != nil {
			bundledAPOCPluginErr = err
			t.Logf("go build apoc plugin: %s", out)
		}
	})
	require.NoError(t, bundledAPOCPluginErr, "build the bundled APOC plugin")
	return bundledAPOCPluginPath
}
