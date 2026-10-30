package observability

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPackageBoundary_NoBusinessImports enforces OBS-01: pkg/observability is a
// leaf in the import graph. It must not (transitively) import any of the
// NornicDB business packages — doing so would let observability code reach
// into Cypher, storage, Bolt, etc., which is a Phase 1 architectural contract.
//
// Implementation: shell out to `go list -deps ./pkg/observability/...` and
// grep for forbidden packages. We run from the repository root.
func TestPackageBoundary_NoBusinessImports(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go binary not on PATH; skipping boundary check")
	}

	// Walk up to the module root (where go.mod lives).
	root, err := filepath.Abs("..")
	require.NoError(t, err)
	root, err = filepath.Abs(filepath.Join(root, ".."))
	require.NoError(t, err)

	cmd := exec.Command("go", "list", "-deps", "./pkg/observability/...")
	cmd.Dir = root
	out, err := cmd.Output()
	require.NoError(t, err, "go list -deps failed: %s", out)

	forbidden := []string{
		"github.com/orneryd/nornicdb/pkg/cypher",
		"github.com/orneryd/nornicdb/pkg/storage",
		"github.com/orneryd/nornicdb/pkg/bolt",
		"github.com/orneryd/nornicdb/pkg/server",
		"github.com/orneryd/nornicdb/pkg/nornicdb",
		"github.com/orneryd/nornicdb/pkg/search",
		"github.com/orneryd/nornicdb/pkg/inference",
		"github.com/orneryd/nornicdb/pkg/replication",
	}

	for _, dep := range strings.Split(string(out), "\n") {
		dep = strings.TrimSpace(dep)
		if dep == "" {
			continue
		}
		for _, bad := range forbidden {
			require.NotEqual(t, bad, dep,
				"OBS-01 violation: pkg/observability imports forbidden business package %q", bad)
			// Also catch sub-packages (e.g. pkg/cypher/foo).
			require.False(t, strings.HasPrefix(dep, bad+"/"),
				"OBS-01 violation: pkg/observability imports forbidden business sub-package %q", dep)
		}
	}
}
