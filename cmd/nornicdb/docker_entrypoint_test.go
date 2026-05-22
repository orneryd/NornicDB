package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestDockerEntrypoint_SearchFlagsPassthrough drives docker/entrypoint.sh
// with NORNICDB_SEARCH_* env vars and verifies the four --search-*
// flags reach the binary's argv.
//
// The Go binary already reads NORNICDB_SEARCH_* directly via
// pkg/config.LoadFromEnv, so the strict correctness story is covered
// without entrypoint involvement. But several CI/audit checks grep for
// --search-* in container logs / `ps` output, and operators expect to
// see overrides reflected in the running process — making the
// passthrough a documented part of the contract. This test is the
// regression guard for that contract.
//
// The test substitutes /app/nornicdb with a shell shim via the
// NORNICDB_BIN escape hatch the entrypoint exposes; the shim records its
// argv to a temp file the test then asserts against.
func TestDockerEntrypoint_SearchFlagsPassthrough(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("entrypoint.sh is /bin/sh; not portable to Windows")
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not on PATH")
	}

	// Locate the script relative to the cmd/nornicdb package directory.
	// `go test` runs in the package dir, so the docker dir is two up.
	scriptPath, err := filepath.Abs(filepath.Join("..", "..", "docker", "entrypoint.sh"))
	if err != nil {
		t.Fatalf("resolving entrypoint path: %v", err)
	}
	if _, err := os.Stat(scriptPath); err != nil {
		t.Fatalf("entrypoint script not found at %s: %v", scriptPath, err)
	}

	tmp := t.TempDir()
	argvOut := filepath.Join(tmp, "argv.txt")

	// Shim: writes its argv (one per line) to argvOut, then exits 0. The
	// entrypoint `exec`s this in place of /app/nornicdb. We give it a
	// distinctive marker on the first line so a partial overwrite from a
	// previous run can't fool the assertion.
	shimPath := filepath.Join(tmp, "nornicdb-shim.sh")
	shimBody := fmt.Sprintf(`#!/bin/sh
{
  echo "__SHIM_INVOKED__"
  for a in "$@"; do
    echo "$a"
  done
} > %q
exit 0
`, argvOut)
	if err := os.WriteFile(shimPath, []byte(shimBody), 0o755); err != nil {
		t.Fatalf("writing shim: %v", err)
	}

	cases := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{
			name: "all four set",
			env: map[string]string{
				"NORNICDB_SEARCH_BM25_ENABLED":   "false",
				"NORNICDB_SEARCH_BM25_WARMING":   "lazy",
				"NORNICDB_SEARCH_VECTOR_ENABLED": "false",
				"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",
			},
			want: []string{
				"--search-bm25-enabled=false",
				"--search-bm25-warming=lazy",
				"--search-vector-enabled=false",
				"--search-vector-warming=lazy",
			},
		},
		{
			name: "bm25 only",
			env: map[string]string{
				"NORNICDB_SEARCH_BM25_ENABLED": "true",
				"NORNICDB_SEARCH_BM25_WARMING": "startup",
			},
			want: []string{
				"--search-bm25-enabled=true",
				"--search-bm25-warming=startup",
			},
		},
		{
			name: "vector only",
			env: map[string]string{
				"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
				"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",
			},
			want: []string{
				"--search-vector-enabled=true",
				"--search-vector-warming=lazy",
			},
		},
		{
			name: "none set — no search flags appear",
			env:  nil,
			want: nil, // verified via "none of the --search-* flags appear" check below
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset the argv capture file so a previous run's content
			// can't leak into this assertion.
			_ = os.Remove(argvOut)

			cmd := exec.Command("sh", scriptPath)
			// Sandbox env: only the variables this test cares about.
			// Inheriting the parent env would mix in whatever the dev's
			// shell exports for nornicdb (config paths, embedding URL
			// etc) and turn the test into a flake under unusual local
			// setups.
			env := []string{
				"PATH=" + os.Getenv("PATH"),
				"NORNICDB_BIN=" + shimPath,
				// Force a deterministic data dir so the script doesn't
				// fall back to /data and require root in CI sandboxes.
				"NORNICDB_DATA_DIR=" + tmp,
			}
			for k, v := range tc.env {
				env = append(env, k+"="+v)
			}
			cmd.Env = env

			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("entrypoint failed: %v\noutput: %s", err, out)
			}

			argvBytes, err := os.ReadFile(argvOut)
			if err != nil {
				t.Fatalf("argv capture not produced (shim never ran?): %v\nentrypoint output: %s",
					err, out)
			}
			argv := strings.Split(strings.TrimSpace(string(argvBytes)), "\n")
			if len(argv) == 0 || argv[0] != "__SHIM_INVOKED__" {
				t.Fatalf("shim marker missing in argv capture: %v", argv)
			}
			argv = argv[1:]

			// Build a set for membership checks.
			seen := make(map[string]bool, len(argv))
			for _, a := range argv {
				seen[a] = true
			}

			for _, want := range tc.want {
				if !seen[want] {
					t.Errorf("expected argv to include %q, got: %v", want, argv)
				}
			}

			// When no NORNICDB_SEARCH_* env was set, none of the
			// passthrough flags should appear at all.
			if tc.env == nil {
				for _, prefix := range []string{
					"--search-bm25-enabled",
					"--search-bm25-warming",
					"--search-vector-enabled",
					"--search-vector-warming",
				} {
					for _, a := range argv {
						if strings.HasPrefix(a, prefix+"=") {
							t.Errorf("expected no %s flag when env unset, got %q", prefix, a)
						}
					}
				}
			}
		})
	}
}
