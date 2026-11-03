package main

import (
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestServeR4FailureRetainsCleanupOutcome(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("NORNICDB_LANGUAGE", "en")
	t.Setenv("NORNICDB_RETENTION_ENABLED", "true")
	// Opening a directory as the policy file is a real cleanup I/O failure.
	t.Setenv("NORNICDB_RETENTION_POLICIES_FILE", dir)
	t.Setenv("NORNICDB_BOLT_TLS_ENABLED", "true")
	t.Setenv("NORNICDB_BOLT_TLS_CERT", "unused-cert")
	t.Setenv("NORNICDB_BOLT_TLS_KEY", "unused-key")
	t.Setenv("NORNICDB_BOLT_TLS_CLIENT_AUTH_MODE", "r4-invalid-mode")
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	root := newTestServerRoot(manager)
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	config := filepath.Join(dir, "settings.yaml")
	require.NoError(t, os.WriteFile(config, []byte("{}\n"), 0600))
	root.SetArgs([]string{"serve", "--config=" + config, "--headless", "--no-auth", "--data-dir=" + filepath.Join(dir, "data"), "--embedding-enabled=false", "--search-bm25-enabled=false", "--search-vector-enabled=false", "--stdio-log-max-kb=0", "--http-port=0", "--bolt-port=0"})
	result := root.Execute()
	require.Error(t, result)
	require.Contains(t, result.Error(), "client auth", "the real post-construction setup failure must be reached")
	require.Contains(t, result.Error(), "save retention policies", "the command discarded its constructed DB's cleanup outcome")
}

func TestCommandR4RetainsOwnedDBCompletion(t *testing.T) {
	for _, command := range [][]string{{"shell"}, {"decay", "stats"}, {"decay", "suppress"}} {
		t.Run(command[len(command)-1], func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("NORNICDB_RETENTION_ENABLED", "true")
			t.Setenv("NORNICDB_RETENTION_POLICIES_FILE", dir)
			t.Setenv("NORNICDB_EMBEDDING_ENABLED", "false")
			// These three commands deliberately use LoadDefaults, which does not
			// apply the retention-enabled environment override. Decorate only
			// that native constructor input; the DB and all cleanup I/O are real.
			previousOpen := openCommandDB
			openCommandDB = func(path string, cfg *nornicdb.Config) (*nornicdb.DB, error) {
				cfg.Compliance.RetentionEnabled = true
				cfg.Retention.PoliciesFile = dir
				return nornicdb.Open(path, cfg)
			}
			defer func() { openCommandDB = previousOpen }()
			if command[0] == "shell" {
				input, output, err := os.Pipe()
				require.NoError(t, err)
				require.NoError(t, output.Close())
				previous := os.Stdin
				os.Stdin = input
				defer func() { os.Stdin = previous; input.Close() }()
			}
			manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
			require.NoError(t, err)
			root := newTestServerRoot(manager)
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			args := append(append([]string{}, command...), "--data-dir="+filepath.Join(dir, "data"))
			root.SetArgs(args)
			result := root.Execute()
			require.Error(t, result, "command discarded its constructed DB's completion outcome")
			if result != nil {
				require.Contains(t, result.Error(), "save retention policies")
			}
		})
	}
}
