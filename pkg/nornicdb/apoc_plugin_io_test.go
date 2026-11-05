package nornicdb

// The shipped APOC plugin (apoc.so, built from apoc/plugin-src/apoc) exports
// real I/O loaders. This check builds and loads that actual binary plugin — as
// the images do — and drives its loaders through public Execute, so the
// statement-bound APOC I/O capability is qualified in the built plugin, not
// only against Go source bound through the test seam.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// apocPluginWatchdog bounds each plugin load's caller lifetime, so a stuck
// handler fails the load instead of hanging the run.
const apocPluginWatchdog = 2 * time.Minute

// TestBundledAPOCPluginLoadersHonorStatementCapability loads the built bundled
// plugin and checks its real I/O loader against the denied default and against
// explicitly allowed import and remote access.
func TestBundledAPOCPluginLoadersHonorStatementCapability(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go buildmode=plugin is unsupported on Windows")
	}
	if testing.Short() {
		t.Skip("builds the APOC plugin")
	}

	pluginsDir := bundledAPOCPluginDir(t)
	resetPluginTestState()
	lookup, list := cypher.PluginFunctionLookup, cypher.PluginFunctionList
	t.Cleanup(func() {
		cypher.PluginFunctionLookup, cypher.PluginFunctionList = lookup, list
		resetPluginTestState()
	})

	// Denied default: the built plugin's loader refuses a test-owned file and a
	// controlled loopback URL, and the loopback server sees no request.
	importRoot := t.TempDir()
	payload := filepath.Join(importRoot, "payload.json")
	require.NoError(t, os.WriteFile(payload, []byte(`[{"name":"neo"}]`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(importRoot, "stream.txt"), []byte("stream-payload"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(importRoot, "binary.txt"), []byte("binary-payload"), 0o600))
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `[{"name":"probe"}]`)
	}))
	t.Cleanup(server.Close)

	denied := openBundledAPOCPluginDatabase(t, pluginsDir)
	requireBundledPluginLoaded(t, denied)
	ctx, cancel := context.WithTimeout(context.Background(), apocPluginWatchdog)
	defer cancel()

	_, err := denied.ExecuteCypher(ctx, "RETURN apoc.load.jsonStream($u) AS value", map[string]interface{}{"u": payload})
	require.ErrorContains(t, err, "local APOC import file access is disabled")
	_, err = denied.ExecuteCypher(ctx, "RETURN apoc.load.jsonStream($u) AS value", map[string]interface{}{"u": server.URL})
	require.ErrorContains(t, err, "remote APOC URL access is disabled")
	require.Zero(t, hits.Load(), "a denied bundled plugin loader must not make an outbound request")
	require.NoError(t, denied.Close())

	// Explicitly allowed access through the same built handlers.
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", importRoot)
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_REMOTE_URL_ACCESS", "true")
	t.Setenv("NORNICDB_APOC_SECURITY_REMOTE_URL_ALLOWLIST", "example.com")

	allowed := openBundledAPOCPluginDatabase(t, pluginsDir)
	requireBundledPluginLoaded(t, allowed)

	result, err := allowed.ExecuteCypher(ctx, "RETURN apoc.load.jsonStream($u) AS value",
		map[string]interface{}{"u": "file:///payload.json"})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	items, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok, "jsonStream returns the document's items: %T", result.Rows[0][0])
	require.Len(t, items, 1)
	require.Equal(t, map[string]interface{}{"name": "neo"}, items[0])

	// Both byte names return the source bytes through the built plugin: the
	// statement retires its context when Execute returns, so a reader could not
	// be read afterwards. Each name is called with a source alone and with the
	// optional config map both names now accept.
	byteCalls := []struct {
		label  string
		query  string
		params map[string]interface{}
	}{
		{"apoc.load.binary/source-alone", "RETURN apoc.load.binary($u) AS value",
			map[string]interface{}{"u": "file:///binary.txt"}},
		{"apoc.load.binary/optional-config", "RETURN apoc.load.binary($u, $c) AS value",
			map[string]interface{}{"u": "file:///binary.txt", "c": map[string]interface{}{}}},
		{"apoc.load.stream/source-alone", "RETURN apoc.load.stream($u) AS value",
			map[string]interface{}{"u": "file:///stream.txt"}},
		{"apoc.load.stream/optional-config", "RETURN apoc.load.stream($u, $c) AS value",
			map[string]interface{}{"u": "file:///stream.txt", "c": map[string]interface{}{}}},
	}
	wantBytes := map[string]string{
		"apoc.load.binary/source-alone":    "binary-payload",
		"apoc.load.binary/optional-config": "binary-payload",
		"apoc.load.stream/source-alone":    "stream-payload",
		"apoc.load.stream/optional-config": "stream-payload",
	}
	for _, call := range byteCalls {
		result, err := allowed.ExecuteCypher(ctx, call.query, call.params)
		require.NoError(t, err, call.label)
		require.Len(t, result.Rows, 1, call.label)
		data, ok := result.Rows[0][0].([]byte)
		require.True(t, ok, "%s returns bytes: %T", call.label, result.Rows[0][0])
		require.Equal(t, wantBytes[call.label], string(data), call.label)
	}

	// The allowlist still gates a remote source the test does not enable.
	_, err = allowed.ExecuteCypher(ctx, "RETURN apoc.load.jsonStream($u) AS value",
		map[string]interface{}{"u": "https://not-allowlisted.example.org/payload.json"})
	require.ErrorContains(t, err, "host is not allowlisted")
}

// openBundledAPOCPluginDatabase opens a database that loads the built plugin.
func openBundledAPOCPluginDatabase(t *testing.T, pluginsDir string) *DB {
	t.Helper()
	resetPluginTestState()
	config := DefaultConfig()
	config.Server.PluginsDir = pluginsDir
	db, err := Open(t.TempDir(), config)
	if db != nil {
		t.Cleanup(func() { _ = db.Close() })
	}
	require.NoError(t, err)
	return db
}

// requireBundledPluginLoaded fails unless the built plugin answered a
// plugin-only function, so the I/O checks above cannot pass on built-ins.
func requireBundledPluginLoaded(t *testing.T, db *DB) {
	t.Helper()
	result, err := db.ExecuteCypher(context.Background(), "RETURN apoc.text.camelCase('hello world') AS v", nil)
	require.NoError(t, err)
	require.Equal(t, "helloWorld", result.Rows[0][0])
}
