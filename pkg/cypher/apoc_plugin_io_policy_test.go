package cypher

// The bundled scalar APOC plugin exports real I/O loaders. These regressions
// bind its actual Go handlers through the production plugin lookup seam and
// drive them through public Execute: the handlers must admit I/O only through
// the statement's typed APOC I/O capability, deny by default, and keep the
// caller's context. See pkg/apocio for the shared owner and
// docs/features/apoc-functions.md for the migration note.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/apoc"
	apocload "github.com/orneryd/nornicdb/apoc/load"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// apocPluginIOLoaders returns the bundled scalar plugin's real I/O handlers:
// the apoc.load plugin exports that open a URL or a file. Every other apoc.load
// plugin export is a placeholder that performs no I/O, so it cannot reach the
// policy; apoc/apoc_load_inventory_test.go pins that population.
func apocPluginIOLoaders() map[string]interface{} {
	return map[string]interface{}{
		"apoc.load.jsonStream": apocload.JSONItems(apocload.DocumentItems),
		"apoc.load.csvStream":  apocload.Csv,
		"apoc.load.xmlSimple":  apocload.Xml,
		"apoc.load.graphQL":    apocload.GraphQL,
		"apoc.load.rest":       apocload.Rest,
		"apoc.load.binary":     apocload.Binary,
		"apoc.load.stream":     apocload.Binary,
	}
}

// apocPluginIOCall is one public call of a bundled I/O loader.
type apocPluginIOCall struct {
	name   string
	query  string
	source string // URL path or file name the case supplies
	// localSource reports whether this loader's transport reads a local import
	// file. apoc.load.graphQL always issues a POST with a query body, so its
	// source is a remote endpoint and a local source is refused.
	localSource bool
}

func apocPluginIOCalls() []apocPluginIOCall {
	return []apocPluginIOCall{
		{name: "apoc.load.jsonStream", query: "RETURN apoc.load.jsonStream($source) AS value", source: "/jsonstream", localSource: true},
		{name: "apoc.load.csvStream", query: "RETURN apoc.load.csvStream($source, $config) AS value", source: "/csvstream", localSource: true},
		{name: "apoc.load.xmlSimple", query: "RETURN apoc.load.xmlSimple($source) AS value", source: "/xmlsimple", localSource: true},
		{name: "apoc.load.graphQL", query: "RETURN apoc.load.graphQL($source, 'query { value }', $config) AS value", source: "/graphql"},
		{name: "apoc.load.rest", query: "RETURN apoc.load.rest($source, $config) AS value", source: "/rest", localSource: true},
		{name: "apoc.load.binary", query: "RETURN apoc.load.binary($source) AS value", source: "/binary", localSource: true},
		{name: "apoc.load.stream", query: "RETURN apoc.load.stream($source, $config) AS value", source: "/stream", localSource: true},
	}
}

// apocPluginIOFixtures holds the controlled payload per loader, for both the
// remote path and the import-root file name.
var apocPluginIOFixtures = map[string]string{
	"/jsonstream":    `[{"name":"neo"}]`,
	"/csvstream":     "name,age\nalice,30\n",
	"/xmlsimple":     "<root/>",
	"/graphql":       `{"data":{"user":"neo"}}`,
	"/rest":          `{"name":"neo"}`,
	"/binary":        "binary-payload",
	"/stream":        "stream-payload",
	"jsonstream.txt": `[{"name":"neo"}]`,
	"csvstream.txt":  "name,age\nalice,30\n",
	"xmlsimple.txt":  "<root/>",
	"graphql.txt":    `{"data":{"user":"neo"}}`,
	"rest.txt":       `{"name":"neo"}`,
	"binary.txt":     "binary-payload",
	"stream.txt":     "stream-payload",
}

// apocPluginIOFileName is the import-root file name a call reads: the loader's
// remote path without its leading slash, plus .txt.
func apocPluginIOFileName(call apocPluginIOCall) string {
	return strings.TrimPrefix(call.source, "/") + ".txt"
}

func apocPluginIOParams(source string) map[string]interface{} {
	return map[string]interface{}{
		"source": source,
		"config": map[string]interface{}{},
	}
}

// apocPluginIOTransport answers every request from apocPluginIOFixtures and
// records what it observed.
type apocPluginIOTransport struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []string
}

func (f *apocPluginIOTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body := ""
	if req.Body != nil {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = string(data)
	}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(apocPluginIOFixtures[req.URL.Path])),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func (f *apocPluginIOTransport) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// allowAPOCPluginRemoteAccess configures an executor for explicitly allowed
// remote APOC loads: access on, the test host allowlisted, a resolver that
// answers with a public address (the address policy never dials this fixture)
// and the recording transport.
func allowAPOCPluginRemoteAccess(exec *StorageExecutor, transport *apocPluginIOTransport) {
	exec.SetAllowRemoteAPOCURLAccess(true)
	exec.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
	})
	exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: transport})
}

// TestBundledAPOCScalarLoaderHonorsDefaultPolicy is the release regression:
// with the default (disabled) APOC I/O configuration, the actual bundled
// handlers must not read a test-owned file or fetch a controlled loopback
// response. It is the port of the failing public-policy reproduction.
func TestBundledAPOCScalarLoaderHonorsDefaultPolicy(t *testing.T) {
	withPluginFunctions(t, map[string]interface{}{"apoc.load.jsonStream": apocload.JSONItems(apocload.DocumentItems)})

	t.Run("remote-disabled", func(t *testing.T) {
		var hits atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = io.WriteString(w, `["policy-probe"]`)
		}))
		t.Cleanup(server.Close)
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(context.Background(), "RETURN apoc.load.jsonStream($source) AS value", map[string]interface{}{"source": server.URL})
		t.Logf("local controlled HTTP accesses=%d error=%v", hits.Load(), err)
		require.ErrorContains(t, err, "remote APOC URL access is disabled")
		require.Nil(t, result)
		require.Zero(t, hits.Load())
	})

	t.Run("file-disabled", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "owned.json")
		require.NoError(t, os.WriteFile(file, []byte(`["owned-file-probe"]`), 0o600))
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(context.Background(), "RETURN apoc.load.jsonStream($source) AS value", map[string]interface{}{"source": file})
		t.Logf("owned local file read error=%v returned_result=%t", err, result != nil)
		require.ErrorContains(t, err, "local APOC import file access is disabled")
		require.Nil(t, result)
	})
}

// TestBundledAPOCScalarLoadersDenyByDefault proves the complete plugin loader
// population is denied by default with no side effect: every real I/O loader
// refuses a controlled loopback URL without reaching it, and refuses a
// test-owned file.
func TestBundledAPOCScalarLoadersDenyByDefault(t *testing.T) {
	withPluginFunctions(t, apocPluginIOLoaders())

	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `["policy-probe"]`)
	}))
	t.Cleanup(server.Close)

	file := filepath.Join(t.TempDir(), "owned.json")
	require.NoError(t, os.WriteFile(file, []byte(`["owned-file-probe"]`), 0o600))

	for _, call := range apocPluginIOCalls() {
		call := call
		t.Run(call.name+"/remote-disabled", func(t *testing.T) {
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			before := hits.Load()
			result, err := exec.Execute(context.Background(), call.query, apocPluginIOParams(server.URL))
			require.ErrorIs(t, err, apocio.ErrRemoteURLAccessDisabled)
			require.Nil(t, result)
			require.Equal(t, before, hits.Load(), "a denied APOC load must not make an outbound request")
		})
		t.Run(call.name+"/file-disabled", func(t *testing.T) {
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			result, err := exec.Execute(context.Background(), call.query, apocPluginIOParams(file))
			require.Error(t, err)
			require.Nil(t, result)
			if call.localSource {
				require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)
				return
			}
			// This loader always issues a remote POST, so a local source is refused
			// by its transport before any file could be opened.
			require.ErrorContains(t, err, "expected local APOC file source, got POST request with options")
		})
	}
}

// TestBundledAPOCScalarLoadersAllowExplicitAccess keeps the existing allowed
// behavior through the same handlers: an allowlisted remote URL and an import
// root file both load, with each loader's format and result preserved.
func TestBundledAPOCScalarLoadersAllowExplicitAccess(t *testing.T) {
	withPluginFunctions(t, apocPluginIOLoaders())

	t.Run("allowlisted-remote", func(t *testing.T) {
		transport := &apocPluginIOTransport{}
		for _, call := range apocPluginIOCalls() {
			call := call
			t.Run(call.name, func(t *testing.T) {
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				allowAPOCPluginRemoteAccess(exec, transport)
				before := transport.requestCount()
				result, err := exec.Execute(context.Background(), call.query, apocPluginIOParams("https://example.com"+call.source))
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, before+1, transport.requestCount())
				apocPluginIOAssertValue(t, call, result.Rows[0][0])
			})
		}
		// The GraphQL loader keeps its POST request with the query body.
		transport.mu.Lock()
		defer transport.mu.Unlock()
		for i, req := range transport.requests {
			if req.URL.Path != "/graphql" {
				continue
			}
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Contains(t, transport.bodies[i], `"query":"query { value }"`)
		}
	})

	t.Run("import-root", func(t *testing.T) {
		root := t.TempDir()
		for name, payload := range apocPluginIOFixtures {
			if !strings.HasSuffix(name, ".txt") {
				continue
			}
			require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(payload), 0o600))
		}
		for _, call := range apocPluginIOCalls() {
			call := call
			t.Run(call.name, func(t *testing.T) {
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				exec.SetAllowLocalAPOCImportFileAccess(true)
				exec.SetAPOCLocalFileAccessRoot(root)
				result, err := exec.Execute(context.Background(), call.query, apocPluginIOParams("file:///"+apocPluginIOFileName(call)))
				if !call.localSource {
					// The loader's transport is a remote POST with a query body, so a
					// local source stays refused instead of being read.
					require.ErrorContains(t, err, "expected local APOC file source, got POST request with options")
					require.Nil(t, result)
					return
				}
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				apocPluginIOAssertValue(t, call, result.Rows[0][0])
			})
		}
	})

	t.Run("allowlisted-host-and-address-still-enforced", func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		transport := &apocPluginIOTransport{}
		allowAPOCPluginRemoteAccess(exec, transport)

		// A host that is not allowlisted is refused before any request.
		_, err := exec.Execute(context.Background(), "RETURN apoc.load.binary($source) AS value", apocPluginIOParams("https://not-allowed.example.org/file.bin"))
		require.ErrorIs(t, err, apocio.ErrRemoteURLHostNotAllowed)
		require.Zero(t, transport.requestCount())

		// An allowlisted host that resolves to a loopback address is refused.
		exec.SetAPOCRemoteURLAllowlist([]string{"loopback.example.com"})
		exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
		})
		_, err = exec.Execute(context.Background(), "RETURN apoc.load.binary($source) AS value", apocPluginIOParams("https://loopback.example.com/file.bin"))
		require.ErrorContains(t, err, "resolves to a disallowed address")
		require.Zero(t, transport.requestCount())
	})
}

// apocPluginIORows reads a loader's rows from the query value domain, where a
// Go []map[string]interface{} result is a list of Cypher maps.
func apocPluginIORows(value interface{}) ([]map[string]interface{}, bool) {
	switch rows := value.(type) {
	case []map[string]interface{}:
		return rows, true
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(rows))
		for _, row := range rows {
			m, ok := row.(map[string]interface{})
			if !ok {
				return nil, false
			}
			out = append(out, m)
		}
		return out, true
	}
	return nil, false
}

// apocPluginIOAssertValue checks the loader's result domain survived the
// migration unchanged.
func apocPluginIOAssertValue(t *testing.T, call apocPluginIOCall, value interface{}) {
	t.Helper()
	switch call.name {
	case "apoc.load.jsonStream":
		items, ok := value.([]interface{})
		require.True(t, ok, "jsonStream returns the document's items: %T", value)
		require.Len(t, items, 1)
		row, ok := items[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "neo", row["name"])
	case "apoc.load.csvStream":
		rows, ok := apocPluginIORows(value)
		require.True(t, ok, "csvStream returns rows: %T", value)
		require.Len(t, rows, 1)
		assert.Equal(t, "alice", rows[0]["name"])
		assert.Equal(t, "30", rows[0]["age"])
	case "apoc.load.xmlSimple":
		document, ok := value.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "xml data", document["root"])
	case "apoc.load.graphQL":
		data, ok := value.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "neo", data["user"])
	case "apoc.load.rest":
		data, ok := value.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "neo", data["name"])
	case "apoc.load.binary":
		assert.Equal(t, "binary-payload", string(value.([]byte)))
	case "apoc.load.stream":
		// The Cypher export returns bytes: a statement retires its context
		// when Execute returns, so a reader could not be read here.
		data, ok := value.([]byte)
		require.True(t, ok, "stream returns bytes: %T", value)
		assert.Equal(t, "stream-payload", string(data))
	default:
		t.Fatalf("unclassified loader %s", call.name)
	}
}

// TestBundledAPOCScalarLoaderFormatAndErrors keeps the loaders' ordinary
// format and error behavior: a JSON object stream, CSV options, a GraphQL
// response without data, and a non-200 response.
func TestBundledAPOCScalarLoaderFormatAndErrors(t *testing.T) {
	withPluginFunctions(t, apocPluginIOLoaders())

	newExec := func(t *testing.T, body string, status int) *StorageExecutor {
		t.Helper()
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		exec.SetAllowRemoteAPOCURLAccess(true)
		exec.SetAPOCRemoteURLAllowlist([]string{"example.com"})
		exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		})
		exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: status,
				Status:     http.StatusText(status),
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})})
		return exec
	}

	t.Run("json-object-document-is-one-item", func(t *testing.T) {
		exec := newExec(t, `{"name":"neo"}`, http.StatusOK)
		result, err := exec.Execute(context.Background(), "RETURN apoc.load.jsonStream($source) AS value", apocPluginIOParams("https://example.com/object.json"))
		require.NoError(t, err)
		items := result.Rows[0][0].([]interface{})
		require.Len(t, items, 1)
		assert.Equal(t, map[string]interface{}{"name": "neo"}, items[0])
	})

	t.Run("json-invalid-document-errors", func(t *testing.T) {
		exec := newExec(t, "not-json", http.StatusOK)
		_, err := exec.Execute(context.Background(), "RETURN apoc.load.jsonStream($source) AS value", apocPluginIOParams("https://example.com/bad.json"))
		require.ErrorContains(t, err, "failed to load JSON")
	})

	t.Run("csv-header-and-separator-options", func(t *testing.T) {
		exec := newExec(t, "x;1\ny;2\n", http.StatusOK)
		params := map[string]interface{}{
			"source": "https://example.com/data.csv",
			"config": map[string]interface{}{"header": false, "sep": ";"},
		}
		result, err := exec.Execute(context.Background(), "RETURN apoc.load.csvStream($source, $config) AS value", params)
		require.NoError(t, err)
		rows, ok := apocPluginIORows(result.Rows[0][0])
		require.True(t, ok, "csvStream returns rows: %T", result.Rows[0][0])
		require.Len(t, rows, 2)
		assert.Equal(t, "x", rows[0]["col0"])
		assert.Equal(t, "1", rows[0]["col1"])
	})

	t.Run("graphql-response-without-data", func(t *testing.T) {
		exec := newExec(t, `{"errors":[{"message":"nope"}]}`, http.StatusOK)
		result, err := exec.Execute(context.Background(), "RETURN apoc.load.graphQL($source, 'query { value }', $config) AS value", apocPluginIOParams("https://example.com/graphql"))
		require.NoError(t, err)
		assert.Nil(t, result.Rows[0][0])
	})

	t.Run("non-200-response-errors", func(t *testing.T) {
		exec := newExec(t, "unavailable", http.StatusServiceUnavailable)
		_, err := exec.Execute(context.Background(), "RETURN apoc.load.binary($source) AS value", apocPluginIOParams("https://example.com/file.bin"))
		require.ErrorContains(t, err, "HTTP error")
	})
}

// TestBundledAPOCScalarLoaderCallerContextCancellation proves the plugin
// loader's URL resolution and outbound request run on the caller's statement
// context: a canceled or expired caller ends the load instead of returning
// data it no longer waits for.
func TestBundledAPOCScalarLoaderCallerContextCancellation(t *testing.T) {
	withPluginFunctions(t, apocPluginIOLoaders())

	seams := []struct {
		name string
		hold string
	}{
		{"allowlist_dns_resolution", apocSeamDNS},
		{"outbound_http", apocSeamHTTP},
	}
	variants := []struct {
		name         string
		withDeadline bool
	}{
		{"cancellation", false},
		{"deadline", true},
	}

	for _, seam := range seams {
		for _, variant := range variants {
			name := strings.Join([]string{"apoc.load.jsonStream", seam.name, variant.name}, "/")
			t.Run(name, func(t *testing.T) {
				// Fake time advances only after actual Execute work is blocked.
				synctest.Test(t, func(t *testing.T) {
					runAPOCPluginLoaderCallerContextCase(t, seam.hold, variant.withDeadline)
				})
			})
		}
	}
}

func runAPOCPluginLoaderCallerContextCase(t *testing.T, hold string, withDeadline bool) {
	t.Helper()

	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetAllowRemoteAPOCURLAccess(true)
	exec.SetAPOCRemoteURLAllowlist([]string{apocCancellationAllowed})

	dns := &apocCancellationDNS{apocCancellationGate: newAPOCCancellationGate(hold == apocSeamDNS)}
	remote := &apocCancellationHTTP{apocCancellationGate: newAPOCCancellationGate(hold == apocSeamHTTP)}
	exec.apocIO = exec.apocIO.WithHostResolver(dns.resolve)
	exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: remote})

	parent := context.WithValue(context.Background(), apocCancellationMarkerKey{}, apocCancellationMarkerValue)
	var ctx context.Context
	var cancel context.CancelFunc
	if withDeadline {
		ctx, cancel = context.WithTimeout(parent, apocCancellationDeadline)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	outcome := make(chan apocCancellationOutcome, 1)
	go func() {
		result, err := exec.Execute(ctx, "RETURN apoc.load.jsonStream($u) AS value", map[string]interface{}{"u": apocCancellationRemoteURL})
		outcome <- apocCancellationOutcome{result: result, err: err}
	}()

	joined := false
	t.Cleanup(func() {
		dns.stop()
		remote.stop()
		if joined {
			return
		}
		select {
		case <-outcome:
		case <-time.After(apocCancellationWaitLimit):
			t.Errorf("APOC plugin loader statement did not finish after its fixtures were released")
		}
	})

	entered := dns.entered
	if hold == apocSeamHTTP {
		entered = remote.entered
	}
	select {
	case <-entered:
	case <-time.After(apocCancellationWaitLimit):
		t.Fatalf("held %s work never started", hold)
	}
	if !withDeadline {
		cancel()
	}

	wantErr := context.Canceled
	if withDeadline {
		wantErr = context.DeadlineExceeded
	}

	var got apocCancellationOutcome
	select {
	case got = <-outcome:
		joined = true
	case <-time.After(apocCancellationWaitLimit):
		t.Fatalf("APOC plugin loader did not return within %s of the caller's %v", apocCancellationWaitLimit, wantErr)
	}

	require.Error(t, got.err, "a canceled APOC plugin loader must fail")
	require.Nil(t, got.result, "a canceled APOC plugin loader must not return rows")
	require.ErrorIs(t, got.err, wantErr)

	if hold == apocSeamDNS {
		seen := dns.seenContexts()
		require.Len(t, seen, 1)
		assert.Equal(t, wantErr, seen[0].Err(), "allowlist DNS resolution did not run on the caller's context")
		assert.Equal(t, apocCancellationMarkerValue, seen[0].Value(apocCancellationMarkerKey{}),
			"allowlist DNS resolution ran outside the caller's statement context")
		assert.Zero(t, remote.requestCount(), "a canceled DNS resolution must not be followed by an outbound request")
		return
	}
	require.Equal(t, 1, remote.requestCount())
	assert.Equal(t, wantErr, remote.requestContextError(), "the outbound request did not carry the caller's context")
}

// TestBundledAPOCScalarLoaderRequiresCapability proves a plugin loader invoked
// with a context that carries no typed capability performs no I/O and does not
// fall back to unrestricted access.
// TestBundledAPOCIOLoaderPopulationMatchesInventory proves the regressions
// above cover every bundled apoc.load export that performs real I/O: the handler
// set they bind equals the plugin's context-first apoc.load exports, which
// apoc/apoc_load_inventory_test.go pins apart from the placeholder stubs. A new
// real loader therefore has to be added to these policy checks.
func TestBundledAPOCIOLoaderPopulationMatchesInventory(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))

	var realIO []string
	for _, name := range apoc.PluginFunctionNames() {
		if !strings.HasPrefix(name, "apoc.load.") {
			continue
		}
		info, ok := apoc.GetInfo(name)
		require.True(t, ok, name)
		signature := info.Handler.Signature()
		require.NotNil(t, signature, name)
		if signature.NumIn() > 0 && signature.In(0) == pluginContextType {
			realIO = append(realIO, name)
		}
	}

	bound := make([]string, 0, len(realIO))
	for name := range apocPluginIOLoaders() {
		bound = append(bound, name)
	}
	require.ElementsMatch(t, realIO, bound,
		"the policy checks must bind exactly the plugin's real I/O apoc.load exports")
}

func TestBundledAPOCScalarLoaderRequiresCapability(t *testing.T) {
	withPluginFunctions(t, apocPluginIOLoaders())

	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `["policy-probe"]`)
	}))
	t.Cleanup(server.Close)

	handler, ok := pluginFunctionHandler("apoc.load.jsonStream")
	require.True(t, ok)

	_, err := callPluginHandlerContext(context.Background(), handler, []interface{}{server.URL})
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)
	require.Zero(t, hits.Load())
}
