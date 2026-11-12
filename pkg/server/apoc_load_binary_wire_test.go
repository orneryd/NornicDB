package server

// Wire qualification for the canonical APOC byte loader over the public Neo4j
// HTTP transaction endpoint.
//
// apoc/load.Binary is the one handler the bundled plugin registers for both
// apoc.load.binary and apoc.load.stream (apoc/apoc.go), so these tests publish
// that actual admitted handler through the production plugin lookup seam
// (cypher.PluginFunctionLookup) and drive POST /db/nornic/tx/commit through the
// real router. What they assert is the serialized HTTP row, not a Go value read
// back from an encoder helper: Neo4j HTTP writes a byte value as the list of
// signed Java byte values, so a payload containing bytes >= 0x80 distinguishes a
// real byte serialization from a list of unsigned integers.
//
// The denied default is qualified too: with APOC file and remote access at
// their defaults, a test-owned import file is refused and a controlled HTTP URL
// is refused without the fixture ever being contacted.
//
// Both names, and both argument forms (source alone and source plus the
// optional config map), are covered. The I/O policy is the executor's: it is
// built from the APOC environment when the HTTP executor is created, which is
// why each policy variant starts its own server with the environment in place,
// and why the handler is published after the database runtime is open - opening
// it publishes the production lookup (pkg/nornicdb) over the seam.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	apocload "github.com/orneryd/nornicdb/apoc/load"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

const (
	// httpLoadBinarySource is the test-owned import file, relative to the
	// configured import root.
	httpLoadBinarySource = "wire.bin"
	// httpLoadBinaryEndpoint is the public implicit-transaction endpoint.
	httpLoadBinaryEndpoint = "/db/nornic/tx/commit"
	// httpLoadBinaryDenialCode is the failure a denied APOC load reports.
	httpLoadBinaryDenialCode = "Neo.ClientError.Statement.SyntaxError"
)

// httpLoadBinaryPayload mixes zero, the sign boundary (0x7f/0x80), a full
// value byte and ASCII so a byte that is negative as a Java byte cannot be
// mistaken for an unsigned one.
var httpLoadBinaryPayload = []byte{0x00, 0x01, 0x7f, 0x80, 0xff, 'n', 'd', 'b'}

// httpLoadBinaryNames are the two names the bundled plugin registers against
// the canonical byte loader.
var httpLoadBinaryNames = []string{"apoc.load.binary", "apoc.load.stream"}

// withBundledLoadBinaryHandler publishes the handler the bundled plugin
// registers - apoc/load.Binary, admitted with registry.Compile - behind the
// production plugin lookup seam for the duration of the test. The previous
// lookup is restored by the test's cleanup. Call it after the server runtime is
// open: opening a database publishes its own lookup.
func withBundledLoadBinaryHandler(t *testing.T) {
	t.Helper()
	handler, err := registry.Compile(apocload.Binary, registry.CanonicalReads)
	require.NoError(t, err)
	previous := cypher.PluginFunctionLookup
	cypher.PluginFunctionLookup = func(name string) (*cypher.PluginFunctionHandler, bool) {
		for _, registered := range httpLoadBinaryNames {
			if name == registered {
				return handler, true
			}
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { cypher.PluginFunctionLookup = previous })
}

// postLoadBinaryStatement sends one statement with parameters to the public
// commit endpoint and returns the status, the decoded Neo4j response and the
// exact response body.
func postLoadBinaryStatement(t *testing.T, client memoryHTTPClient, statement string, params map[string]any) (int, TransactionResponse, []byte) {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"statements": []map[string]any{{"statement": statement, "parameters": params}},
	})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, client.base+httpLoadBinaryEndpoint, bytes.NewReader(encoded))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", client.token)
	response, err := client.client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	var decoded TransactionResponse
	require.NoError(t, json.Unmarshal(raw, &decoded), string(raw))
	return response.StatusCode, decoded, raw
}

// loadBinaryRawRow returns the single column's value as the JSON numbers the
// server actually wrote, so a byte conversion cannot be inferred from a Go
// value the test decoded into a convenient type.
func loadBinaryRawRow(t *testing.T, raw []byte) []string {
	t.Helper()
	var envelope struct {
		Results []struct {
			Data []struct {
				Row []json.RawMessage `json:"row"`
			} `json:"data"`
		} `json:"results"`
	}
	require.NoError(t, json.Unmarshal(raw, &envelope), string(raw))
	require.Len(t, envelope.Results, 1, string(raw))
	require.Len(t, envelope.Results[0].Data, 1, string(raw))
	require.Len(t, envelope.Results[0].Data[0].Row, 1, "one returned column")
	var numbers []json.Number
	require.NoError(t, json.Unmarshal(envelope.Results[0].Data[0].Row[0], &numbers), string(raw))
	row := make([]string, len(numbers))
	for index, number := range numbers {
		row[index] = number.String()
	}
	return row
}

// wantLoadBinarySignedBytes is the payload as Neo4j HTTP writes it: signed
// Java byte values, in order.
func wantLoadBinarySignedBytes() []string {
	want := make([]string, len(httpLoadBinaryPayload))
	for index, value := range httpLoadBinaryPayload {
		want[index] = strconv.FormatInt(int64(int8(value)), 10)
	}
	return want
}

// TestHTTPLoadBinaryWireTransmitsBytes qualifies the allowed path: with the
// test-owned import file explicitly allowed, both registered names transmit the
// file's exact bytes for both argument forms.
func TestHTTPLoadBinaryWireTransmitsBytes(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, httpLoadBinarySource), httpLoadBinaryPayload, 0o600))
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", root)
	_, client, _ := startMemoryHTTPServer(t)
	withBundledLoadBinaryHandler(t)

	arguments := []struct {
		name   string
		call   string
		params map[string]any
	}{
		{
			name:   "source-alone",
			call:   "$source",
			params: map[string]any{"source": httpLoadBinarySource},
		},
		{
			name:   "optional-map",
			call:   "$source, $config",
			params: map[string]any{"source": httpLoadBinarySource, "config": map[string]any{"format": "raw"}},
		},
	}
	want := wantLoadBinarySignedBytes()
	for _, name := range httpLoadBinaryNames {
		name := name
		t.Run(name, func(t *testing.T) {
			for _, argument := range arguments {
				argument := argument
				t.Run(argument.name, func(t *testing.T) {
					status, body, raw := postLoadBinaryStatement(t, client,
						"RETURN "+name+"("+argument.call+") AS value", argument.params)
					require.Equal(t, http.StatusOK, status, string(raw))
					require.Empty(t, body.Errors, "%+v", body.Errors)
					require.Len(t, body.Results, 1, string(raw))
					require.Equal(t, []string{"value"}, body.Results[0].Columns)
					require.Len(t, body.Results[0].Data, 1, string(raw))
					require.Equal(t, want, loadBinaryRawRow(t, raw),
						"the committed row must carry the source's exact bytes as signed Java byte values")
				})
			}
		})
	}
}

// TestHTTPLoadBinaryWireDeniesBeforeAccess keeps the denied default on the same
// public call: a test-owned import file is refused, and a controlled HTTP URL
// is refused without a single request reaching the fixture.
func TestHTTPLoadBinaryWireDeniesBeforeAccess(t *testing.T) {
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "")
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_REMOTE_URL_ACCESS", "")
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", "")
	t.Setenv("NORNICDB_APOC_SECURITY_REMOTE_URL_ALLOWLIST", "")

	root := t.TempDir()
	importPath := filepath.Join(root, httpLoadBinarySource)
	require.NoError(t, os.WriteFile(importPath, httpLoadBinaryPayload, 0o600))
	var contacted atomic.Int64
	controlled := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		contacted.Add(1)
		_, _ = writer.Write(httpLoadBinaryPayload)
	}))
	t.Cleanup(controlled.Close)

	_, client, _ := startMemoryHTTPServer(t)
	withBundledLoadBinaryHandler(t)

	denials := []struct {
		name   string
		source string
		reason string
	}{
		{name: "test-owned-file", source: importPath, reason: "local APOC import file access is disabled"},
		{name: "controlled-http", source: controlled.URL + "/" + httpLoadBinarySource, reason: "remote APOC URL access is disabled"},
	}
	arguments := []struct {
		name       string
		call       string
		parameters bool
	}{
		{name: "source-alone", call: "$source"},
		{name: "optional-map", call: "$source, $config", parameters: true},
	}
	for _, name := range httpLoadBinaryNames {
		name := name
		for _, argument := range arguments {
			argument := argument
			for _, denial := range denials {
				denial := denial
				t.Run(name+"/"+argument.name+"/"+denial.name, func(t *testing.T) {
					params := map[string]any{"source": denial.source}
					if argument.parameters {
						params["config"] = map[string]any{}
					}
					status, body, raw := postLoadBinaryStatement(t, client,
						"RETURN "+name+"("+argument.call+") AS value", params)
					require.Equal(t, http.StatusOK, status, string(raw))
					require.Len(t, body.Errors, 1, string(raw))
					require.Equal(t, httpLoadBinaryDenialCode, body.Errors[0].Code, string(raw))
					require.Equal(t, denial.reason, body.Errors[0].Message, string(raw))
					for _, result := range body.Results {
						require.Empty(t, result.Data, "a denied load must not publish a row")
					}
				})
			}
		}
	}
	require.Zero(t, contacted.Load(), "a denied APOC load must not contact the controlled fixture")
}
