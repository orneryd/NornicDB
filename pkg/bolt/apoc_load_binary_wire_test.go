package bolt

// Wire qualification for the canonical APOC byte loader over the Bolt
// protocol.
//
// apoc/load.Binary is the one handler the bundled plugin registers for both
// apoc.load.binary and apoc.load.stream (apoc/apoc.go), so these tests publish
// that actual admitted handler through the production plugin lookup seam
// (cypher.PluginFunctionLookup) and speak Bolt to a real integration server.
// The RECORD message is decoded from the socket, so what they assert is the
// serialized result: Bolt has no byte-array list type, and a Go []byte value is
// transmitted as the PackStream byte array, whose payload includes bytes >=
// 0x80 without any signed/unsigned reinterpretation.
//
// The denied default is qualified on the same public path: with APOC file and
// remote access at their defaults, a test-owned import file is refused and a
// controlled HTTP URL is refused without the fixture ever being contacted.
//
// Both names, and both argument forms (source alone and source plus the
// optional config map), are covered. The I/O policy is the executor's: the Bolt
// integration server builds its executor when it starts, which is why the
// environment is in place before the server is created.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	apocload "github.com/orneryd/nornicdb/apoc/load"
	"github.com/orneryd/nornicdb/apoc/registry"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// boltLoadBinarySource is the test-owned import file, relative to the
	// configured import root.
	boltLoadBinarySource = "wire.bin"
	// boltLoadBinaryDatabase names the namespace this fixture stores nothing
	// in; it exists so the server has an ordinary engine like its other tests.
	boltLoadBinaryDatabase = "apoc_load_wire"
	// boltLoadBinaryDenialCode is the failure a denied APOC load reports.
	boltLoadBinaryDenialCode = "Neo.ClientError.Statement.SyntaxError"
)

// boltLoadBinaryPayload mixes zero, the sign boundary (0x7f/0x80), a full value
// byte and ASCII so every byte of the PackStream byte array has to survive.
var boltLoadBinaryPayload = []byte{0x00, 0x01, 0x7f, 0x80, 0xff, 'n', 'd', 'b'}

// boltLoadBinaryNames are the two names the bundled plugin registers against
// the canonical byte loader.
var boltLoadBinaryNames = []string{"apoc.load.binary", "apoc.load.stream"}

// withBundledLoadBinaryHandler publishes the handler the bundled plugin
// registers - apoc/load.Binary, admitted with registry.Compile - behind the
// production plugin lookup seam for the duration of the test. The previous
// lookup is restored by the test's cleanup.
func withBundledLoadBinaryHandler(t *testing.T) {
	t.Helper()
	handler, err := registry.Compile(apocload.Binary, registry.CanonicalReads)
	require.NoError(t, err)
	previous := cypher.PluginFunctionLookup
	cypher.PluginFunctionLookup = func(name string) (*cypher.PluginFunctionHandler, bool) {
		for _, registered := range boltLoadBinaryNames {
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

// boltLoadBinaryArguments are the two argument forms both names accept.
var boltLoadBinaryArguments = []struct {
	name   string
	call   string
	params map[string]any
}{
	{name: "source-alone", call: "$source", params: map[string]any{"source": boltLoadBinarySource}},
	{
		name:   "optional-map",
		call:   "$source, $config",
		params: map[string]any{"source": boltLoadBinarySource, "config": map[string]any{"format": "raw"}},
	},
}

// boltLoadBinaryFailure sends the statement on a live connection and returns
// the FAILURE message's code and message.
func boltLoadBinaryFailure(t *testing.T, conn net.Conn, statement string, params map[string]any) (string, string) {
	t.Helper()
	requireNoError(t, SendRun(t, conn, statement, params, nil))
	code, message, err := AssertFailure(t, conn)
	requireNoError(t, err)
	return code, message
}

// boltLoadBinaryServer starts the integration server over an owned memory
// engine and returns its port.
func boltLoadBinaryServer(t *testing.T) int {
	t.Helper()
	store := storage.NewNamespacedEngine(
		fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		boltLoadBinaryDatabase,
	)
	t.Cleanup(func() { _ = store.Close() })
	_, port := startBoltIntegrationServer(t, store, boltIntegrationAutocommit)
	return port
}

// TestBoltLoadBinaryWireTransmitsBytes qualifies the allowed path: with the
// test-owned import file explicitly allowed, both registered names transmit the
// file's exact bytes for both argument forms.
func TestBoltLoadBinaryWireTransmitsBytes(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, boltLoadBinarySource), boltLoadBinaryPayload, 0o600))
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "true")
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", root)
	port := boltLoadBinaryServer(t)
	withBundledLoadBinaryHandler(t)
	conn := openBoltTestConn(t, port)

	for _, name := range boltLoadBinaryNames {
		name := name
		t.Run(name, func(t *testing.T) {
			for _, argument := range boltLoadBinaryArguments {
				argument := argument
				t.Run(argument.name, func(t *testing.T) {
					records := runBoltQueryWithParams(t, conn,
						"RETURN "+name+"("+argument.call+") AS value", argument.params)
					require.Len(t, records, 1)
					require.Len(t, records[0], 1)
					data, ok := records[0][0].([]byte)
					require.True(t, ok, "the serialized result must be the Loader's bytes, got %T", records[0][0])
					require.Equal(t, boltLoadBinaryPayload, data,
						"the record must carry the source's exact bytes in wire order")
				})
			}
		})
	}
}

// TestBoltLoadBinaryWireDeniesBeforeAccess keeps the denied default on the same
// public path: a test-owned import file is refused, and a controlled HTTP URL
// is refused without a single request reaching the fixture.
func TestBoltLoadBinaryWireDeniesBeforeAccess(t *testing.T) {
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "")
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_REMOTE_URL_ACCESS", "")
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", "")
	t.Setenv("NORNICDB_APOC_SECURITY_REMOTE_URL_ALLOWLIST", "")

	root := t.TempDir()
	importPath := filepath.Join(root, boltLoadBinarySource)
	require.NoError(t, os.WriteFile(importPath, boltLoadBinaryPayload, 0o600))
	var contacted atomic.Int64
	controlled := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		contacted.Add(1)
		_, _ = writer.Write(boltLoadBinaryPayload)
	}))
	t.Cleanup(controlled.Close)

	port := boltLoadBinaryServer(t)
	withBundledLoadBinaryHandler(t)

	denials := []struct {
		name   string
		source string
		reason string
	}{
		{name: "test-owned-file", source: importPath, reason: "local APOC import file access is disabled"},
		{name: "controlled-http", source: controlled.URL + "/" + boltLoadBinarySource, reason: "remote APOC URL access is disabled"},
	}
	for _, name := range boltLoadBinaryNames {
		name := name
		for _, argument := range boltLoadBinaryArguments {
			argument := argument
			for _, denial := range denials {
				denial := denial
				t.Run(name+"/"+argument.name+"/"+denial.name, func(t *testing.T) {
					params := map[string]any{"source": denial.source}
					if argument.name == "optional-map" {
						params["config"] = map[string]any{}
					}
					// A FAILURE leaves the connection in the failed state, so
					// each denial request gets its own connection.
					conn := openBoltTestConn(t, port)
					code, message := boltLoadBinaryFailure(t, conn,
						"RETURN "+name+"("+argument.call+") AS value", params)
					require.Equal(t, boltLoadBinaryDenialCode, code)
					require.Equal(t, denial.reason, message)
				})
			}
		}
	}
	require.Zero(t, contacted.Load(), "a denied APOC load must not contact the controlled fixture")
}
