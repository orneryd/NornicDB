package bolt

// Closeout lane L3, P-N152-8 (NOR-182) decoder half. The UI locks
// neo4j-driver 6.2.0 (ui/package-lock.json); its PackStream decoder is
// neo4j-driver-bolt-connection 6.2.0, lib/packstream/packstream-v1.js
// Unpacker._unpackMapWithSize (value[key] = ... on a plain {}, :567). The two
// packages are vendored byte for byte from the npm tarballs whose sha512 equal
// the lock's integrity values, under testing/closeoutl3/testdata/node_modules.
//
// The test captures the RECORD message this server actually sends for a map
// with key __proto__ (raw Bolt 4.4 client), then decodes exactly those bytes
// under node on the worker with the vendored Unpacker. The bypass decodes the
// same bytes with only the map assignment replaced by an own-property
// definition. Test-only.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const closeoutL3DecoderScript = `
const v1 = require('neo4j-driver-bolt-connection/lib/packstream/packstream-v1');
const ChannelBuffer = require('neo4j-driver-bolt-connection/lib/channel/channel-buf').default;
const pkg = require('neo4j-driver-bolt-connection/package.json');
const [mode, hexArg] = process.argv.slice(-2); // node -e: arguments follow argv[0]
const bytes = Buffer.from(hexArg, 'hex');
const unpacker = new v1.Unpacker(false, false, false);
if (mode === 'bypass') {
  unpacker._unpackMapWithSize = function (size, buffer, hydrateStructure) {
    const value = {};
    for (let i = 0; i < size; i++) {
      const key = this.unpack(buffer, hydrateStructure);
      Object.defineProperty(value, key, { value: this.unpack(buffer, hydrateStructure), enumerable: true, writable: true, configurable: true });
    }
    return value;
  };
}
const fields = unpacker.unpack(new ChannelBuffer(bytes));
const m = fields[0];
const own = (o) => Object.keys(o);
console.log(JSON.stringify({
  node: process.version, driver: pkg.version, mode,
  keys: own(m), nestedKeys: own(m.nested),
  protoOwn: Object.prototype.hasOwnProperty.call(m, '__proto__'),
  prototypeIsObject: Object.getPrototypeOf(m) === Object.prototype,
}));
`

type closeoutL3DecodeResult struct {
	Node              string   `json:"node"`
	Driver            string   `json:"driver"`
	Mode              string   `json:"mode"`
	Keys              []string `json:"keys"`
	NestedKeys        []string `json:"nestedKeys"`
	ProtoOwn          bool     `json:"protoOwn"`
	PrototypeIsObject bool     `json:"prototypeIsObject"`
}

func closeoutL3CaptureRecord(t *testing.T) []byte {
	t.Helper()
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, port := startBoltIntegrationServer(t, storage.NewNamespacedEngine(engine, "proto_keys"), boltIntegrationExplicitTx)
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(20*time.Second)))
	require.NoError(t, PerformHandshake(conn))
	require.NoError(t, SendMessage(conn, BuildHelloMessage(nil)))
	kind, _, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, byte(MsgSuccess), kind, "PRECONDITION: HELLO")
	run := []byte{0xB3, MsgRun}
	run = append(run, encodePackStreamString("RETURN {__proto__: 7, ordinary: 13, nested: {__proto__: 8}} AS value")...)
	run = append(run, 0xA0)
	run = append(run, encodePackStreamMap(map[string]any{"db": "proto_keys"})...)
	require.NoError(t, SendMessage(conn, run))
	kind, data, err := ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, byte(MsgSuccess), kind, "PRECONDITION: RUN %x", data)
	require.NoError(t, SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(-1)})))
	var record []byte
	for i := 0; i < 10; i++ {
		kind, data, err = ReadMessage(conn)
		require.NoError(t, err)
		if kind == 0x71 { // RECORD
			record = append([]byte(nil), data...)
			continue
		}
		require.Equal(t, byte(MsgSuccess), kind, "PRECONDITION: PULL %x", data)
		break
	}
	require.NotEmpty(t, record, "PRECONDITION: the server sent no RECORD")
	t.Logf("CLOSEOUT_L3_RC_DECODER server RECORD fields (hex)=%s", hex.EncodeToString(record))
	require.Contains(t, string(record), "__proto__", "PRECONDITION: the server's bytes carry the key")
	return record
}

func closeoutL3RunDecoder(t *testing.T, mode string, record []byte) closeoutL3DecodeResult {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatalf("NODE_UNAVAILABLE on this worker: %v", err)
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	modules := filepath.Join(filepath.Dir(file), "..", "..", "testing", "closeoutl3", "testdata", "node_modules")
	_, err = os.Stat(filepath.Join(modules, "neo4j-driver-bolt-connection", "lib", "packstream", "packstream-v1.js"))
	require.NoError(t, err, "PRECONDITION: vendored driver present")
	cmd := exec.Command(node, "-e", closeoutL3DecoderScript, mode, hex.EncodeToString(record))
	cmd.Env = append(os.Environ(), "NODE_PATH="+modules)
	out, err := cmd.CombinedOutput()
	t.Logf("CLOSEOUT_L3_RC_DECODER %s exit=%v output=%s", mode, err, strings.TrimSpace(string(out)))
	require.NoError(t, err, "node decode")
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var got closeoutL3DecodeResult
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &got))
	require.Equal(t, "6.2.0", got.Driver, "PRECONDITION: the UI's locked driver version")
	return got
}

func closeoutL3HasKey(keys []string, k string) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

// The locked driver's decoder on the server's actual bytes.
func TestCloseoutL3_RC_N152_8_Driver620DecodesServerRecord(t *testing.T) {
	record := closeoutL3CaptureRecord(t)
	realRes := closeoutL3RunDecoder(t, "real", record)
	t.Logf("CLOSEOUT_L3_RC_DECODER real node=%s driver=%s keys=%v nested=%v protoOwn=%v prototypeIsObject=%v", realRes.Node, realRes.Driver, realRes.Keys, realRes.NestedKeys, realRes.ProtoOwn, realRes.PrototypeIsObject)
	require.True(t, closeoutL3HasKey(realRes.Keys, "ordinary"), "control: ordinary key decoded")
	if !closeoutL3HasKey(realRes.Keys, "__proto__") || !closeoutL3HasKey(realRes.NestedKeys, "__proto__") {
		t.Errorf("DEFECT: neo4j-driver-bolt-connection 6.2.0 Unpacker._unpackMapWithSize (lib/packstream/packstream-v1.js:567) dropped __proto__ from the server's record: keys=%v nested=%v", realRes.Keys, realRes.NestedKeys)
	}
	bypass := closeoutL3RunDecoder(t, "bypass", record)
	t.Logf("CLOSEOUT_L3_RC_DECODER bypass keys=%v nested=%v protoOwn=%v", bypass.Keys, bypass.NestedKeys, bypass.ProtoOwn)
	require.True(t, closeoutL3HasKey(bypass.Keys, "__proto__") && closeoutL3HasKey(bypass.NestedKeys, "__proto__") && closeoutL3HasKey(bypass.Keys, "ordinary"),
		"BYPASS: own-property definition in the same decoder must keep the key: keys=%v nested=%v", bypass.Keys, bypass.NestedKeys)
}
