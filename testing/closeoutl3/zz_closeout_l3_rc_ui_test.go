package closeoutl3

// Closeout lane L3, root-cause proof for P-N152-8 (Plane N152-8 / NOR-182), UI
// half: runs the UI's ACTUAL map walkers from ui/src/utils/api.ts under node
// on the worker (type annotations stripped by node itself), on a map that
// carries an own key "__proto__", the shape a Bolt map with that key decodes
// to. The real walkers drop the key (ui/src/utils/api.ts:493 Bolt path,
// :641 HTTP tx path). The bypass runs the same extracted code with only those
// two assignments replaced by an own-property definition and keeps the key.
// The only stub is isInt (the neo4j-driver import is not installed in the
// repository; the input holds no driver Integer). Test-only.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	closeoutL3RCBoltAssign = "out[k] = neo4jValueToPlain(val);"
	closeoutL3RCBoltOwn    = "Object.defineProperty(out, k, {value: neo4jValueToPlain(val), enumerable: true, writable: true, configurable: true});"
	closeoutL3RCTxAssign   = "out[key] = walk(nested);"
	closeoutL3RCTxOwn      = "Object.defineProperty(out, key, {value: walk(nested), enumerable: true, writable: true, configurable: true});"
)

// closeoutL3RCSlice returns src from the first occurrence of start up to (not
// including) the first occurrence of end after it.
func closeoutL3RCSlice(t *testing.T, src, start, end string) string {
	t.Helper()
	i := strings.Index(src, start)
	if i < 0 {
		t.Fatalf("PRECONDITION: %q not found in api.ts", start)
	}
	j := strings.Index(src[i:], end)
	if j < 0 {
		t.Fatalf("PRECONDITION: %q not found after %q in api.ts", end, start)
	}
	return src[i : i+j]
}

type closeoutL3RCUIResult struct {
	Node     string   `json:"node"`
	Src      []string `json:"src"`
	BoltKeys []string `json:"boltKeys"`
	TxKeys   []string `json:"txKeys"`
}

func closeoutL3RCRunWalkers(t *testing.T, code, label string) closeoutL3RCUIResult {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatalf("NODE_UNAVAILABLE on this worker: %v", err)
	}
	harness := strings.Join([]string{
		"const isInt = (_v: unknown): boolean => false;",
		code,
		`const src = JSON.parse('{"__proto__":7,"ordinary":13}');`,
		`const bolt = neo4jValueToPlain({ m: src }) as Record<string, any>;`,
		`const tx = txRowToBoltShape([src], [null, null], undefined) as any[];`,
		`console.log(JSON.stringify({ node: process.version, src: Object.keys(src), boltKeys: Object.keys(bolt.m), txKeys: Object.keys(tx[0]) }));`,
	}, "\n")
	dir := t.TempDir()
	file := filepath.Join(dir, "walkers_"+label+".ts")
	if err := os.WriteFile(file, []byte(harness), 0o644); err != nil {
		t.Fatalf("write harness: %v", err)
	}
	var out []byte
	var runErr error
	for _, args := range [][]string{{"--experimental-strip-types", file}, {file}} {
		out, runErr = exec.Command(node, args...).CombinedOutput()
		t.Logf("CLOSEOUT_L3_RC_UI %s node %v exit=%v output=%s", label, args[:len(args)-1], runErr, strings.TrimSpace(string(out)))
		if runErr == nil {
			break
		}
	}
	if runErr != nil {
		version, _ := exec.Command(node, "--version").CombinedOutput()
		t.Fatalf("PRECONDITION: node %s could not run the extracted TypeScript: %v", strings.TrimSpace(string(version)), runErr)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var got closeoutL3RCUIResult
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("decode %q: %v", lines[len(lines)-1], err)
	}
	return got
}

func closeoutL3RCHas(keys []string, k string) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

func closeoutL3RCWalkerCode(t *testing.T) string {
	t.Helper()
	src := closeoutL3Read(t, "ui/src/utils/api.ts")
	bolt := closeoutL3RCSlice(t, src, "function neo4jValueToPlain(", "\ninterface TxGraphNode")
	tx := closeoutL3RCSlice(t, src, "function isTxEntityMeta(", "\nfunction boltErrorCode(")
	for _, s := range []struct{ code, needle string }{{bolt, closeoutL3RCBoltAssign}, {tx, closeoutL3RCTxAssign}} {
		if strings.Count(s.code, s.needle) != 1 {
			t.Fatalf("PRECONDITION: expected exactly one %q in the extracted walker", s.needle)
		}
	}
	return bolt + "\n" + tx
}

// The real walkers: the input has own key __proto__, the walker output does not.
func TestCloseoutL3_RC_N152_8_UIWalkersDropProtoKey(t *testing.T) {
	got := closeoutL3RCRunWalkers(t, closeoutL3RCWalkerCode(t), "real")
	t.Logf("CLOSEOUT_L3_RC_UI real node=%s src=%v bolt=%v tx=%v", got.Node, got.Src, got.BoltKeys, got.TxKeys)
	if !closeoutL3RCHas(got.Src, "__proto__") || !closeoutL3RCHas(got.Src, "ordinary") {
		t.Fatalf("PRECONDITION: decoded input must hold own keys __proto__ and ordinary: %v", got.Src)
	}
	if !closeoutL3RCHas(got.BoltKeys, "ordinary") || !closeoutL3RCHas(got.TxKeys, "ordinary") {
		t.Fatalf("control: ordinary key must survive both walkers: bolt=%v tx=%v", got.BoltKeys, got.TxKeys)
	}
	if !closeoutL3RCHas(got.BoltKeys, "__proto__") {
		t.Errorf("DEFECT: Bolt walker neo4jValueToPlain (ui/src/utils/api.ts:493) dropped key __proto__: %v", got.BoltKeys)
	}
	if !closeoutL3RCHas(got.TxKeys, "__proto__") {
		t.Errorf("DEFECT: HTTP tx walker (ui/src/utils/api.ts:641) dropped key __proto__: %v", got.TxKeys)
	}
}

// Bypass: the same extracted code with only the two key assignments replaced
// by own-property definitions keeps the key on both paths.
func TestCloseoutL3_RC_N152_8_UIWalkersOwnPropertyBypass(t *testing.T) {
	code := closeoutL3RCWalkerCode(t)
	code = strings.Replace(code, closeoutL3RCBoltAssign, closeoutL3RCBoltOwn, 1)
	code = strings.Replace(code, closeoutL3RCTxAssign, closeoutL3RCTxOwn, 1)
	got := closeoutL3RCRunWalkers(t, code, "bypass")
	t.Logf("CLOSEOUT_L3_RC_UI bypass node=%s src=%v bolt=%v tx=%v", got.Node, got.Src, got.BoltKeys, got.TxKeys)
	for _, keys := range [][]string{got.BoltKeys, got.TxKeys} {
		if !closeoutL3RCHas(keys, "__proto__") || !closeoutL3RCHas(keys, "ordinary") {
			t.Errorf("BYPASS: own-property copy must keep both keys: %v", keys)
		}
	}
}
