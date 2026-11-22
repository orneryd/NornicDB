package closeoutl3

// Closeout lane L3, P-N152-8 (Plane N152-8 / NOR-182), UI half: runs the UI's
// actual map walkers from ui/src/utils/api.ts under node on the worker (type
// annotations stripped by node itself), on a map that carries an own key
// "__proto__", the shape a Bolt map with that key decodes to. On the original
// candidate, plain assignments dropped this key. The f44 tip now uses
// copyConvertedEntries/Object.fromEntries, which should preserve it; this test
// adaptation extracts that helper so the same input and assertion run on the
// current implementation. The control applies the old assignment bypass when
// present, or reruns the current safe copier on the tip.
// The only stub is isInt (the neo4j-driver import is not installed in the
// repository; the input holds no driver Integer). Test-only.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	// Node older than 22.6 cannot strip types: a test-side stripper removes the
	// type syntax only (annotations, casts, generics); no statement changes.
	stripped := closeoutL3RCStripTypes(harness)
	jsFile := filepath.Join(dir, "walkers_"+label+".js")
	if err := os.WriteFile(jsFile, []byte(stripped), 0o644); err != nil {
		t.Fatalf("write stripped harness: %v", err)
	}
	var out []byte
	var runErr error
	for _, args := range [][]string{{"--experimental-strip-types", file}, {jsFile}} {
		out, runErr = exec.Command(node, args...).CombinedOutput()
		t.Logf("CLOSEOUT_L3_RC_UI %s node %v exit=%v output=%s", label, args[:len(args)-1], runErr, strings.TrimSpace(string(out)))
		if runErr == nil {
			break
		}
	}
	if runErr != nil {
		t.Logf("CLOSEOUT_L3_RC_UI stripped code:\n%s", stripped)
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
	copyHelper := closeoutL3RCSlice(t, src, "function copyConvertedEntries(", "\n// neo4jValueToPlain")
	bolt := closeoutL3RCSlice(t, src, "function neo4jValueToPlain(", "\ninterface TxGraphNode")
	tx := closeoutL3RCSlice(t, src, "function isTxEntityMeta(", "\nfunction boltErrorCode(")
	if !strings.Contains(copyHelper, "Object.fromEntries(entries)") {
		t.Fatalf("PRECONDITION: expected the current safe entry copier in api.ts")
	}
	return copyHelper + "\n" + bolt + "\n" + tx
}

// The real walkers must preserve the input's own key __proto__.
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

// Control: the current safe copier or a test-side own-property patch keeps the
// key on both paths.
func TestCloseoutL3_RC_N152_8_UIWalkersOwnPropertyBypass(t *testing.T) {
	code := closeoutL3RCWalkerCode(t)
	replaced := false
	if strings.Contains(code, closeoutL3RCBoltAssign) {
		code = strings.Replace(code, closeoutL3RCBoltAssign, closeoutL3RCBoltOwn, 1)
		replaced = true
	}
	if strings.Contains(code, closeoutL3RCTxAssign) {
		code = strings.Replace(code, closeoutL3RCTxAssign, closeoutL3RCTxOwn, 1)
		replaced = true
	}
	if !replaced {
		// The release tip already uses Object.fromEntries in the current copier.
		// Run the same input/assertion against that implementation as the control.
		if !strings.Contains(code, "Object.fromEntries(entries)") {
			t.Fatalf("PRECONDITION: neither legacy assignments nor the safe copier were found")
		}
		t.Log("TIP ADAPTATION: safe own-property copier is already present; bypass control exercises the same current implementation")
	}
	got := closeoutL3RCRunWalkers(t, code, "bypass")
	t.Logf("CLOSEOUT_L3_RC_UI bypass node=%s src=%v bolt=%v tx=%v", got.Node, got.Src, got.BoltKeys, got.TxKeys)
	for _, keys := range [][]string{got.BoltKeys, got.TxKeys} {
		if !closeoutL3RCHas(keys, "__proto__") || !closeoutL3RCHas(keys, "ordinary") {
			t.Errorf("BYPASS: own-property copy must keep both keys: %v", keys)
		}
	}
}

var (
	closeoutL3RCTypeAtom   = `(?:Array<Record<string, unknown>>|Record<string, unknown>|Record<string, any>|unknown\[\]|any\[\]|unknown|boolean|string|any|TxGraph|TxEntityMeta|Integer|undefined)`
	closeoutL3RCTypeExpr   = closeoutL3RCTypeAtom + `(?:\s*\|\s*` + closeoutL3RCTypeAtom + `)*`
	closeoutL3RCAsLiteral  = regexp.MustCompile(`\s+as\s+\{[^{}]*\}`)
	closeoutL3RCAsCast     = regexp.MustCompile(`\s+as\s+` + closeoutL3RCTypeExpr + `(?:[^\w\[<]|$)`)
	closeoutL3RCGuard      = regexp.MustCompile(`\)\s*:\s*\w+\s+is\s+\w+\s*\{`)
	closeoutL3RCGeneric    = regexp.MustCompile(`new Map<[^>()]*>\(`)
	closeoutL3RCAnnotation = regexp.MustCompile(`(\w|\))\??\s*:\s*` + closeoutL3RCTypeExpr + `(\s*[,)=;{]|\s*=>|\s*$)`)
)

// closeoutL3RCStripTypes removes the TypeScript type syntax used by the
// extracted walkers. Object-literal values (`key: value`) are untouched
// because their values are never one of the type atoms.
func closeoutL3RCStripTypes(code string) string {
	code = closeoutL3RCAsLiteral.ReplaceAllString(code, "")
	code = closeoutL3RCAsCast.ReplaceAllStringFunc(code, func(m string) string {
		last := m[len(m)-1:]
		if regexp.MustCompile(`[^\w\[<]`).MatchString(last) {
			return last
		}
		return ""
	})
	code = closeoutL3RCGuard.ReplaceAllString(code, ") {")
	code = closeoutL3RCGeneric.ReplaceAllString(code, "new Map(")
	for i := 0; i < 3; i++ {
		code = closeoutL3RCAnnotation.ReplaceAllString(code, "$1$2")
	}
	return code
}

// The stripper itself, on the type forms the walkers use (no node needed).
func TestCloseoutL3_RC_N152_8_StripperControl(t *testing.T) {
	cases := map[string]string{
		"function f(v: unknown): unknown {":                   "function f(v) {",
		"const out: Record<string, unknown> = {};":            "const out = {};",
		"const obj = v as Record<string, unknown>;":           "const obj = v;",
		"(m as Record<string, unknown>).type":                 "(m).type",
		"function g(m: unknown): m is TxEntityMeta {":         "function g(m) {",
		"const nodes = new Map<string, TxGraphNode>();":       "const nodes = new Map();",
		"  meta: unknown[] | undefined,":                      "  meta,",
		"const toEntity = (\n  p: Record<string, unknown>,\n  m: TxEntityMeta,\n): Record<string, unknown> => {": "const toEntity = (\n  p,\n  m,\n) => {",
		"return { identity: m.id, type: rel?.type ?? \"\" };": "return { identity: m.id, type: rel?.type ?? \"\" };",
		"const node = v as {\n  identity: unknown;\n  labels?: string[];\n};": "const node = v;",
		"const isInt = (_v: unknown): boolean => false;":      "const isInt = (_v) => false;",
	}
	for in, want := range cases {
		if got := closeoutL3RCStripTypes(in); got != want {
			t.Errorf("strip %q = %q, want %q", in, got, want)
		}
	}
}
