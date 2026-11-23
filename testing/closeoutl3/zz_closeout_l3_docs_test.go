// Package closeoutl3 holds closeout lane L3's repository-reading checks
// (documentation and UI lock items). Test-only; no product code.
package closeoutl3

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func closeoutL3RepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above " + file)
		}
		dir = parent
	}
}

func closeoutL3Read(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(closeoutL3RepoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}

var closeoutL3Space = regexp.MustCompile(`\s+`)

// closeoutL3Flat joins wrapped Markdown lines so a sentence is one string.
func closeoutL3Flat(s string) string {
	return closeoutL3Space.ReplaceAllString(s, " ")
}

var closeoutL3Heading = regexp.MustCompile(`\n#{1,6} `)

// closeoutL3Section returns the text after heading up to the next heading of
// the same or a higher level.
func closeoutL3Section(t *testing.T, doc, heading string) string {
	t.Helper()
	start := strings.Index(doc, "\n"+heading+"\n")
	if start < 0 {
		t.Fatalf("heading %q not found", heading)
	}
	level := strings.Count(strings.SplitN(heading, " ", 2)[0], "#")
	rest := doc[start+1+len(heading):]
	offset := 0
	for {
		loc := closeoutL3Heading.FindStringIndex(rest[offset:])
		if loc == nil {
			return rest
		}
		hashes := loc[1] - loc[0] - 2 // "\n" + hashes + " "
		if hashes <= level {
			return rest[:offset+loc[0]]
		}
		offset += loc[1]
	}
}

// ---------------------------------------------------------------- P-N152-109

const closeoutL3CDCGuide = "docs/user-guides/change-data-capture.md"

func TestCloseoutL3_N152_109_Control(t *testing.T) {
	doc := closeoutL3Read(t, closeoutL3CDCGuide)
	for _, h := range []string{"## Exactness", "## Errors", "### Earliest marker and retention"} {
		if !strings.Contains(doc, "\n"+h+"\n") {
			t.Fatalf("guide lacks heading %q", h)
		}
	}
	marker := closeoutL3Flat(closeoutL3Section(t, doc, "### Earliest marker and retention"))
	t.Logf("CLOSEOUT_L3_CDC marker_section=%q", marker)
	if !strings.Contains(marker, "marker") {
		t.Fatal("marker section does not describe the earliest marker")
	}
}

// Exactness must not say that managed start-up applies journaled commits
// before the capture options are loaded and records no event (the current
// Open loads options before replay; TestRecoveredCommitPublishesItsChangeEvent
// and TestRecoveredCommitUsesCatalogOptionReplayOrder in pkg/nornicdb show
// the recovered commit's event exactly once).
func TestCloseoutL3_N152_109_ExactnessManagedRecovery(t *testing.T) {
	exact := closeoutL3Flat(closeoutL3Section(t, closeoutL3Read(t, closeoutL3CDCGuide), "## Exactness"))
	stale := []string{
		"before the databases' capture options are loaded",
		"no changes are recorded for such a commit",
		"nothing in the feed shows that one is missing",
		"apart from the commits applied from the journal described above",
	}
	for _, s := range stale {
		found := strings.Contains(exact, s)
		t.Logf("CLOSEOUT_L3_CDC exactness stale=%q present=%v", s, found)
		if found {
			t.Errorf("Exactness still says %q", s)
		}
	}
}

// The Errors table row for identifiers older than db.cdc.earliest() must
// carry the reserved earliest-marker exception documented below it.
func TestCloseoutL3_N152_109_ErrorsTableEarliestMarker(t *testing.T) {
	errs := closeoutL3Section(t, closeoutL3Read(t, closeoutL3CDCGuide), "## Errors")
	var row string
	for _, line := range strings.Split(errs, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "|") && strings.Contains(line, "older than") {
			row = line
		}
	}
	t.Logf("CLOSEOUT_L3_CDC errors_row=%q", row)
	if row == "" {
		// The row is gone: nothing describes older identifiers as invalid
		// without the exception any more.
		return
	}
	lower := strings.ToLower(row)
	if !strings.Contains(lower, "marker") && !strings.Contains(lower, "except") {
		t.Errorf("Errors row calls identifiers older than earliest() invalid without the earliest-marker exception: %s", row)
	}
}

// ---------------------------------------------------------------- P-N151-33

const closeoutL3BoltDoc = "docs/api-reference/bolt-protocol.md"

func TestCloseoutL3_N151_33_Control(t *testing.T) {
	doc := closeoutL3Read(t, closeoutL3BoltDoc)
	if !strings.Contains(doc, "Bolt") {
		t.Fatal("bolt reference unreadable")
	}
	src := closeoutL3Read(t, "pkg/bolt/server.go")
	if !strings.Contains(src, "BoltV5_0 = 0x0500") {
		t.Fatal("server.go no longer declares BoltV5_0; re-check the item")
	}
}

// The reference must not claim that native temporal/spatial values are not
// encoded as Bolt structures or that time is sent as Unix milliseconds
// (pkg/bolt encodes DateTime/Date/Time/Duration/Point structures; see
// TestCloseoutL3_N151_33_ServerBehaviour in pkg/bolt).
func TestCloseoutL3_N151_33_ReferenceTemporalClaim(t *testing.T) {
	doc := closeoutL3Flat(closeoutL3Read(t, closeoutL3BoltDoc))
	for _, s := range []string{
		"not currently encoded as Bolt temporal structs",
		"encoded as an integer (Unix millis)",
	} {
		found := strings.Contains(doc, s)
		t.Logf("CLOSEOUT_L3_BOLTDOC stale=%q present=%v", s, found)
		if found {
			t.Errorf("bolt-protocol.md still says %q", s)
		}
	}
}

// The pkg/bolt package introduction lists the accepted Bolt versions; 5.0 is
// accepted (selectBoltVersion) and must be listed.
func TestCloseoutL3_N151_33_PackageIntroVersions(t *testing.T) {
	src := closeoutL3Read(t, "pkg/bolt/server.go")
	end := strings.Index(src, "\npackage bolt")
	if end < 0 {
		t.Fatal("package clause not found")
	}
	intro := src[:end]
	var versions string
	for _, line := range strings.Split(intro, "\n") {
		if strings.Contains(line, "Bolt 4") || strings.Contains(line, "Bolt 5") {
			versions += strings.TrimSpace(line) + " | "
		}
	}
	t.Logf("CLOSEOUT_L3_BOLTDOC intro_versions=%q", versions)
	if !strings.Contains(intro, "5.0") && !strings.Contains(intro, "5.x") && !strings.Contains(intro, "Bolt 5") {
		t.Errorf("package introduction lists no Bolt 5 version: %s", versions)
	}
}

// ---------------------------------------------------------------- P-N152-8

// The UI's locked neo4j-driver packages: the item's evidence isolates the
// __proto__ key loss in driver 6.2.0's decoder (the fix candidate locks 6.3.2).
func TestCloseoutL3_N152_8_UILockedDriver(t *testing.T) {
	var lock struct {
		Packages map[string]struct {
			Version string `json:"version"`
		} `json:"packages"`
	}
	if err := json.Unmarshal([]byte(closeoutL3Read(t, "ui/package-lock.json")), &lock); err != nil {
		t.Fatalf("parse lock: %v", err)
	}
	for _, name := range []string{"neo4j-driver", "neo4j-driver-core", "neo4j-driver-bolt-connection"} {
		v := lock.Packages["node_modules/"+name].Version
		t.Logf("CLOSEOUT_L3_UI locked %s=%s", name, v)
		if v == "" {
			t.Fatalf("%s not locked", name)
		}
		if v == "6.2.0" {
			t.Errorf("%s is locked at 6.2.0, the decoder the item shows dropping __proto__", name)
		}
	}
}

// The UI's recursive map walkers copy keys with out[key] = ... onto a plain
// object literal; in JavaScript that assignment with key "__proto__" sets the
// prototype instead of an own property, so the key disappears.
func TestCloseoutL3_N152_8_UIMapWalkersAssignByKey(t *testing.T) {
	src := closeoutL3Read(t, "ui/src/utils/api.ts")
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`const out: Record<string, unknown> = \{\};\s*for \(const \[k, val\] of Object\.entries\(obj\)\) \{\s*out\[k\] = neo4jValueToPlain\(val\);`),
		regexp.MustCompile(`const out: Record<string, unknown> = \{\};\s*for \(const \[key, nested\] of Object\.entries\(value\)\) \{\s*out\[key\] = walk\(nested\);`),
	}
	names := []string{"bolt neo4jValueToPlain", "http tx walker"}
	for i, re := range patterns {
		loc := re.FindStringIndex(src)
		line := 0
		if loc != nil {
			line = strings.Count(src[:loc[0]], "\n") + 1
		}
		t.Logf("CLOSEOUT_L3_UI walker=%q plain_assignment_present=%v line=%d", names[i], loc != nil, line)
		if loc != nil {
			t.Errorf("ui/src/utils/api.ts:%d %s copies map keys by plain assignment onto {}; key __proto__ is lost", line, names[i])
		}
	}
}

// Runs the JavaScript semantics the walkers rely on, when node is on the
// worker: plain assignment of "__proto__" onto {} keeps no own key, while the
// JSON-decoded input has it. The defineProperty copy is the control.
func TestCloseoutL3_N152_8_NodeAssignmentSemantics(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatalf("NODE_UNAVAILABLE on this worker: %v", err)
	}
	script := strings.Join([]string{
		`const src = JSON.parse('{"__proto__":7,"ordinary":13}');`,
		`const plain = {}; for (const [k, v] of Object.entries(src)) { plain[k] = v; }`,
		`const own = {}; for (const [k, v] of Object.entries(src)) { Object.defineProperty(own, k, {value: v, enumerable: true, writable: true, configurable: true}); }`,
		`console.log(JSON.stringify({src: Object.keys(src), plain: Object.keys(plain), own: Object.keys(own)}));`,
	}, "\n")
	out, err := exec.Command(node, "-e", script).CombinedOutput()
	t.Logf("CLOSEOUT_L3_UI node=%s output=%s", node, strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	var got struct {
		Src   []string `json:"src"`
		Plain []string `json:"plain"`
		Own   []string `json:"own"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Src) != 2 || len(got.Own) != 2 {
		t.Fatalf("control failed: %+v", got)
	}
	if len(got.Plain) != 2 {
		t.Errorf("plain-assignment copy keeps %v of %v", got.Plain, got.Src)
	}
}
