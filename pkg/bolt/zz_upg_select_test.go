package bolt

// v152 upgrade lane: store root and fail-closed scenario selection for the upgrade readers
// (TestV152UpgradeRead, TestV152SnapshotRebuild, TestV152TornAuditRead). Research reader, not for merge.
//
//   UPR_ROOT      root of the store tree; default /work/tnrun/v152-upg (the lab tree). Layout:
//                 <root>/<build>/uor/stores/<scenario>/{expected.json,data/,cdc-writer.json?}
//                 <root>/<build>/torn-audit/<name>/data
//   UPR_READ_DIR  scratch area below the root for copies; default read-pin.
//   UPR_ONLY      comma-separated <build>/<scenario> tokens; when set exactly these run.
//   UPR_BUILDS    builds scanned when UPR_ONLY is unset; default b6,v150,v151 (torn reader: b6, the
//                 only older build whose writer produces the oversized audit record).
//
// A reader fails (never skips or continues) when a selected build directory or scenario is missing,
// when the selection selects nothing, or when a selected scenario did not run (a -run filter that
// excludes it).

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func uprEnv(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

type uprCase struct {
	build, name string
	dir         string // the scenario directory
}

func (c uprCase) id() string { return c.build + "/" + c.name }

func uprTokens(name string) []string {
	var out []string
	for _, tok := range strings.Split(os.Getenv(name), ",") {
		if tok = strings.TrimSpace(tok); tok != "" {
			out = append(out, tok)
		}
	}
	return out
}

// uprSelectStores returns the selected <build>/<scenario> stores under <root>/<build>/uor/stores.
// accept filters the scenarios a reader can read (nil: all); a token naming a scenario the reader
// cannot read fails the test.
func uprSelectStores(t *testing.T, accept func(name string) bool) []uprCase {
	t.Helper()
	var out []uprCase
	if only := uprTokens("UPR_ONLY"); len(only) > 0 {
		for _, tok := range only {
			build, name, ok := strings.Cut(tok, "/")
			if !ok || build == "" || name == "" || strings.Contains(name, "/") {
				t.Fatalf("UPR_ONLY token %q is not <build>/<scenario>", tok)
			}
			if accept != nil && !accept(name) {
				t.Fatalf("UPR_ONLY selects %s, which this reader does not read", tok)
			}
			dir := filepath.Join(uprRoot, build, "uor", "stores", name)
			if st, err := os.Stat(dir); err != nil || !st.IsDir() {
				t.Fatalf("selected store %s is missing: %s (%v)", tok, dir, err)
			}
			out = append(out, uprCase{build, name, dir})
		}
		return out
	}
	for _, build := range strings.Split(uprEnv("UPR_BUILDS", "b6,v150,v151"), ",") {
		build = strings.TrimSpace(build)
		storesDir := filepath.Join(uprRoot, build, "uor", "stores")
		entries, err := os.ReadDir(storesDir)
		if err != nil {
			t.Fatalf("store directory of build %s is missing: %v", build, err)
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() && (accept == nil || accept(e.Name())) {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		if len(names) == 0 {
			t.Fatalf("build %s has no store this reader reads in %s", build, storesDir)
		}
		for _, name := range names {
			out = append(out, uprCase{build, name, filepath.Join(storesDir, name)})
		}
	}
	return out
}

// uprRequireRan fails the test when a selected scenario's subtest did not run.
func uprRequireRan(t *testing.T, selected []string, ran map[string]bool) {
	t.Helper()
	var missing []string
	for _, id := range selected {
		if !ran[id] {
			missing = append(missing, id)
		}
	}
	if len(selected) == 0 {
		t.Fatalf("the selection selects no scenario")
	}
	if len(missing) > 0 {
		t.Fatalf("selected scenarios did not run (a -run filter excludes them?): %s", strings.Join(missing, ", "))
	}
}
