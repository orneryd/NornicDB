package observability_test

// Report gap T22 (v1.5.2 verification campaign, test-side only): does
// TestCredentialURLSinkStructure accept a key that is classified both as a
// reviewed outbound sink (testdata/url_sink_inventory.json) and as
// non-egress (testdata/url_sink_non_egress.json)?
//
// The repository test reads its two manifests from testdata/ and walks the
// source from ../... This test makes a working directory at the same depth
// (pkg/zzgapt22/) holding a copy of the manifests, changes one thing per
// case, and runs the unmodified repository test there:
//   unchanged:        the manifests as they are (control, must pass);
//   conflict:         one real reviewed egress key ALSO added to non-egress
//                     (left in the inventory): a conflicting key;
//   reclassified:     the same key moved: removed from the inventory and added
//                     to non-egress (a consistent reclassification);
//   unknown-nonegress: a key that names no real sink added to non-egress
//                     (control: the test reports it as stale).
// Diagnostic: logs GAPT22 per case whether the repository test passed.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func gapT22Load(t *testing.T, path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func gapT22Write(t *testing.T, path string, m map[string]string) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGapT22ConflictingKey(t *testing.T) {
	inventory := gapT22Load(t, filepath.Join("testdata", "url_sink_inventory.json"))
	nonEgress := gapT22Load(t, filepath.Join("testdata", "url_sink_non_egress.json"))
	keys := make([]string, 0, len(inventory))
	for k := range inventory {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	key := keys[0]
	t.Logf("GAPT22 manifests: inventory=%d non_egress=%d; conflicting key used: %q (inventory reason %q)", len(inventory), len(nonEgress), key, inventory[key])
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(inv, ne map[string]string)
	}{
		{"unchanged", func(inv, ne map[string]string) {}},
		{"conflict", func(inv, ne map[string]string) { ne[key] = "gap-t22: conflicting classification of a reviewed egress" }},
		{"reclassified", func(inv, ne map[string]string) {
			delete(inv, key)
			ne[key] = "gap-t22: reclassified as non-egress"
		}},
		{"unknown-nonegress", func(inv, ne map[string]string) { ne["pkg/zzgapt22/none.go:Nothing 0000"] = "gap-t22: names no sink" }},
	}
	for _, c := range cases {
		work := filepath.Join(repoRoot, "pkg", "zzgapt22")
		if err := os.MkdirAll(filepath.Join(work, "testdata"), 0o755); err != nil {
			t.Fatal(err)
		}
		inv := map[string]string{}
		for k, v := range inventory {
			inv[k] = v
		}
		ne := map[string]string{}
		for k, v := range nonEgress {
			ne[k] = v
		}
		c.mutate(inv, ne)
		gapT22Write(t, filepath.Join(work, "testdata", "url_sink_inventory.json"), inv)
		gapT22Write(t, filepath.Join(work, "testdata", "url_sink_non_egress.json"), ne)
		if err := os.Chdir(work); err != nil {
			t.Fatal(err)
		}
		passed := t.Run(c.name, TestCredentialURLSinkStructure)
		if err := os.Chdir(cwd); err != nil {
			t.Fatal(err)
		}
		_ = os.RemoveAll(work)
		t.Logf("GAPT22 case=%s key_in_inventory=%t key_in_non_egress=%t repository_test_passed=%t", c.name, inv[key] != "", ne[key] != "", passed)
	}
}
