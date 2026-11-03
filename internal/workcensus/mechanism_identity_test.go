package workcensus

// Author: #1667 implementation lane (Codex). Identity producer classes are
// qualified independently of any client's current approved manifest.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBindingIdentitySurvivesUnrelatedRemoval(t *testing.T) {
	const source = "package sample\nfunc Root(ctx interface{}){var unrelated int;_ = unrelated;target:=ctx;_ =target}\n"
	c := declarationIdentityConfig(t, true, source)
	c.SourceOnly = true
	base := mustExtract(t, c)
	removed := "var unrelated int;_ = unrelated;"
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "source.go"): []byte(strings.Replace(source, removed, strings.Repeat(" ", len(removed)), 1))}
	actual := mustExtract(t, c)
	ref := func(inv Inventory) string {
		for _, d := range inv.Definitions {
			for _, v := range d.Values {
				if v.Name == "target" {
					return v.Reference
				}
			}
		}
		return ""
	}
	if before, after := ref(base), ref(actual); before == "" || before != after {
		t.Fatal("retained binding identity reassigned", before, after)
	}
}

func TestLexicalOccurrenceIdentitySurvivesRemoval(t *testing.T) {
	dir := t.TempDir()
	const raw = "Observe(ctx);Observe(ctx);"
	path := filepath.Join(dir, "peer.tf")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	c := Config{Dir: dir, Documents: []Document{{Path: "peer.tf", Language: "terraform", Mode: "lexical"}}, Selectors: []Selector{{Kind: "observation", Node: "text", Pattern: `Observe\(ctx\)`, Reachability: "all"}}}
	base := mustExtract(t, c)
	if len(base.Members) != 2 {
		t.Fatal("lexical occurrence population", len(base.Members))
	}
	c.Overlay = map[string][]byte{path: []byte(strings.Repeat(" ", len("Observe(ctx)")) + ";Observe(ctx);")}
	actual := mustExtract(t, c)
	if len(actual.Members) != 1 || base.Members[1].SiteID == "" || actual.Members[0].SiteID != base.Members[1].SiteID {
		t.Fatal("lexical occurrence identity lost after unrelated removal")
	}
}

func TestOverlappingSyntaxOccurrenceIdentity(t *testing.T) {
	c := declarationIdentityConfig(t, true, "package sample\nvar matrix=[][]int{{1}}\nvar _ = matrix[0][0]\n")
	c.SourceOnly = true
	c.Selectors = []Selector{{Kind: "index", Node: "index", Reachability: "all"}}
	actual := mustExtract(t, c)
	if len(actual.Members) != 2 {
		t.Fatal("nested index population", len(actual.Members))
	}
	seen := map[string]bool{}
	for _, m := range actual.Members {
		if m.SiteID == "" || seen[m.SiteID] {
			t.Fatal("overlapping syntax occurrences collide", m.SiteID)
		}
		seen[m.SiteID] = true
	}
	broken := actual
	broken.Members = append([]Member(nil), actual.Members...)
	broken.Members[1].SiteID = broken.Members[0].SiteID
	failures := Validate(broken, reviewed(actual), fixturePolicy)
	found := false
	for _, failure := range failures {
		found = found || strings.HasPrefix(failure, "DUPLICATE ACTUAL SITE ")
	}
	if !found {
		t.Fatal("site collision escaped the actual validator", failures)
	}
}
