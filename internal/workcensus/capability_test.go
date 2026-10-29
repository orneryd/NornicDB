package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCapabilitySyntaxPopulation(t *testing.T) {
	c := genericConfig(t)
	c.IncludeInactive = true
	c.Selectors = nil
	for _, node := range []string{"type-assert", "type-switch", "go"} {
		c.Selectors = append(c.Selectors, Selector{Node: node, Kind: node, Reachability: "all"})
	}
	c.Overlay = map[string][]byte{}
	for _, file := range []string{"source.go", "inactive.go", "generated.go"} {
		path := filepath.Join(c.Dir, file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		probe := `func syntaxProbe[T any](v any) { _, ok := v.(T); _ = ok; switch v.(type) { case string: }; go func(){ _ = v.(string) }() }
var syntaxInitializer = func(v any) bool { _, ok := v.(int); return ok }(1)
`
		if file != "source.go" {
			probe = strings.ReplaceAll(probe, "syntaxProbe", "syntaxProbe"+strings.TrimSuffix(file, ".go"))
			probe = strings.ReplaceAll(probe, "syntaxInitializer", "syntaxInitializer"+strings.TrimSuffix(file, ".go"))
		}
		c.Overlay[path] = append(raw, []byte("\n"+probe)...)
	}
	inv := mustExtract(t, c)
	counts := map[string]int{}
	for _, m := range inv.Members {
		counts[m.Kind]++
		if m.Owner == "" || m.OwnerDigest == "" {
			t.Fatal("unowned syntax", m.ID)
		}
	}
	if counts["type-assert"] != 12 || counts["type-switch"] != 3 || counts["go"] != 3 {
		t.Fatal("incomplete capability syntax", counts)
	}
	approval := reviewed(inv)
	if failures := Validate(inv, approval, fixturePolicy); len(failures) > 0 {
		t.Fatal(failures)
	}
	omitted := approval
	omitted.Members = append([]Member(nil), approval.Members[1:]...)
	if !strings.Contains(strings.Join(Validate(inv, omitted, fixturePolicy), "\n"), "UNLISTED "+inv.Members[0].ID) {
		t.Fatal("assertion omission escaped")
	}
}
