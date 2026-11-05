package workcensus

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func genericConfig(t *testing.T) Config {
	t.Helper()
	dir, err := filepath.Abs("testdata/fixture")
	if err != nil {
		t.Fatal(err)
	}
	return Config{Dir: dir, Patterns: []string{"./..."}, Roots: []Root{{Package: "census.fixture", Name: "Execute", Required: true}}, Selectors: []Selector{
		{Kind: "for", Node: "for", Group: "work"}, {Kind: "range", Node: "range", Group: "work"},
		{Kind: "declaration", Node: "declaration", Reachability: "all"},
		{Kind: "split", Node: "call", Packages: []string{"strings"}, Names: []string{"Split"}},
		{Kind: "regex", Node: "call", Packages: []string{"regexp"}, NamePrefixes: []string{"Match"}},
		{Kind: "string-index", Node: "index", OperandTypes: []string{"string"}},
	}}
}

func mustExtract(t *testing.T, c Config) Inventory {
	t.Helper()
	inv, err := Extract(c)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}
func reviewed(inv Inventory) Inventory {
	out := inv
	out.Members = append([]Member(nil), inv.Members...)
	for i := range out.Members {
		out.Members[i].Class = "reviewed"
		out.Members[i].Reason = "fixture review"
		out.Members[i].ReviewOwner = "fixture"
		out.Members[i].Subjects = []string{"fixture semantics"}
	}
	return out
}

var fixturePolicy = Policy{Classes: []string{"reviewed"}, ClassKinds: map[string]string{"reviewed": "review"}, RequireReviewOwner: true, RequireSubjects: true, RequireOwnerDigest: true, Owners: []string{"fixture"}, Subjects: []string{"fixture semantics"}}

func TestGenericCensusClientsAndControls(t *testing.T) {
	c := genericConfig(t)
	positive := mustExtract(t, c)
	approved := reviewed(positive)
	if failures := Validate(positive, approved, fixturePolicy); len(failures) > 0 {
		t.Fatal(failures)
	}
	checks := map[string]bool{}
	for _, m := range positive.Members {
		if m.Reachable && m.Kind == "range" && strings.Contains(m.Owner, "Walk$") {
			checks["generic method closure"] = true
		}
		if m.Reachable && m.Kind == "declaration" && strings.Contains(m.Owner, "registry") && m.Declaration == "closure" {
			checks["registry callback"] = true
		}
		if m.Reachable && m.Kind == "range" && strings.Contains(m.Owner, "Execute$") {
			checks["opaque callback"] = true
		}
		if !m.Reachable && strings.Contains(m.Owner, "arbitraryUntouched") && strings.Contains(m.Signature, "census.fixture/meta.Schema") {
			checks["unused method imported alias"] = true
		}
		if m.Declaration == "var" && strings.Contains(m.Code, "headers.Schema") {
			checks["global initializer"] = true
		}
		if m.Kind == "split" {
			checks["imported call alias"] = true
			if strings.Contains(m.Code, "splitAlias(") {
				checks["function-value alias"] = true
			}
		}
		if m.Kind == "regex" {
			checks["regex receiver"] = true
			if strings.Contains(m.Code, "matchAlias(") {
				checks["cross-file method alias"] = true
			}
		}
		if m.Kind == "string-index" {
			checks["typed text index"] = true
		}
	}
	for _, check := range []string{"generic method closure", "registry callback", "opaque callback", "unused method imported alias", "global initializer", "imported call alias", "regex receiver", "typed text index", "function-value alias", "cross-file method alias"} {
		if !checks[check] {
			t.Errorf("missing %s", check)
		}
	}
	if len(positive.InvisibleBodies) == 0 || len(positive.Limitations) == 0 {
		t.Fatal("approximation boundaries not reported")
	}
	file := filepath.Join(c.Dir, "source.go")
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct{ name, append, old, replacement, marker string }{
		{name: "arbitrary producer", append: "\nfunc freshUnfamiliar() []string { return []string{\"a\"} }\n", marker: "freshUnfamiliar"},
		{name: "membership", append: "\nfunc freshMember(m hiddenMap,k string) bool { _,ok:=m[k];return ok }\n", marker: "freshMember"},
		{name: "call kind", append: "\nfunc freshKind(k int) bool { return k==9 }\n", marker: "freshKind"},
		{name: "alias map", append: "\ntype freshAlias = hiddenMap\nfunc freshAliasProducer() freshAlias { return freshAlias{\"x\":1} }\n", marker: "freshAlias"},
		{name: "global schema", append: "\nvar freshGlobal = headers.Schema{Header: []string{\"new\"}}\n", marker: "freshGlobal"},
		{name: "known consumer", old: "return matcher.MatchString(query)", replacement: "if query == \"secret\" { return false }; return matcher.MatchString(query)", marker: "sourceConsumer"},
		{name: "without fact tokens", append: "\nfunc arbitraryNoise() int { return 1 }\n", marker: "arbitraryNoise"},
		{name: "reachable loop", old: "func Execute() {", replacement: "func Execute() { for censusProbe:=0;censusProbe<4;censusProbe++ { Sink++ }", marker: "Execute"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			mutated := string(raw) + probe.append
			if probe.old != "" {
				if !strings.Contains(mutated, probe.old) {
					t.Fatal("fixture moved")
				}
				mutated = strings.Replace(mutated, probe.old, probe.replacement, 1)
			}
			config := c
			config.Overlay = map[string][]byte{file: []byte(mutated)}
			actual := mustExtract(t, config)
			failures := Validate(actual, approved, fixturePolicy)
			found := false
			for _, failure := range failures {
				if strings.Contains(failure, probe.marker) && (strings.HasPrefix(failure, "UNLISTED ") || strings.HasPrefix(failure, "CHANGED ") || strings.HasPrefix(failure, "OWNER CHANGED ")) {
					found = true
				}
			}
			if !found {
				t.Fatalf("mutation escaped the shared gate: %s: %v", probe.name, failures)
			}
		})
	}
	// Named source identities survive location changes. Anonymous owners are
	// occurrence identities and require fresh coordinates when moved by an edit.
	c.Overlay = map[string][]byte{file: append([]byte("\n\n"), raw...)}
	named := func(inv Inventory) Inventory {
		var members []Member
		for _, m := range inv.Members {
			if !strings.Contains(m.Owner, "@source:") {
				members = append(members, m)
			}
		}
		inv.Members = members
		return inv
	}
	if failures := Validate(named(mustExtract(t, c)), named(approved), fixturePolicy); len(failures) > 0 {
		t.Fatal("line-based identity", failures)
	}
}

func TestGenericCensusSelectorData(t *testing.T) {
	c := genericConfig(t)
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Selectors) != len(c.Selectors) || decoded.Roots[0].Required != c.Roots[0].Required {
		t.Fatal("configuration data lost")
	}
	actual := mustExtract(t, decoded)
	if len(actual.Members) == 0 {
		t.Fatal("JSON client data extracted no members")
	}
	var selector Selector
	if err := json.Unmarshal([]byte(`{"kind":"walk","node":"range","operand_types":["[]uint8"],"reachability":"all","package_prefixes":["example/"]}`), &selector); err != nil {
		t.Fatal(err)
	}
	if selector.OperandTypes[0] != "[]uint8" || selector.PackagePrefixes[0] != "example/" {
		t.Fatal("snake-case selector data not accepted")
	}
	c.Roots = nil
	c.Selectors = []Selector{{Kind: "field", Node: "field", Names: []string{"Extra"}, Packages: []string{"census.fixture/meta"}, Reachability: "all"}}
	fields := mustExtract(t, c)
	if len(fields.Members) != 1 || !strings.Contains(fields.Members[0].Code, "Extra") {
		t.Fatal("aliased/multiple field selector missed its symbol", fields.Members)
	}
}

func TestGenericCensusDeclarationBoundaries(t *testing.T) {
	c := genericConfig(t)
	c.Roots = nil
	c.IncludeInactive = true
	c.Selectors = []Selector{{Kind: "all", Node: "declaration", Reachability: "all"}}
	inv := mustExtract(t, c)
	inactive, generated, alias := false, false, false
	for _, m := range inv.Members {
		inactive = inactive || (strings.Contains(m.State, "inactive") && !m.Typed && strings.Contains(m.Owner, "arbitraryInactive"))
		generated = generated || (strings.Contains(m.State, "generated") && strings.Contains(m.Owner, "arbitraryGenerated"))
		alias = alias || (m.Declaration == "alias" && strings.Contains(m.UnderlyingType, "map["))
	}
	if !inactive || !generated || !alias {
		t.Fatalf("declaration boundaries missing: inactive=%v generated=%v alias=%v", inactive, generated, alias)
	}
	if len(inv.Sources) < 4 {
		t.Fatal("missing source/build boundary report")
	}
	c = genericConfig(t)
	c.Roots = []Root{{Package: "census.fixture", Name: "Walk", Receiver: "box", Required: true}}
	if len(mustExtract(t, c).Roots) != 1 {
		t.Fatal("generic receiver root did not resolve uniquely")
	}
	c.Roots[0].Receiver = "missing"
	if _, err := Extract(c); err == nil {
		t.Fatal("missing required method root passed")
	}
}

func TestGenericCensusBoundProofs(t *testing.T) {
	c := genericConfig(t)
	c.Roots = nil
	c.Selectors = []Selector{{Kind: "for", Node: "for", Reachability: "all"}, {Kind: "range", Node: "range", Reachability: "all"}}
	inv := mustExtract(t, c)
	checks := map[string]bool{}
	rangeFunctions := 0
	for _, m := range inv.Members {
		if m.Kind == "range" && strings.HasSuffix(m.Owner, ".rangeFunction") {
			rangeFunctions++
		}
		switch {
		case strings.Contains(m.Code, "fixedCounter"):
			checks["fixed"] = m.Bound == 3
		case strings.Contains(m.Code, "resetCounter"):
			checks["body assignment"] = m.Bound == 0
		case strings.Contains(m.Code, "escape(&escapedCounter)"):
			checks["address escape"] = m.Bound == 0
		case strings.Contains(m.Code, "range []int{7: 1}"):
			checks["keyed literal"] = m.Bound == 8
		case strings.Contains(m.Code, "range values") && strings.Contains(m.Code, "values[key+1]"):
			checks["map capacity"] = m.Bound == 0
		}
	}
	for _, name := range []string{"fixed", "body assignment", "address escape", "keyed literal", "map capacity"} {
		if !checks[name] {
			t.Errorf("bound proof failed: %s", name)
		}
	}
	if rangeFunctions != 1 {
		t.Fatalf("range-over-function source counted %d times", rangeFunctions)
	}
}
