package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenericCensusDispositionContradictions(t *testing.T) {
	c := genericConfig(t)
	c.Selectors = []Selector{{Kind: "split", Node: "call", Packages: []string{"strings"}, Names: []string{"Split"}}}
	actual := mustExtract(t, c)
	if len(actual.Members) == 0 || !actual.Members[0].Reachable {
		t.Fatal("fixture must expose reachable source facts", actual)
	}
	for _, class := range []string{"non-statement boundary", "owned by another layer"} {
		t.Run(class, func(t *testing.T) {
			approval := reviewed(actual)
			for i := range approval.Members {
				approval.Members[i].Class = class
			}
			failures := Validate(actual, approval, Policy{Classes: []string{class}})
			if len(failures) == 0 {
				t.Fatalf("disposition contradicts extraction but was accepted: %s", class)
			}
		})
	}
	// Custom class spellings carry the same checked semantics. Falsifying the
	// reviewed copy's reachability cannot override the extracted source fact.
	approval := reviewed(actual)
	for i := range approval.Members {
		approval.Members[i].Class = "maintenance exemption"
		approval.Members[i].Reachable = false
	}
	p := Policy{Classes: []string{"maintenance exemption"}, ClassKinds: map[string]string{"maintenance exemption": "non-statement"}}
	if failures := Validate(actual, approval, p); !strings.Contains(strings.Join(failures, "\n"), "REACHABLE NON-STATEMENT") {
		t.Fatal("custom non-statement class escaped facts", failures)
	}
	p.ClassKinds["non-statement boundary"] = "review"
	if failures := Validate(actual, approval, p); !strings.Contains(strings.Join(failures, "\n"), "CLASS KIND CONTRADICTION") {
		t.Fatal("conventional meanings cannot be disabled", failures)
	}
	// A layer listing the actual opaque target still lacks a visible body.
	opaque := reviewed(actual)
	for i := range opaque.Members {
		opaque.Members[i].Class, opaque.Members[i].OwnerLayer = "owned by another layer", "opaque readers"
	}
	opaquePolicy := Policy{Classes: []string{"owned by another layer"}, Layers: []OwnerLayer{{Name: "opaque readers", Members: []LayerMember{{Symbol: "strings.Split", Digest: "claimed invisible body"}}}}}
	if failures := Validate(actual, opaque, opaquePolicy); !strings.Contains(strings.Join(failures, "\n"), "OWNER LAYER CALLEE") {
		t.Fatal("opaque source is not ownership evidence", failures)
	}

	c.Selectors = []Selector{{Kind: "delegate", Node: "call", Names: []string{"sourceConsumer"}, Packages: []string{"census.fixture"}}}
	actual = mustExtract(t, c)
	approval = reviewed(actual)
	var target Definition
	for _, definition := range actual.Definitions {
		if definition.Symbol == "census.fixture.sourceConsumer" {
			target = definition
		}
	}
	if target.Digest == "" || len(actual.Members) == 0 {
		t.Fatal("missing real callee definition", actual)
	}
	for i := range approval.Members {
		approval.Members[i].Class = "owned by another layer"
		approval.Members[i].OwnerLayer = "fixture readers"
	}
	p = Policy{Classes: []string{"owned by another layer"}, Layers: []OwnerLayer{{Name: "fixture readers", Members: []LayerMember{{Symbol: target.Symbol, Digest: target.Digest}}}}}
	if failures := Validate(actual, approval, p); len(failures) != 0 {
		t.Fatal("real source callee must pass ownership consistency", failures)
	}
	for _, probe := range []struct{ name, symbol, digest string }{
		{"wrong callee", "census.fixture.arbitraryHeader", target.Digest},
		{"stale body", target.Symbol, "old-body"},
		{"invisible callee", "invisible.Plugin", "claimed-body"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			bad := p
			bad.Layers = []OwnerLayer{{Name: "fixture readers", Members: []LayerMember{{Symbol: probe.symbol, Digest: probe.digest}}}}
			if failures := Validate(actual, approval, bad); !strings.Contains(strings.Join(failures, "\n"), "OWNER LAYER CALLEE") {
				t.Fatal("invented delegation escaped extracted definitions", failures)
			}
		})
	}
	c.Selectors = []Selector{{Kind: "range", Node: "range"}}
	loops := mustExtract(t, c)
	claims := reviewed(loops)
	for i := range claims.Members {
		claims.Members[i].Class, claims.Members[i].OwnerLayer = "owned by another layer", "fixture readers"
	}
	if failures := Validate(loops, claims, p); !strings.Contains(strings.Join(failures, "\n"), "NO DELEGATABLE CALLEE") {
		t.Fatal("nested callee cannot own an enclosing loop", failures)
	}
}

func TestGenericCensusSliceViewBounds(t *testing.T) {
	c := genericConfig(t)
	c.Roots = nil
	c.Selectors = []Selector{{Kind: "range", Node: "range", Reachability: "all"}}
	actual := mustExtract(t, c)
	seen := map[string]bool{}
	for _, m := range actual.Members {
		switch {
		case strings.HasSuffix(m.Owner, ".runtimeHighView"):
			seen["runtime"] = true
			if m.Bound != 0 || m.BoundReason != "" {
				t.Errorf("runtime high is not bounded by backing length: %+v", m)
			}
			approval := reviewed(Inventory{Members: []Member{m}})
			approval.Members[0].Class = "bounded"
			approval.Members[0].Bound = 1
			approval.Members[0].BoundReason = "maximum of all fixed-size initializers/assignments (no unknown length writer)"
			if len(Validate(Inventory{Members: []Member{m}}, approval, Policy{Classes: []string{"bounded"}})) == 0 {
				t.Error("runtime-sized view accepted as bounded")
			}
			// Exact human metadata equality cannot manufacture a proof.
			approval.Members[0].Bound, approval.Members[0].BoundReason = m.Bound, m.BoundReason
			if failures := Validate(Inventory{Members: []Member{m}}, approval, Policy{Classes: []string{"bounded"}}); !strings.Contains(strings.Join(failures, "\n"), "UNSOUND BOUND") {
				t.Error("matching unknown evidence is not a bound", failures)
			}
		case strings.HasSuffix(m.Owner, ".constantExpandedView"):
			seen["constant"] = true
			if m.Bound != 64 {
				t.Errorf("constant expansion has 64 elements, got bound %d", m.Bound)
			}
			approval := reviewed(Inventory{Members: []Member{m}})
			approval.Members[0].Class = "bounded"
			if failures := Validate(Inventory{Members: []Member{m}}, approval, Policy{Classes: []string{"bounded"}}); len(failures) != 0 {
				t.Error("sound constant extent rejected", failures)
			}
		case strings.HasSuffix(m.Owner, ".uncertainProducerView"), strings.HasSuffix(m.Owner, ".shadowedMakeView"):
			seen[m.Owner] = true
			if m.BoundKnown || m.Bound != 0 || m.BoundReason != "" {
				t.Error("uncertain producer manufactured a fixed length", m)
			}
		}
	}
	if !seen["runtime"] || !seen["constant"] || !seen["census.fixture.uncertainProducerView"] || !seen["census.fixture.shadowedMakeView"] {
		t.Fatal("missing slice view fixtures", seen)
	}
}

// A bounded induction proof must include representable termination, not just
// an increasing token. These bodies are extracted, never executed.
func TestGenericCensusInductionOverflow(t *testing.T) {
	c := genericConfig(t)
	c.Selectors = []Selector{{Kind: "for", Node: "for", Reachability: "all"}}
	path := filepath.Join(c.Dir, "source.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{path: append(raw, []byte(`
 func overflowingInduction() { for i:=uint8(0);i<=255;i++{Sink++};for i:=int8(0);i<=127;i++{Sink++} }
 func terminatingInduction() { for i:=uint8(0);i<255;i++{Sink++};for i:=int8(0);i<=126;i++{Sink++} }
 `)...)}
	inv := mustExtract(t, c)
	seen := 0
	for _, m := range inv.Members {
		if strings.Contains(m.Owner, "overflowingInduction") {
			seen++
			if m.BoundKnown {
				t.Error("overflowing induction marked bounded", m.ID, m.Bound)
			}
		}
		if strings.Contains(m.Owner, "terminatingInduction") && !m.BoundKnown {
			t.Error("lost sound terminating bound", m.ID)
		}
	}
	if seen != 2 {
		t.Fatal("overflow fixtures missing", seen)
	}
}
