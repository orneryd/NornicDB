package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenericCensusDelegationObservationControl(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Selectors = []Selector{{Kind: "delegate", Node: "call", Names: []string{"Execute"}, Packages: []string{"census.fixture"}}, {Kind: "observation", Node: "call", Names: []string{"Slice"}, Packages: []string{"sort"}}}
	path := filepath.Join(c.Dir, "source.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{path: append(raw, []byte("\nfunc delegatingEntrypoint(){ _ = sort.Strings; Execute() }\n")...)}
	c.Roots = append(c.Roots, Root{Package: "census.fixture", Name: "delegatingEntrypoint", Required: true})
	actual := mustExtract(t, c)
	if actual.Program == nil || actual.Program.SSA == nil || actual.Program.CallGraph == nil || len(actual.Program.Packages) == 0 || len(actual.Program.Functions) == 0 {
		t.Fatal("shared loaded program unavailable")
	}
	var delegate, observation Member
	var definition Definition
	for _, m := range actual.Members {
		if m.Kind == "delegate" {
			delegate = m
		}
		if m.Kind == "observation" {
			observation = m
		}
	}
	for _, d := range actual.Definitions {
		if d.Symbol == "census.fixture.Execute" {
			definition = d
		}
	}
	if delegate.ID == "" || observation.ID == "" || observation.OwnerSymbol != definition.Symbol {
		t.Fatal("missing exact callee/call facts", delegate, observation, definition)
	}
	control := SourceControl{ID: "remove sort call", MemberID: observation.ID, Replacement: "_ = values"}
	witness := CallWitness{MemberID: observation.ID, Digest: observation.Digest, Callee: "sort.Slice", RemovalControl: control.ID}
	p := Policy{Classes: []string{"owned by another layer", "reviewed"}, ClassKinds: map[string]string{"reviewed": "review"}, RequireDelegationWitnesses: true, Controls: []SourceControl{control}, Layers: []OwnerLayer{{Name: "fixture owner", Members: []LayerMember{{Symbol: definition.Symbol, Digest: definition.Digest, Observations: []CallWitness{witness}}}}}}
	approval := reviewed(actual)
	for i := range approval.Members {
		if approval.Members[i].Kind == "delegate" {
			approval.Members[i].Class = "owned by another layer"
			approval.Members[i].OwnerLayer = "fixture owner"
		}
	}
	if failures := Validate(actual, approval, p); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, probe := range []struct {
		name, want string
		change     func(*Policy)
	}{
		{"missing witness", "NO DELEGATION OBSERVATION", func(p *Policy) { p.Layers[0].Members[0].Observations = nil }},
		{"wrong site", "DELEGATION OBSERVATION OWNER", func(p *Policy) {
			p.Layers[0].Members[0].Observations[0].MemberID = delegate.ID
			p.Controls[0].MemberID = delegate.ID
		}},
		{"wrong target", "DELEGATION OBSERVATION CHANGED", func(p *Policy) { p.Layers[0].Members[0].Observations[0].Callee = "imaginary.Observe" }},
		{"missing removal control", "NO OBSERVATION REMOVAL CONTROL", func(p *Policy) { p.Controls = nil }},
	} {
		t.Run(probe.name, func(t *testing.T) {
			copy := p
			member := p.Layers[0].Members[0]
			member.Observations = append([]CallWitness(nil), member.Observations...)
			copy.Layers = []OwnerLayer{{Name: "fixture owner", Members: []LayerMember{member}}}
			copy.Controls = append([]SourceControl(nil), p.Controls...)
			probe.change(&copy)
			if !strings.Contains(strings.Join(Validate(actual, approval, copy), "\n"), probe.want) {
				t.Fatal("witness contradiction accepted", probe.want)
			}
		})
	}
	mutated, err := ExtractWithControl(c, control)
	if err != nil {
		t.Fatal("removal must compile through the real extraction", err)
	}
	// Refresh the generic body pin and remove the observation approval too.
	// The delegated row must STILL fail for its missing observation, independently
	// of ordinary stale rows or hashes. Clients retain behavioral execution proofs.
	for _, d := range mutated.Definitions {
		if d.Symbol == definition.Symbol {
			p.Layers[0].Members[0].Digest = d.Digest
		}
	}
	remaining := approval
	remaining.Members = nil
	for _, m := range approval.Members {
		if m.ID != observation.ID {
			remaining.Members = append(remaining.Members, m)
		}
	}
	failures := Validate(mutated, remaining, p)
	want := "DELEGATION OBSERVATION MISSING " + delegate.ID + " " + observation.ID
	if len(failures) != 1 || failures[0] != want {
		t.Fatal("source removal escaped the delegation control", failures)
	}
}
