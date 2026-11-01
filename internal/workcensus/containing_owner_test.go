package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const containingFixture = `
type censusBudget struct{ observations, events int }
func lowerObserve(scope *censusBudget) int { scope.observations++; return 0 }
func lowerEvent(scope *censusBudget) { scope.events++ }
func foreignObservation(scope *censusBudget) { _ = lowerObserve(scope) }
func containingEntry() { lowerInside(&censusBudget{}, &censusBudget{}, []int{1,2}) }
func lowerInside(scope, other *censusBudget, values []int) {
 for range values { _ = lowerObserve(scope); lowerEvent(scope) }
}
`

func containingFixtureConfig(t *testing.T, snippet string) Config {
	t.Helper()
	c := genericConfig(t)
	path := filepath.Join(c.Dir, "source.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{path: append(raw, []byte(snippet)...)}
	c.Roots = append(c.Roots, Root{Package: "census.fixture", Name: "containingEntry", Required: true})
	c.Selectors = []Selector{
		{Kind: "work loop", Node: "range"},
		{Kind: "observation", Node: "call", Reachability: "all", Packages: []string{"census.fixture"}, Names: []string{"lowerObserve"}},
		{Kind: "work event", Node: "call", Packages: []string{"census.fixture"}, Names: []string{"lowerEvent"}},
	}
	return c
}

func containingApproval(t *testing.T, actual Inventory) (Inventory, Policy, Member, Member) {
	t.Helper()
	var loop, observation Member
	for _, m := range actual.Members {
		if strings.Contains(m.Owner, "lowerInside") {
			if m.Kind == "work loop" {
				loop = m
			}
			if m.Kind == "observation" {
				observation = m
			}
		}
	}
	var owner, scopeOwner Definition
	for _, d := range actual.Definitions {
		if d.ID == loop.OwnerID {
			owner = d
		}
	}
	scopeOwner = owner
	if owner.Kind == "closure" {
		for _, d := range actual.Definitions {
			if d.ID == owner.ParentID {
				scopeOwner = d
			}
		}
	}
	var value ValueDefinition
	for _, v := range scopeOwner.Values {
		if v.Name == "scope" && v.Role == "parameter" {
			value = v
		}
	}
	if loop.ID == "" || observation.ID == "" || value.Reference == "" {
		t.Fatal("missing actual owner/binding/observation facts", loop, observation, owner)
	}
	control := SourceControl{ID: "remove containing observation", MemberID: observation.ID, Replacement: "0"}
	witness := &ContainingOwnerWitness{OwnerID: owner.ID, OwnerDigest: owner.Digest, Observation: CallWitness{MemberID: observation.ID, Digest: observation.Digest, Callee: observation.Callees[0], RemovalControl: control.ID}, Scope: ScopeWitness{OwnerID: scopeOwner.ID, Value: value.Reference, Type: value.Type}}
	p := Policy{Classes: []string{"lower-owned work", "reviewed"}, ClassKinds: map[string]string{"lower-owned work": "observing-owner", "reviewed": "review"}, Controls: []SourceControl{control}, Layers: []OwnerLayer{{Name: "primitive layer", Members: []LayerMember{{OwnerID: owner.ID, Symbol: owner.Symbol, Digest: owner.Digest}}}}}
	approval := reviewed(actual)
	for i := range approval.Members {
		m := &approval.Members[i]
		if m.OwnerID == owner.ID && (m.Kind == "work loop" || m.Kind == "work event") {
			m.Class, m.OwnerLayer, m.ContainingOwner = "lower-owned work", "primitive layer", witness
		}
	}
	return approval, p, loop, observation
}

func TestGenericCensusContainingOwnerWitness(t *testing.T) {
	for _, closure := range []bool{false, true} {
		t.Run(map[bool]string{false: "function", true: "captured closure"}[closure], func(t *testing.T) {
			snippet := containingFixture
			if closure {
				snippet = strings.Replace(snippet, "for range values { _ = lowerObserve(scope); lowerEvent(scope) }", "body := func() { for range values { _ = lowerObserve(scope); lowerEvent(scope) } }; body()", 1)
			}
			c := containingFixtureConfig(t, snippet)
			actual := mustExtract(t, c)
			approval, policy, loop, _ := containingApproval(t, actual)
			if failures := Validate(actual, approval, policy); len(failures) > 0 {
				t.Fatal(failures)
			}
			mutated, err := ExtractWithControl(c, policy.Controls[0])
			if err != nil {
				t.Fatal("source removal control must still type-check", err)
			}
			remaining := approval
			remaining.Members = nil
			workEventRemains := false
			// Refresh ordinary syntax/body approvals and even the layer body pin.
			// Only the required observation is removed; the work event remains.
			for _, m := range mutated.Members {
				var previous Member
				for _, a := range approval.Members {
					if a.ID == m.ID {
						previous = a
					}
				}
				previous.Digest, previous.OwnerDigest = m.Digest, m.OwnerDigest
				if previous.ContainingOwner != nil {
					copy := *previous.ContainingOwner
					copy.OwnerDigest = m.OwnerDigest
					previous.ContainingOwner = &copy
					workEventRemains = workEventRemains || m.Kind == "work event"
				}
				remaining.Members = append(remaining.Members, previous)
			}
			for _, d := range mutated.Definitions {
				if d.ID == loop.OwnerID {
					policy.Layers[0].Members[0].Digest = d.Digest
				}
			}
			failures := Validate(mutated, remaining, policy)
			found := false
			for _, failure := range failures {
				found = found || strings.HasPrefix(failure, "OWNER OBSERVATION MISSING "+loop.ID+" ")
			}
			if !workEventRemains || !found {
				t.Fatal("removal retained work but escaped exact containing-owner proof", workEventRemains, failures)
			}
		})
	}
}

func TestGenericCensusContainingScopeContradictions(t *testing.T) {
	c := containingFixtureConfig(t, containingFixture)
	actual := mustExtract(t, c)
	for _, change := range []string{"wrong owner", "wrong binding", "wrong type", "missing control", "wrong layer"} {
		t.Run(change, func(t *testing.T) {
			approval, policy, loop, observation := containingApproval(t, actual)
			w := approval.Members[0].ContainingOwner
			for _, row := range approval.Members {
				if row.ID == loop.ID {
					w = row.ContainingOwner
				}
			}
			want := "OWNER OBSERVATION SCOPE"
			switch change {
			case "wrong owner":
				for _, m := range actual.Members {
					if strings.Contains(m.Owner, "foreignObservation") && m.Kind == "observation" {
						w.Observation.MemberID, w.Observation.Digest = m.ID, m.Digest
						policy.Controls[0].MemberID = m.ID
					}
				}
				want = "OWNER OBSERVATION OWNER"
			case "wrong binding":
				w.Scope.Value += "-different"
			case "wrong type":
				w.Scope.Type = "int"
			case "missing control":
				policy.Controls = nil
				want = "NO OWNER OBSERVATION REMOVAL CONTROL"
			case "wrong layer":
				policy.Layers[0].Members[0].OwnerID = observation.ID
				policy.Layers[0].Members[0].Symbol = ""
				want = "OWNER LAYER CONTAINING BODY"
			}
			if !strings.Contains(strings.Join(Validate(actual, approval, policy), "\n"), want+" "+loop.ID) {
				t.Fatal("contradiction accepted", change)
			}
		})
	}
	for _, statement := range []string{"scope = other;", "scope, other = other, scope;", "_ = &scope;", "{ scope := other; _ = lowerObserve(scope); lowerEvent(scope) };"} {
		snippet := strings.Replace(containingFixture, "for range values { _ = lowerObserve(scope); lowerEvent(scope) }", "for range values { "+statement+" _ = lowerObserve(scope); lowerEvent(scope) }", 1)
		if strings.HasPrefix(statement, "{") {
			snippet = strings.Replace(containingFixture, "for range values { _ = lowerObserve(scope); lowerEvent(scope) }", "for range values "+strings.TrimSuffix(statement, ";"), 1)
		}
		mutated := mustExtract(t, containingFixtureConfig(t, snippet))
		approval, policy, loop, _ := containingApproval(t, mutated)
		if !strings.Contains(strings.Join(Validate(mutated, approval, policy), "\n"), "OWNER OBSERVATION SCOPE "+loop.ID) {
			t.Fatal("mutated, escaping or shadowed scope accepted", statement)
		}
	}
}
