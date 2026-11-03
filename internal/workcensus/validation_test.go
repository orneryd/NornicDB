package workcensus

import (
	"strings"
	"testing"
)

func TestCensusClassificationIsStrict(t *testing.T) {
	m := Member{ID: "new helper/iterator#1", Digest: "reviewed", Reachable: true, Class: "contextual statement owner", Reason: "caller owns enumeration"}
	good := Inventory{Roots: []string{"statement"}, Members: []Member{m}}
	if failures := Validate(good, good, strictClassificationPolicy()); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct {
		name             string
		actual, approved Inventory
		want             string
	}{
		{"new", good, Inventory{Roots: good.Roots}, "UNLISTED"},
		{"stale", Inventory{Roots: good.Roots}, good, "STALE"},
		{"roots", Inventory{Members: good.Members}, good, "ROOT POPULATION CHANGED"},
		{"changed", Inventory{Roots: good.Roots, Members: []Member{{ID: m.ID, Digest: "mutated"}}}, good, "CHANGED"},
		{"class", good, Inventory{Roots: good.Roots, Members: []Member{{ID: m.ID, Digest: m.Digest, Reason: m.Reason}}}, "UNCLASSIFIED"},
		{"reason", good, Inventory{Roots: good.Roots, Members: []Member{{ID: m.ID, Digest: m.Digest, Class: m.Class}}}, "NO REASON"},
		{"duplicate", good, Inventory{Roots: good.Roots, Members: []Member{m, m}}, "DUPLICATE"},
		{"bound", good, Inventory{Roots: good.Roots, Members: []Member{{ID: m.ID, Digest: m.Digest, Class: "bounded", Reason: m.Reason, Bound: 2, BoundReason: "fixed array length"}}}, "BOUND CHANGED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failures := Validate(tc.actual, tc.approved, strictClassificationPolicy())
			if !strings.Contains(strings.Join(failures, "\n"), tc.want) {
				t.Fatalf("wanted %s: %v", tc.want, failures)
			}
		})
	}
}

func TestCensusEvidencePolicy(t *testing.T) {
	m := Member{ID: "source/owner/declaration#1", Digest: "syntax", OwnerDigest: "body", Class: "reviewed", Reason: "reason", ReviewOwner: "owner", Subjects: []string{"behavior"}}
	good := Inventory{Roots: []string{"root"}, Members: []Member{m}}
	p := Policy{Classes: []string{"reviewed"}, ClassKinds: map[string]string{"reviewed": "review"}, RequireOwnerDigest: true, RequireReviewOwner: true, RequireSubjects: true, Owners: []string{"owner"}, Subjects: []string{"behavior"}}
	if failures := Validate(good, good, p); len(failures) > 0 {
		t.Fatal(failures)
	}
	for _, tc := range []struct {
		name, want string
		change     func(*Inventory)
	}{
		{"owner body", "OWNER CHANGED", func(i *Inventory) { i.Members[0].OwnerDigest = "changed" }},
		{"review owner", "REVIEW OWNER", func(i *Inventory) { i.Members[0].ReviewOwner = "" }},
		{"unknown owner", "REVIEW OWNER", func(i *Inventory) { i.Members[0].ReviewOwner = "unreviewed" }},
		{"subject", "NO SUBJECT", func(i *Inventory) { i.Members[0].Subjects = nil }},
		{"unknown subject", "UNKNOWN SUBJECT", func(i *Inventory) { i.Members[0].Subjects = []string{"unknown"} }},
		{"duplicate root", "DUPLICATE ROOT", func(i *Inventory) { i.Roots = append(i.Roots, "root") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := Inventory{Roots: append([]string(nil), good.Roots...), Members: append([]Member(nil), good.Members...)}
			tc.change(&inv)
			if f := Validate(good, inv, p); !strings.Contains(strings.Join(f, "\n"), tc.want) {
				t.Fatalf("wanted %s: %v", tc.want, f)
			}
		})
	}
	actual := good
	actual.Members = append(append([]Member(nil), good.Members...), m)
	if f := Validate(actual, good, p); !strings.Contains(strings.Join(f, "\n"), "DUPLICATE ACTUAL") {
		t.Fatal(f)
	}
}

// This fixture isolates class/identity consistency. Production evidence metadata
// is exercised by TestCensusEvidencePolicy; this package has no client manifest.
func strictClassificationPolicy() Policy {
	return Policy{Classes: []string{"contextual statement owner", "bounded", "local statement work", "distinct responsibility"}, ClassKinds: map[string]string{"local statement work": "statement", "distinct responsibility": "review"}}
}
