package cyphertext

import (
	"reflect"
	"strings"
	"testing"
)

func TestFilterImportConjunctsPreservesPolicies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		terms    []string
		column   string
		policy   IdentifierReferencePolicy
		want     string
		accepted bool
	}{
		{name: "whitespace fast path", terms: []string{" \n\t"}, column: "seed", policy: IdentifierExact, accepted: true},
		{name: "nil", column: "seed", policy: IdentifierExact, accepted: true},
		{name: "blank terms", terms: []string{"", " \t"}, column: "seed", policy: IdentifierUnqualifiedFold, accepted: true},
		{name: "all guards folded", terms: []string{" SEED is not null ", "`seed` IS NOT NULL"}, column: "seed", policy: IdentifierExact, accepted: true},
		{name: "preserve conjunct order", terms: []string{"seed IS NOT NULL", " n.active = true ", "`seed` IS NOT NULL", "n.count > 0"}, column: "seed", policy: IdentifierUnqualifiedFold, want: "n.active = true AND n.count > 0", accepted: true},
		{name: "fold rejects case variant", terms: []string{"SEED > 0"}, column: "seed", policy: IdentifierUnqualifiedFold},
		{name: "exact retains case variant", terms: []string{"SEED > 0"}, column: "seed", policy: IdentifierExact, want: "SEED > 0", accepted: true},
		{name: "fold excludes property name", terms: []string{"n.seed > 0"}, column: "seed", policy: IdentifierUnqualifiedFold, want: "n.seed > 0", accepted: true},
		{name: "exact rejects property name", terms: []string{"n.seed > 0"}, column: "seed", policy: IdentifierExact},
		{name: "fold excludes property owner", terms: []string{"seed.active = true"}, column: "seed", policy: IdentifierUnqualifiedFold, want: "seed.active = true", accepted: true},
		{name: "exact rejects property owner", terms: []string{"seed.active = true"}, column: "seed", policy: IdentifierExact},
		{name: "quoted literal not guard", terms: []string{"'seed' IS NOT NULL"}, column: "seed", policy: IdentifierExact, want: "'seed' IS NOT NULL", accepted: true},
		{name: "quoted source and comment", terms: []string{"n.text = 'seed AND seed' /* seed */"}, column: "seed", policy: IdentifierUnqualifiedFold, want: "n.text = 'seed AND seed' /* seed */", accepted: true},
		{name: "other column guard retained", terms: []string{"other IS NOT NULL"}, column: "seed", policy: IdentifierExact, want: "other IS NOT NULL", accepted: true},
		{name: "nullable import rejected", terms: []string{"seed IS NULL"}, column: "seed", policy: IdentifierUnqualifiedFold},
		{name: "guard comment prevents whole guard", terms: []string{"seed IS NOT NULL /* retained context */"}, column: "seed", policy: IdentifierExact},
		{name: "later reference prevents rewrite", terms: []string{"seed IS NOT NULL", "n.active = true", "seed > 1"}, column: "seed", policy: IdentifierUnqualifiedFold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := append([]string(nil), tc.terms...)
			source := strings.Join(tc.terms, " AND ")
			splitCalls := 0
			got, accepted := FilterImportConjuncts(source, tc.column, tc.policy, func(gotSource string) []string {
				splitCalls++
				if gotSource != source {
					t.Fatal("filter changed source before caller-owned splitting")
				}
				return tc.terms
			})
			wantSplitCalls := 1
			if strings.TrimSpace(source) == "" {
				wantSplitCalls = 0
			}
			if splitCalls != wantSplitCalls {
				t.Fatalf("split calls = %d, want %d", splitCalls, wantSplitCalls)
			}
			if got != tc.want || accepted != tc.accepted {
				t.Fatalf("filtered = (%q, %v), want (%q, %v)", got, accepted, tc.want, tc.accepted)
			}
			if !reflect.DeepEqual(tc.terms, original) {
				t.Fatal("filter mutated caller-owned conjuncts")
			}
		})
	}
}
