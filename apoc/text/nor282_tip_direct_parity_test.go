package text

import "testing"

func TestNOR282DirectReferenceParity(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	ran := 0
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "text" {
			continue
		}
		ran++
		t.Run(tc.ID, func(t *testing.T) {
			if tc.Query == "" {
				t.Fatalf("case %s has no public query", tc.ID)
			}
			owner, ok := nor282TextOwners[tc.Direct.Function]
			if !ok {
				t.Fatalf("corpus names unknown apoc/text owner %q", tc.Direct.Function)
			}
			got, err := owner(tc.Direct.Args)
			nor282RequireExpectation(t, "apoc/text "+tc.Direct.Function, tc, got, err)
		})
	}
	if ran == 0 {
		t.Fatal("no direct apoc/text cases ran")
	}
}

// TestNOR282DirectObserved reproduces the current NornicDB values so the
// defect is pinned independently of the reference oracle.
func TestNOR282DirectObserved(t *testing.T) {
	corpus := loadNOR282Corpus(t)
	for _, tc := range corpus.Cases {
		if tc.Direct == nil || tc.Direct.Package != "text" {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			owner, ok := nor282TextOwners[tc.Direct.Function]
			if !ok {
				t.Fatalf("corpus names unknown apoc/text owner %q", tc.Direct.Function)
			}
			got, err := owner(tc.Direct.Args)
			if tc.Observed.Kind == "" {
				t.Fatalf("no nornicdb_observed recorded for %s: owner %s returned %#v, error %v", tc.ID, tc.Owner, got, err)
			}
			nor282RequireObserved(t, "apoc/text "+tc.Direct.Function, tc, got, err)
		})
	}
}

// nor282RequireExpectation compares a call result with the recorded reference
// expectation. A value must match exactly; a function_error must make the call
// fail as the fresh APOC reference does.
