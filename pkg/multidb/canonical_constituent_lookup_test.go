package multidb

import "testing"

func TestFindCanonicalConstituentUsesLastFoldedAlias(t *testing.T) {
	refs := []ConstituentRef{
		{Alias: "Remote", DatabaseName: "older"},
		{Alias: "remote", DatabaseName: "winner"},
		{Alias: "İ", DatabaseName: "unicode"},
	}

	index, ref, found := findCanonicalConstituent(refs, "REMOTE")
	if !found || index != 1 || ref.DatabaseName != "winner" {
		t.Fatal("lookup did not select the last case-folded alias")
	}

	index, ref, found = findCanonicalConstituent(refs, "i")
	if !found || index != 2 || ref.DatabaseName != "unicode" {
		t.Fatal("lookup did not preserve lowercase Unicode matching")
	}
}

func TestFindCanonicalConstituentASCIIPathDoesNotAllocate(t *testing.T) {
	refs := []ConstituentRef{
		{Alias: "Remote", DatabaseName: "remote"},
		{Alias: "r", DatabaseName: "short"},
		{Alias: "much-longer-alias", DatabaseName: "long"},
	}
	var selected ConstituentRef
	allocs := testing.AllocsPerRun(1000, func() {
		_, selected, _ = findCanonicalConstituent(refs, "REMOTE")
	})
	if allocs != 0 {
		t.Fatalf("ASCII alias lookup used %.2f allocations per operation", allocs)
	}
	if selected.DatabaseName != "remote" {
		t.Fatal("alias lookup returned the wrong constituent")
	}
}
