package workcensus

import "testing"

func TestGenericCensusProgramReachability(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	inv := mustExtract(t, c)
	root := Root{Package: "census.fixture", Name: "Execute", Required: true}
	r, err := inv.Program.ReachableFrom([]Root{root})
	if err != nil || !r.Contains(root) || len(r.Roots) != 1 {
		t.Fatal(r.Roots, err)
	}
	for _, member := range inv.Members {
		if member.Reachable && member.OwnerSymbol != "" {
			found := false
			for fn := range r.Functions {
				found = found || sourceFunction(fn).String() == member.OwnerSymbol
			}
			if !found {
				t.Fatal("Extract and program query disagree", member.ID)
			}
		}
	}
	empty, err := inv.Program.ReachableFrom(nil)
	if err != nil || len(empty.Functions) != 0 {
		t.Fatal("empty roots widened population", err)
	}
	if _, err := inv.Program.ReachableFrom([]Root{{Package: "census.fixture", Name: "missing", Required: true}}); err == nil {
		t.Fatal("missing root accepted")
	}
}
