package fabric

// collectPlannedExecs returns the executable fragments of a plan in
// execution order (APPLY input before inner, UNION left before right).
func collectPlannedExecs(fragment Fragment) []*FragmentExec {
	switch f := fragment.(type) {
	case *FragmentExec:
		return []*FragmentExec{f}
	case *FragmentApply:
		return append(collectPlannedExecs(f.Input), collectPlannedExecs(f.Inner)...)
	case *FragmentUnion:
		return append(collectPlannedExecs(f.LHS), collectPlannedExecs(f.RHS)...)
	default:
		return nil
	}
}

func plannerUseCatalog() *Catalog {
	catalog := NewCatalog()
	catalog.Register("cmp", &LocationLocal{DBName: "cmp"})
	catalog.Register("cmp.a", &LocationLocal{DBName: "dba"})
	catalog.Register("cmp.b", &LocationLocal{DBName: "dbb"})
	return catalog
}

// TestPlan_DynamicUseKeepsReference pins that a leading dynamic USE plans
// one fragment that carries the graph reference (not a graph name) and the
// session's composite as the scope its constituents come from, for a
// session on the composite or on one of its constituents.
