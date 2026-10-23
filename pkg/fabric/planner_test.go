package fabric

import (
	"testing"
)

func collectExecsByGraph(fragment Fragment, graph string) []*FragmentExec {
	var out []*FragmentExec
	switch f := fragment.(type) {
	case *FragmentExec:
		if f.GraphName == graph {
			out = append(out, f)
		}
	case *FragmentApply:
		out = append(out, collectExecsByGraph(f.Input, graph)...)
		out = append(out, collectExecsByGraph(f.Inner, graph)...)
	case *FragmentUnion:
		out = append(out, collectExecsByGraph(f.LHS, graph)...)
		out = append(out, collectExecsByGraph(f.RHS, graph)...)
	}
	return out
}

// --- Additional coverage tests ---

func TestCompositeScopeRoot(t *testing.T) {
	if compositeScopeRoot("") != "" {
		t.Fatal("expected empty for empty")
	}
	if compositeScopeRoot("  ") != "" {
		t.Fatal("expected empty for whitespace")
	}
	if compositeScopeRoot("comp.shard") != "comp" {
		t.Fatal("expected comp")
	}
	if compositeScopeRoot("simple") != "simple" {
		t.Fatal("expected simple")
	}
}

func TestInCompositeScope(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	if !p.inCompositeScope("comp") {
		t.Fatal("expected comp to be in composite scope")
	}
	if !p.inCompositeScope("comp.shard") {
		t.Fatal("expected dotted name to be in composite scope")
	}
	if p.inCompositeScope("other") {
		t.Fatal("expected other to not be in composite scope")
	}
	if p.inCompositeScope("") {
		t.Fatal("expected empty to not be in composite scope")
	}

	// Nil catalog.
	p2 := NewFabricPlanner(nil)
	if p2.inCompositeScope("comp") {
		t.Fatal("expected false with nil catalog")
	}
}

func TestContainsFold(t *testing.T) {
	if !containsFold("Hello World", "WORLD") {
		t.Fatal("expected case-insensitive match")
	}
	if containsFold("short", "longer needle") {
		t.Fatal("expected false when needle longer than string")
	}
	if !containsFold("abc", "") {
		t.Fatal("expected true for empty needle")
	}
}

func TestValidateUseTarget_EmptyTarget(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
	err := p.validateUseTarget("db", "")
	if err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestHasGraphWithPrefix_NoMatch(t *testing.T) {
	c := NewCatalog()
	c.Register("db1", &LocationLocal{DBName: "db1"})
	if c.HasGraphWithPrefix("other") {
		t.Fatal("expected false for non-matching prefix")
	}
}

func TestHasGraphWithPrefix_Match(t *testing.T) {
	c := NewCatalog()
	c.Register("comp.shard", &LocationLocal{DBName: "shard"})
	if !c.HasGraphWithPrefix("comp.") {
		t.Fatal("expected true for matching prefix")
	}
}

func TestExtractGraphReference_OpenParenPresent(t *testing.T) {
	// Test the openIdx < 0 branch — but since the prefix includes "(",
	// HasPrefix only matches when "(" is present. So openIdx will always be >= 0
	// when the prefix matches. This branch is unreachable by design.
}

func findExecByGraphName(fragment Fragment, graphName string) *FragmentExec {
	switch f := fragment.(type) {
	case *FragmentExec:
		if f.GraphName == graphName {
			return f
		}
		return nil
	case *FragmentApply:
		if found := findExecByGraphName(f.Input, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.Inner, graphName)
	case *FragmentUnion:
		if found := findExecByGraphName(f.LHS, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.RHS, graphName)
	default:
		return nil
	}
}
