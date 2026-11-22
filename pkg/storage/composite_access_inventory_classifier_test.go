package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Exercise the extractor, not just a classifier helper: the error binding and
// its scope must come from the same parsed assignment as the access call.
func TestCompositeAccessInventoryClassifier(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		want         constituentAccessClass
	}{
		{"direct", `_, err := c.getConstituent("a"); if err != nil { return nil, err }`, accessPropagates},
		{"renamed", `_, openErr := c.getConstituent("a"); if openErr != nil { return nil, openErr }`, accessPropagates},
		{"reversed_comparison", `_, failure := c.getConstituent("a"); if nil != failure { return nil, failure }`, accessPropagates},
		{"negated_success", `_, failure := c.getConstituent("a"); if !(failure == nil) { return nil, failure }`, accessPropagates},
		{"success_only_return", `engine, err := c.getConstituentForWrite("a"); if err == nil && engine != nil { return nil }`, constituentAccessClass("fallthrough")},
		{"nested_success_return", `engine, err := c.getConstituentForWrite("a"); if err == nil && engine != nil { if createErr == nil { return nil }; return createErr }`, constituentAccessClass("fallthrough")},
		{"success_then_error_return", `_, err := c.getConstituent("a"); if err == nil { return nil }; return err`, accessPropagates},
		{"wrong_error_guard", `_, openErr := c.getConstituent("a"); if err != nil { return err }`, accessUnclassified},
		{"wrong_error_return", `_, err := c.getConstituent("a"); if err != nil { return otherErr }`, accessDiscards},
		{"nil_error_return", `_, err := c.getConstituent("a"); if err != nil { return nil }`, accessDiscards},
		{"error_in_value_slot", `_, err := c.getConstituent("a"); if err != nil { return err, nil }`, accessDiscards},
		{"localized_wrapper", `_, openErr := c.getConstituent("a"); if openErr != nil { return nil, localizedError(message(openErr), openErr) }`, accessPropagates},
		{"unproved_wrapper", `_, err := c.getConstituent("a"); if err != nil { return hide(err) }`, accessUnclassified},
		{"discard_binding", `engine, _ := c.getConstituent("a"); use(engine)`, accessDiscards},
		{"continue", `_, err := c.getConstituent("a"); if err != nil { continue }`, accessSkips},
		{"conditional_return_continue", `_, err := c.getConstituent("a"); if err != nil { if required { return err }; continue }`, constituentAccessClass("conditional")},
		{"conditional_return_fallthrough", `_, err := c.getConstituent("a"); if err != nil && required { return err }`, constituentAccessClass("conditional")},
		{"failure_or_other_guard", `_, err := c.getConstituent("a"); if err != nil || engine == nil { return err }`, accessPropagates},
		{"else_returns_error", `_, err := c.getConstituent("a"); if err == nil { return nil } else { return err }`, accessPropagates},
		{"same_outcome_both_branches", `_, err := c.getConstituent("a"); if err != nil { if flag { return nil, err } else { return nil, err } }`, accessPropagates},
		{"zero_value_declaration", `_, err := c.getConstituent("a"); if err != nil { var zero V; return zero, err }`, accessPropagates},
		{"shadowed_guard", `_, err := c.getConstituent("a"); if err := other(); err != nil { return err }`, accessUnclassified},
		{"overwritten_error", `_, err := c.getConstituent("a"); err = other(); if err != nil { return err }`, accessUnclassified},
		{"unsupported_switch", `_, err := c.getConstituent("a"); switch { case err != nil: return err }`, accessUnclassified},
		{"forwarded_results", `return c.getConstituent("a")`, accessPropagates},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", "package fixture; func check() {"+tc.source+"}", 0)
			if err != nil {
				t.Fatal(err)
			}
			function := file.Decls[0].(*ast.FuncDecl)
			sites, seen := scanConstituentAccessBlock(fset, function.Body.List, "fixture.go", function.Name.Name, constituentAccessMethods)
			if seen != 1 || len(sites) != 1 {
				t.Fatalf("want one classified access site, got %d / %v", seen, sites)
			}
			if sites[0].class != tc.want {
				t.Fatalf("access error handling = %s, want %s", sites[0].class, tc.want)
			}
		})
	}
}
