package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
)

// Compile each mutation, then exercise the actual repository guard. These
// constructions never execute; this tests the guard's claimed boundary only.
func TestReviewer13e4GuardMutations(t *testing.T) {
	mutations := []struct {
		name, body string
		reject     bool
	}{
		{"literal", `func outside(s staticTypeScope){_ = staticOperatorChecker{scope:s}}`, true},
		{"alias_chain", `type one=staticOperatorChecker;type two=one;func outside(s staticTypeScope){_ = two{scope:s}}`, true},
		{"defined_type", `type other staticOperatorChecker;func outside(s staticTypeScope){_ = other{scope:s}}`, true},
		{"container_alias", `type many=map[string][]*staticOperatorChecker;func outside(s staticTypeScope){_ = many{"x":{{scope:s}}}}`, true},
		{"storage_executor_control", `type StorageExecutor struct{shellParams map[string]interface{}};func(StorageExecutor)validateStatement(){}`, false},
		{"named_result", `func outside()(checker staticOperatorChecker){return}`, true},
		{"array_zero", `func outside(){var a [1]staticOperatorChecker;_ = a[0]}`, true},
		{"parenthesized_new", `func outside(){_ = new((staticOperatorChecker))}`, true},
		{"conversion", `func outside(s staticTypeScope){_ = staticOperatorChecker(struct{scope staticTypeScope}{scope:s})}`, true},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			source := []byte("package fixture;type staticTypeScope struct{};type staticOperatorChecker struct{scope staticTypeScope};" + m.body)
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "outside.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (&types.Config{}).Check("fixture", set, []*ast.File{file}, nil); err != nil {
				t.Fatal("mutation must compile:", err)
			}
			violations, err := checkerConstructionViolations(map[string][]byte{"outside.go": source})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("COMPILES guardRejects=%v wantReject=%v body=%s", len(violations) > 0, m.reject, m.body)
			if (len(violations) > 0) != m.reject {
				t.Errorf("guard boundary mismatch: %v", violations)
			}
		})
	}
}
