package cypher

import "testing"

// Pin the inferred source of the public nested-frame failure at both shared
// operand entrances, against the actual checker and the actual admitted scope.
func TestReviewer13e4NestedPrecisionOwner(t *testing.T) {
	scope := staticTypeScope{values: map[string]string{"x": "Boolean"}, params: map[string]interface{}{"p": true}, noLocalScopes: true}
	for _, c := range []struct{ text, want string }{
		{"[x]", "List<Boolean>"},
		{"[[x]]", "List<List<Boolean>>"},
		{"[[[x]]]", "List<List<List<Boolean>>>"},
		{"[$p]", "List<Boolean>"},
		{"[[$p]]", "List<List<Boolean>>"},
		{"[[true]]", "List<List<Boolean>>"},
	} {
		t.Run(c.text, func(t *testing.T) {
			checked, err := scope.checkOperand(c.text)
			if err != nil {
				t.Fatal(err)
			}
			signature := scope.staticExpressionOperand(c.text)
			t.Logf("literal=%s checked=%s signature=%s want=%s", staticLiteralTypeName(c.text), checked.typeName(), signature.typeName(), c.want)
			if checked.typeName() != c.want {
				t.Errorf("checker loses admitted type: %s", checked.typeName())
			}
			if signature.typeName() != c.want {
				t.Errorf("signature loses admitted type: %s", signature.typeName())
			}
		})
	}
}
