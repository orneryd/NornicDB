package testfixture

import (
	"go/parser"
	"testing"
)

func TestReceiverNamePolicies(t *testing.T) {
	for _, tc := range []struct {
		source, declaration, base string
	}{
		{"Store", "Store", "Store"},
		{"*Store", "*Store", "Store"},
		{"**Store", "**Store", "Store"},
		{"Box[T]", "Box", "Box"},
		{"*Box[K, V]", "*Box", "Box"},
		{"*[2]int", "*", ""},
		{"[]int", "", ""},
	} {
		t.Run(tc.source, func(t *testing.T) {
			expression, err := parser.ParseExpr(tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if got := ReceiverName(expression, ReceiverWithPointers); got != tc.declaration {
				t.Fatalf("declaration identity = %q, want %q", got, tc.declaration)
			}
			if got := ReceiverName(expression, ReceiverBaseName); got != tc.base {
				t.Fatalf("base identity = %q, want %q", got, tc.base)
			}
		})
	}
	if ReceiverName(nil, ReceiverWithPointers) != "" {
		t.Fatal("nil syntax must have no identity")
	}
}
