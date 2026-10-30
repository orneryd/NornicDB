package storage

import "testing"

func TestCompareValues_SwitchAndFallbackBranches(t *testing.T) {
	cases := []struct {
		name string
		a    interface{}
		b    interface{}
		want bool
	}{
		// Force switch branches (numeric-vs-nonnumeric bypasses numericConstraintValue fast path).
		{name: "int_vs_string", a: int(7), b: "7", want: false},
		{name: "int64_vs_bool", a: int64(7), b: true, want: false},
		{name: "float64_vs_slice", a: float64(1.5), b: []int{1, 5}, want: false},
		{name: "string_vs_int", a: "x", b: 1, want: false},
		{name: "bool_vs_string", a: false, b: "false", want: false},
		// Nil + default comparable/non-comparable fallback paths.
		{name: "nil_vs_nil", a: nil, b: nil, want: true},
		{name: "nil_vs_value", a: nil, b: "v", want: false},
		{name: "deep_equal_non_comparable_true", a: []int{1, 2}, b: []int{1, 2}, want: true},
		{name: "deep_equal_non_comparable_false", a: map[string]int{"x": 1}, b: map[string]int{"x": 2}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := compareValues(tc.a, tc.b); got != tc.want {
				t.Fatalf("compareValues(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
