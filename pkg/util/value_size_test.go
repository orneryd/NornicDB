package util

import "testing"

func TestEstimateValueBytes(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  int64
	}{
		{"nil", nil, 0},
		{"string", "abcd", 16 + 4},
		{"bytes", []byte{1, 2, 3}, 24 + 3},
		{"strings", []string{"ab", "c"}, 24 + 16*2 + 3},
		{"scalar", 42, 16},
		{"list", []any{"ab", 1}, 24 + 16*2 + (16 + 2) + 16},
		{"map", map[string]any{"k": "v"}, 48 + 32 + 1 + (16 + 1)},
		{"nested", map[string]any{"k": []any{"v"}}, 48 + 32 + 1 + (24 + 16 + (16 + 1))},
	}
	for _, tc := range cases {
		if got := EstimateValueBytes(tc.value); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}
