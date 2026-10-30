package antlr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKnowledgePolicyNameReader(t *testing.T) {
	for _, tc := range []struct {
		source string
		start  int
		name   string
		end    int
	}{
		{"alpha rest", 0, "alpha", 5},
		{"'alpha beta' rest", 0, "alpha beta", 12},
		{`"alpha" rest`, 0, "alpha", 7},
		{`'a\u0062' rest`, 0, `a\u0062`, 9},
		{"'unterminated", 0, "", 0},
		{"'' rest", 0, "", 0},
		{"'bad\\q'", 0, "bad\\q", 7},
		{" rest", 0, "", 0},
		{"", 0, "", 0},
		{"x", 1, "", 1},
		{"x 'alpha'", 2, "alpha", 9},
	} {
		t.Run(tc.source, func(t *testing.T) {
			name, end := kpScanName(tc.source, tc.start)
			require.Equal(t, tc.name, name)
			require.Equal(t, tc.end, end)
		})
	}
}
