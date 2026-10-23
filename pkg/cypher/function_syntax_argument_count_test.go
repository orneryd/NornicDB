package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxArgumentCount(t *testing.T) {
	for _, text := range []string{
		"", " /* only trivia */ ", "'2020-01-01'", "'a,b'", "'a,\\'b'",
		"a,b", "a,", "a, /* no second argument */", ",a", "a,,",
		"coalesce(null,1)", "coalesce(null,1), [1,2]", "{a:1,b:2}, `x``y`",
		"a // don't count the ,\n, b", "'/* , */'", "coalesce(1,2",
	} {
		for _, prefix := range []string{"", "/* " + strings.Repeat("padding ", 2400) + " */ "} {
			text := prefix + text
			want := len(splitTopLevelComma(text))
			require.Equal(t, want, functionArgumentCount(text), text)
			require.Zero(t, testing.AllocsPerRun(10, func() {
				if got := functionArgumentCount(text); got != want {
					t.Fatalf("count=%d want=%d", got, want)
				}
			}), text)
		}
	}
}
