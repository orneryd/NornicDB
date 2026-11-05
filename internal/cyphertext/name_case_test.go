package cyphertext

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNameCaseOrdinarySemanticsAndExactUnits(t *testing.T) {
	ordinary := []string{"", "plain", "PLAIN", "Straße", "Σ", "σ", "ς", "İ", "i", "ſ", "S", "K", "K", "λ😀", `literal\uD800`, "\xff", "\x80", "\xed", "\xed\x9f", "a\xffZ", "\ufffd", "\ufffd\ufffd\ufffd"}
	for _, left := range ordinary {
		require.Equal(t, strings.ToLower(left), NameLower(left), "ordinary lower %q", left)
		for _, right := range ordinary {
			require.Equal(t, strings.EqualFold(left, right), NameEqualFold(left, right), "ordinary fold %q / %q", left, right)
		}
	}
	units := []string{"\xed\xa0\x80", "\xed\xa0\x81", "\xed\xb0\x80", "\xed\xbf\xbf"}
	runs := []string{"", "A", "a", "İ", "i", "Σ", "ς", "ſ", "S", `\uD800`, "x\xffZ"}
	for _, unit := range units {
		for _, prefix := range runs {
			for _, suffix := range runs {
				actual := prefix + unit + suffix
				require.Equal(t, strings.ToLower(prefix)+unit+strings.ToLower(suffix), NameLower(actual), "unit lower %q", actual)
			}
		}
		require.False(t, NameEqualFold(unit, `\ud800`))
		require.False(t, NameEqualFold(unit, "\ufffd"))
		require.False(t, NameEqualFold(unit, "\ufffd\ufffd\ufffd"))
		for _, other := range units {
			for _, left := range runs {
				for _, right := range runs {
					want := unit == other && strings.EqualFold(left, right)
					require.Equal(t, want, NameEqualFold(left+unit, right+other))
					require.Equal(t, want, NameEqualFold(unit+left, other+right))
					require.Equal(t, want, NameEqualFold("A"+unit+left, "a"+other+right))
					require.Equal(t, "a"+unit+other+"z", NameLower("A"+unit+other+"Z"))
				}
			}
		}
	}
	require.NotEqual(t, NameLower("ſ"), NameLower("S"), "lowercase keys retain their existing distinctions")
	require.True(t, NameEqualFold("ſ", "S"), "simple folding keeps its broader scalar contract")
	require.Equal(t, NameLower("İ"), NameLower("i"))
	require.False(t, NameEqualFold("İ", "i"))
}

func TestNameLowerUnchangedNamesDoNotAllocate(t *testing.T) {
	for _, name := range []string{"ordinary_name", "unit_\xed\xa0\x80", "\xed\xa0\x80\xed\xb0\x80"} {
		require.Zero(t, testing.AllocsPerRun(1000, func() {
			if NameLower(name) != name {
				panic("changed identity")
			}
		}), name)
	}
}
