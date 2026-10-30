package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchIndexSeek_TopLevelScannerAdditionalBranches(t *testing.T) {
	require.Equal(t, -1, topLevelSymbolIndex("`a<b`", "<"))
	require.Equal(t, -1, topLevelSymbolIndex("[a<b]", "<"))
	require.Equal(t, -1, topLevelSymbolIndex("{a<b}", "<"))
	require.Equal(t, 4, topLevelSymbolIndex("(a) < b", "<"))

	require.Equal(t, -1, topLevelEqualsIndex("a != b"))
	require.Equal(t, -1, topLevelEqualsIndex("a <= b"))
	require.Equal(t, -1, topLevelEqualsIndex("a >= b"))
	require.Equal(t, -1, topLevelEqualsIndex("`a=b`"))
	require.Equal(t, -1, topLevelEqualsIndex("{x:1, y:2}"))
	require.Equal(t, -1, topLevelEqualsIndex("[a=b]"))
	require.Equal(t, 2, topLevelEqualsIndex("a = 1 = b"))
}

func TestMatchIndexSeek_NullRewriteAdditionalBranches(t *testing.T) {
	require.Equal(t, "", tryRewriteNullNormalizedPredicate(""))
	require.Equal(t, "coalesce(n.flag, false) = false AND n.id = 1", tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) = false AND n.id = 1"))
	require.Equal(t, "n.flag = false", tryRewriteNullNormalizedPredicate("n.flag = false"))
	require.Equal(t, "coalesce(n.flag, false", tryRewriteNullNormalizedPredicate("coalesce(n.flag, false"))
	require.Equal(t, "coalesce(n.flag false) = false", tryRewriteNullNormalizedPredicate("coalesce(n.flag false) = false"))
	require.Equal(t, "coalesce(n.flag, ) = false", tryRewriteNullNormalizedPredicate("coalesce(n.flag, ) = false"))
	require.Equal(t, "coalesce(nflag, false) = false", tryRewriteNullNormalizedPredicate("coalesce(nflag, false) = false"))
	require.Equal(t, "coalesce(n.flag, false) > false", tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) > false"))
	require.Equal(t, "coalesce(n.flag, false) =", tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) = "))
	require.Equal(t, "coalesce(n.flag, false) = true", tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) = true"))

	require.Equal(
		t,
		"(n.flag <> false AND n.flag IS NOT NULL)",
		tryRewriteNullNormalizedPredicate("coalesce(n.flag, false) != false"),
	)
}

func TestMatchIndexSeek_CompareLiteralValuesDefaultBranch(t *testing.T) {
	ok, handled := compareLiteralValues(int64(1), int64(2), "??")
	require.False(t, handled)
	require.False(t, ok)

	ok, handled = compareLiteralValues(int64(2), int64(2), "<=")
	require.True(t, handled)
	require.True(t, ok)
}
