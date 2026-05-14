package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitLegacyParamSyntax(t *testing.T) {
	// Standard "key value" form.
	k, v, ok := splitLegacyParamSyntax("key1 42")
	require.True(t, ok)
	require.Equal(t, "key1", k)
	require.Equal(t, "42", v)

	// Colon separator after the key gets stripped.
	k, v, ok = splitLegacyParamSyntax("key1: 42")
	require.True(t, ok)
	require.Equal(t, "key1:", k)
	require.Equal(t, "42", v)

	// Single field → not enough.
	_, _, ok = splitLegacyParamSyntax("onlyone")
	require.False(t, ok)

	// Empty value (only the key is present after stripping colon).
	_, _, ok = splitLegacyParamSyntax("key:")
	require.False(t, ok)

	// Quoted-key style with embedded quotes — normalizePropertyKey
	// should strip them; if the key normalizes to empty, fail.
	_, _, ok = splitLegacyParamSyntax(`"" 42`)
	require.False(t, ok)
}

func TestNormalizeParamMap_Variants(t *testing.T) {
	// map[string]interface{} → returned as-is.
	in1 := map[string]interface{}{"a": 1, "b": 2}
	got1, err := normalizeParamMap(in1)
	require.NoError(t, err)
	require.Equal(t, in1, got1)

	// map[interface{}]interface{} with string keys → normalized.
	in2 := map[interface{}]interface{}{"a": 1, "b": 2}
	got2, err := normalizeParamMap(in2)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"a": 1, "b": 2}, got2)

	// map[interface{}]interface{} with non-string key → error.
	in3 := map[interface{}]interface{}{42: "v"}
	_, err = normalizeParamMap(in3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "keys must be strings")

	// Wrong type entirely → error.
	_, err = normalizeParamMap("not a map")
	require.Error(t, err)
	require.Contains(t, err.Error(), "must evaluate to a map")
}

func TestTopLevelColonIndex_BoundaryCases(t *testing.T) {
	// Simple top-level colon.
	require.Equal(t, 3, topLevelColonIndex("foo:bar"))

	// Quotes hide a colon.
	require.Equal(t, 5, topLevelColonIndex(`"a:b":c`))
	require.Equal(t, 5, topLevelColonIndex(`'a:b':c`))

	// Colon inside braces is hidden by depth tracking; the only
	// top-level colon is the one outside the brace pair, at index 5.
	require.Equal(t, 5, topLevelColonIndex("{a:b}: c"))

	// Same for parens — only the colon at depth 0 (after the close
	// paren) counts. Index 6 is the close-paren-then-colon position.
	require.Equal(t, 6, topLevelColonIndex("(a: b): c"))

	// No top-level colon → -1.
	require.Equal(t, -1, topLevelColonIndex(`{"a":1}`))

	// Single-quote-inside-double doesn't toggle inSingle.
	require.Equal(t, 5, topLevelColonIndex(`"a'b":c`))
}
