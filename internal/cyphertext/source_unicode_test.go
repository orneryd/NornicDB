package cyphertext

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnicodeSourceProjectionOnce(t *testing.T) {
	for _, tc := range []struct{ raw, accepted string }{
		{"RETURN {`a\\u0062`:1}", "RETURN {`ab`:1}"},
		{"RETURN {a\\u0062:1}", "RETURN {ab:1}"},
		{"RETURN {`a\\u005cu0062`:1}", "RETURN {`a\\u0062`:1}"},
		{"RETURN {`a\\\\u0062`:1}", "RETURN {`a\\\\u0062`:1}"},
		{"RETURN {`a\\uD83D\\uDE00`:1}", "RETURN {`a😀`:1}"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			source := NormalizeSource(tc.raw)
			require.Equal(t, tc.raw, source.Original)
			require.Equal(t, tc.accepted, source.Text)
		})
	}
}

func TestUnicodeSourceLiteralAndStoredNamePolicies(t *testing.T) {
	for _, name := range []string{"a\\u0062", "a\\\\u0062", "a`b", "π", string([]byte{0xed, 0xa0, 0x80}), string([]byte{0xed, 0xb0, 0x80})} {
		written := PrintIdentifier(name, nil, RawSource)
		source := NormalizeSource(written)
		require.NoError(t, source.Err)
		identity, ok := ScanIdentifier(source.Text, 0)
		require.True(t, ok)
		require.Equal(t, name, identity.Value, "stored identities must not be renamed")
	}
	for _, tc := range []struct{ raw, value string }{
		{"'\\u005cu0062'", "\\u0062"},
		{"'\\\\u0062'", "\\u0062"},
		{"'\\u0062'", "b"},
		{"'\\u005c\\u0062'", "\b"},
		{"'\\U00000062'", "b"},
	} {
		source := NormalizeSource(tc.raw)
		require.NoError(t, source.Err)
		value, ok := DecodeString(source.Text, AcceptedSource)
		require.True(t, ok)
		require.Equal(t, tc.value, value)
		rawValue, rawOK := DecodeString(tc.raw, RawSource)
		require.True(t, rawOK)
		require.Equal(t, tc.value, rawValue)
	}
	for _, text := range []string{"RETURN {`a\\uZZZZ`:1}", "RETURN {`a\\u006`:1}"} {
		require.Error(t, NormalizeSource(text).Err)
	}
	source := NormalizeSource("RETURN `a\\uD800`")
	require.NoError(t, source.Err)
	boundaries, err := source.CodepointBoundaries(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13}, boundaries)
}

func TestEmitRawSourceReversesUnicodeProjection(t *testing.T) {
	cases := []string{"RETURN 1", "RETURN {`a\\u0062`:1}", "RETURN 'literal\\u0062'", "RETURN '\\\\u0062'", "RETURN '\\\\\\u0062'", "// literal\\u0027\nRETURN 1", "/* \\u002a\\u002f */ RETURN 1", "RETURN `a``\\u0062`", "RETURN 'π😀'", "RETURN `a" + string([]byte{0xed, 0xa0, 0x80}) + "`"}
	for _, accepted := range cases {
		raw := EmitRawSource(accepted)
		projected := ProjectUnicode(t.Context(), raw)
		require.NoError(t, projected.Err)
		require.Equal(t, accepted, projected.Text, "raw emission must preserve accepted source exactly")
	}
}

func TestSourceStageRejectsUnknownEnum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, stage := range []SourceStage{RawSource, AcceptedSource} {
		source := NewSource(context.Background(), `RETURN 1`, stage)
		require.NoError(t, source.Err)
		require.Equal(t, `RETURN 1`, source.Text)
	}
	source := NewSource(ctx, "sensitive query", SourceStage(255))
	require.Error(t, source.Err)
	require.NotContains(t, source.Err.Error(), "sensitive")
}
