package cyphertext

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUppercaseUnicodeLiteralStageContract(t *testing.T) {
	for _, stage := range []SourceStage{RawSource, AcceptedSource} {
		for _, tc := range []struct{ raw, want string }{{`'\U0001F600'`, "😀"}, {`'\U0010FFFF'`, "\U0010FFFF"}, {`'\U00000062'`, "b"}, {`'\\U0001F600'`, `\U0001F600`}} {
			got, ok := DecodeString(tc.raw, stage)
			require.True(t, ok, tc.raw)
			require.Equal(t, tc.want, got)
		}
		for _, raw := range []string{`'\U00110000'`, `'\UFFFFFFFF'`, `'\U0000D800'`, `'\U0000DC00'`, `'\U0000zzzz'`, `'\U001F60'`} {
			_, ok := DecodeString(raw, stage)
			require.False(t, ok, raw)
		}
	}
	// Lowercase-u source projection remains once-only; the literal decoder
	// must not reinterpret its generated lowercase-u spelling a second time.
	source := NormalizeSource(`'\u005cu0062'`)
	require.NoError(t, source.Err)
	got, ok := DecodeString(source.Text, AcceptedSource)
	require.True(t, ok)
	require.Equal(t, `\u0062`, got)
	// Uppercase-U is a literal escape, never a global symbolic-name rewrite.
	name := NormalizeSource("`a\\U00000062`")
	require.NoError(t, name.Err)
	require.Equal(t, "`a\\U00000062`", name.Text)
}
