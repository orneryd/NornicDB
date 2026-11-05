package antlr

import (
	"context"
	"strings"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

func TestSourceStreamPublicStagesAndCancellation(t *testing.T) {
	for _, raw := range []string{`RETURN 1 AS value`, `RETURN '\u03bb' AS value`, `RETURN '\u005cu0062' AS value`, `RETURN '\ud800' AS value`, `RETURN 'λ😀' AS value`} {
		projected := cyphertext.NewSource(t.Context(), raw, cyphertext.RawSource)
		require.NoError(t, projected.Err)
		for _, stage := range []SourceStage{RawSource, AcceptedSource} {
			source := raw
			if stage == AcceptedSource {
				source = projected.Text
			}
			live, cancel := context.WithCancel(t.Context())
			tokens, err := LexContext(live, source, stage)
			require.NoError(t, err)
			var accepted strings.Builder
			for _, token := range tokens {
				accepted.WriteString(token.GetText())
			}
			require.Equal(t, projected.Text, accepted.String())
			require.NoError(t, ValidateContext(live, source, stage))
			require.NoError(t, live.Err())
			cancel()
			tokens, err = LexContext(live, source, stage)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, tokens)
			require.ErrorIs(t, ValidateContext(live, source, stage), context.Canceled)
		}
	}
}

func TestSourceStreamCoordinatesAndOriginalText(t *testing.T) {
	raw := `RETURN '\u03bb\ud800😀' AS value`
	source := cyphertext.NewSource(t.Context(), raw, cyphertext.RawSource)
	require.NoError(t, source.Err)
	core, input := sourceInputStream(t.Context(), source)
	require.NotNil(t, core.boundaries)
	require.Equal(t, source.Text, input.GetText(0, input.Size()-1))
	require.Equal(t, raw, core.OriginalText(0, input.Size()-1))
	for at := 0; at < input.Size(); at++ {
		expected, _ := cyphertext.ReadCodepoint(source.Text, core.boundaries[at])
		input.Seek(at)
		require.Equal(t, int(expected), input.LA(1))
	}
	input.Seek(input.Size())
	require.Equal(t, antlr.TokenEOF, input.LA(1))
	require.Equal(t, 0, input.LA(0))
	require.Empty(t, input.GetText(input.Size()+1, input.Size()+2))
	require.Empty(t, input.GetText(3, 1))
	require.Empty(t, core.OriginalText(3, 1))
	require.Panics(t, input.Consume)
}
