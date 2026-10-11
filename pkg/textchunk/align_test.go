package textchunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Align places chunks in the text they were cut from: verbatim tilings
// (Voyage's contextualized chunks keep the text's whitespace), overlapping
// trimmed chunks (NornicDB's chunker), repeated text, chunks whose
// whitespace a chunker collapsed, and chunks that aren't in the text.
func TestAlignFindsChunksInOrder(t *testing.T) {
	text := "alpha beta gamma delta epsilon"
	require.Equal(t, []Span{{0, 11}, {11, 23}, {23, 30}}, Align(text, []string{"alpha beta ", "gamma delta ", "epsilon"}))
	require.Equal(t, []Span{{0, 16}, {11, 22}, {17, 30}}, Align(text, []string{"alpha beta gamma", "gamma delta", "delta epsilon"}))
	require.Equal(t, []Span{{0, 2}, {3, 5}, {6, 8}}, Align("ab ab ab", []string{"ab", "ab", "ab"}))
	require.Equal(t, []Span{{0, 15}}, Align("one  two\n\nthree", []string{"one two three"}))
	require.Equal(t, []Span{{0, 5}, {-1, -1}, {11, 16}}, Align(text, []string{"alpha", "zzz", "gamma"}))
	require.Equal(t, []Span{{-1, -1}, {0, 5}}, Align(text, []string{"  ", "alpha"}))
	require.Equal(t, []Span{{0, 7}, {8, 17}, {18, 23}}, Align("Straße 東京都 café", []string{"Straße", "東京都", "café"}))
	require.Equal(t, []Span{{8, 17}}, Align("Straße 東京都 café", []string{"東 京\n都"}))
	require.False(t, Span{-1, -1}.Found())
}
