package cyphertext

import (
	"encoding/json"
	"os"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestFoundationNormalizationPopulation(t *testing.T) {
	owner := map[rune]bool{}
	for character := range syntaxConfusableReplacements {
		owner[character] = true
	}
	for _, table := range []map[rune]bool{ignorableFormatRunes, arrowDashRunes, arrowLeftRunes, arrowRightRunes} {
		for character, accepted := range table {
			if accepted {
				owner[character] = true
			}
		}
	}
	// Enumerate the actual whitespace owner, including Unicode table updates.
	for character := rune(0); character <= utf8.MaxRune; character++ {
		if IsWhitespace(character) {
			owner[character] = true
		}
	}
	data, err := os.ReadFile("testdata/normalization.json")
	require.NoError(t, err)
	var fixtures []struct{ Character, Input, Accepted string }
	require.NoError(t, json.Unmarshal(data, &fixtures))
	seen := map[rune]bool{}
	for _, fixture := range fixtures {
		characters := []rune(fixture.Character)
		require.Len(t, characters, 1)
		character := characters[0]
		require.True(t, owner[character], "fixture outside owner population: U%04X", character)
		require.False(t, seen[character], "duplicate fixture: U%04X", character)
		seen[character] = true
		require.Equal(t, fixture.Accepted, NormalizeSource(fixture.Input).Text)
	}
	for character := range owner {
		require.True(t, seen[character], "untested normalization class U%04X", character)
	}
	require.Len(t, fixtures, len(owner))
}
