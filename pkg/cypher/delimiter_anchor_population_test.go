package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOpeningDelimiterAnchorPopulation(t *testing.T) {
	for _, test := range []struct {
		text        string
		anchor      int
		open, close rune
		want        int
	}{
		{"[x[y]]", 0, '[', ']', 5}, {"[x[y]]", 2, '[', ']', 4},
		{"[x[y]", 0, '[', ']', -1}, {"[x[y]", 2, '[', ']', 4},
		{"({x:[1]})", 0, '(', ')', 8}, {"({x:[1]})", 1, '{', '}', 7},
		{"`a``[b` [2]", 8, '[', ']', 10},
		{"([)]", 0, '(', ')', 2}, {"([)]", 1, '[', ']', 3},
		{"['ignored ]' /* ] */ [1]]", 0, '[', ']', 24},
		{"['unfinished", 0, '[', ']', -1}, {"[/*unfinished", 0, '[', ']', -1},
		{"[]", -1, '[', ']', -1}, {"[]", 2, '[', ']', -1}, {"[]", 1, '[', ']', -1},
	} {
		require.Equal(t, test.want, cyphertext.FindMatchingDelimiter(test.text, test.anchor, test.open, test.close, cyphertext.OpeningDelimiter), "%q at %d", test.text, test.anchor)
	}
}

func TestClosingDelimiterAnchorPopulation(t *testing.T) {
	for _, test := range []struct {
		text        string
		anchor      int
		open, close byte
		want        int
	}{
		{"[x[y]]", 4, '[', ']', 2}, {"[x[y]]", 5, '[', ']', 0},
		{"[x[y]", 4, '[', ']', -1}, {"[] [x]", 5, '[', ']', 3},
		{"({x:[1]})", 8, '(', ')', 0}, {"({x:[1]})", 7, '{', '}', 1},
		{"`a``[b` [2]", 10, '[', ']', 8},
		{"([)]", 2, '(', ')', 0}, {"([)]", 3, '[', ']', 1},
		{"['ignored ]' /* ] */ [1]]", 24, '[', ']', 0},
		{"'[]'", 2, '[', ']', -1}, {"/* [] */", 4, '[', ']', -1},
		{"[x[y] 'unfinished", 4, '[', ']', -1},
		{"[]", -1, '[', ']', -1}, {"[]", 2, '[', ']', -1}, {"[]", 0, '[', ']', -1},
	} {
		require.Equal(t, test.want, cyphertext.FindMatchingDelimiter(test.text, test.anchor, rune(test.open), rune(test.close), cyphertext.ClosingDelimiter), "%q at %d", test.text, test.anchor)
	}
}
