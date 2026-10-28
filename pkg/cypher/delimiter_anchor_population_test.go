package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDelimiterAnchorPopulations(t *testing.T) {
	type delimiterCase struct {
		text        string
		anchor      int
		open, close rune
		want        int
	}
	for _, population := range []struct {
		name  string
		cases []delimiterCase
		find  func(string, int, rune, rune) int
	}{
		{name: "TestOpeningDelimiterAnchorPopulation", find: func(text string, anchor int, open, close rune) int {
			return cyphertext.FindMatchingDelimiter(text, anchor, open, close, cyphertext.OpeningDelimiter)
		}, cases: []delimiterCase{
			{"[x[y]]", 0, '[', ']', 5}, {"[x[y]]", 2, '[', ']', 4},
			{"[x[y]", 0, '[', ']', -1}, {"[x[y]", 2, '[', ']', 4},
			{"({x:[1]})", 0, '(', ')', 8}, {"({x:[1]})", 1, '{', '}', 7},
			{"`a``[b` [2]", 8, '[', ']', 10},
			{"([)]", 0, '(', ')', 2}, {"([)]", 1, '[', ']', 3},
			{"['ignored ]' /* ] */ [1]]", 0, '[', ']', 24},
			{"['unfinished", 0, '[', ']', -1}, {"[/*unfinished", 0, '[', ']', -1},
			{"[]", -1, '[', ']', -1}, {"[]", 2, '[', ']', -1}, {"[]", 1, '[', ']', -1},
		}},
		{name: "TestClosingDelimiterAnchorPopulation", find: func(text string, anchor int, open, close rune) int {
			return cyphertext.FindMatchingDelimiter(text, anchor, rune(byte(open)), rune(byte(close)), cyphertext.ClosingDelimiter)
		}, cases: []delimiterCase{
			{"[x[y]]", 4, '[', ']', 2}, {"[x[y]]", 5, '[', ']', 0},
			{"[x[y]", 4, '[', ']', -1}, {"[] [x]", 5, '[', ']', 3},
			{"({x:[1]})", 8, '(', ')', 0}, {"({x:[1]})", 7, '{', '}', 1},
			{"`a``[b` [2]", 10, '[', ']', 8},
			{"([)]", 2, '(', ')', 0}, {"([)]", 3, '[', ']', 1},
			{"['ignored ]' /* ] */ [1]]", 24, '[', ']', 0},
			{"'[]'", 2, '[', ']', -1}, {"/* [] */", 4, '[', ']', -1},
			{"[x[y] 'unfinished", 4, '[', ']', -1},
			{"[]", -1, '[', ']', -1}, {"[]", 2, '[', ']', -1}, {"[]", 0, '[', ']', -1},
		}},
	} {
		t.Run(population.name, func(t *testing.T) {
			for _, test := range population.cases {
				require.Equal(t, test.want, population.find(test.text, test.anchor, test.open, test.close), "%q at %d", test.text, test.anchor)
			}
		})
	}
}
