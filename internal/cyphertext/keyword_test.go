package cyphertext

import "testing"

func TestKeywordEndGrammarBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text     string
		alphabet KeywordAlphabet
		want     int
	}{
		{"iN value", ASCIIWord, 2}, {"in_value", ASCIIWord, -1},
		{"IN$", ASCIIWord, 2}, {"IN$", CypherWord, -1},
		{"INé", ASCIIWord, 2}, {"INé", CypherWord, -1},
		{"IN\u00a0value", CypherWord, 2}, {"INSERT", CypherWord, -1},
	} {
		if got := KeywordEnd(tc.text, 0, "IN", tc.alphabet); got != tc.want {
			t.Errorf("%q/%d got %d want %d", tc.text, tc.alphabet, got, tc.want)
		}
	}
	if KeywordEnd("IN", -1, "IN", ASCIIWord) != -1 || KeywordEnd("I", 0, "IN", ASCIIWord) != -1 {
		t.Fatal("invalid extent matched")
	}
}
