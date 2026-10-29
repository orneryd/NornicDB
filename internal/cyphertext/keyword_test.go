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

// TestKeywordEndLexicalSpans ports the incoming identity cases onto the
// canonical KeywordEnd/KeywordAlphabet owner: an ASCII-only match, no Unicode
// folding, and the exact right boundary of the consuming alphabet over a byte
// span that must itself be valid. Whole-token checks also apply the caller's
// left boundary through KeywordBoundary; KeywordEnd owns the right boundary.
func TestKeywordEndLexicalSpans(t *testing.T) {
	for _, text := range []string{"SKIP", "skip", "SkIp"} {
		if KeywordEnd(text, 0, "SKIP", ASCIIWord) != len(text) {
			t.Errorf("ASCII keyword %q was not recognized", text)
		}
	}
	for _, text := range []string{"\u017fKIP", "S\u212aIP", "SKIPx", "xSKIP", "`SKIP`", "123", ""} {
		if KeywordEnd(text, 0, "SKIP", ASCIIWord) != -1 {
			t.Errorf("non-keyword %q acquired SKIP identity", text)
		}
	}
	if KeywordEnd("S\u212aIP", 0, "SKIP", ASCIIWord) != -1 {
		t.Error("Unicode folding acquired SKIP identity")
	}
	if KeywordEnd(" SkIp\t", 1, "skip", ASCIIWord) != 5 {
		t.Error("complete ASCII identifier extent was rejected")
	}
	if end := KeywordEnd("xSKIP", 1, "SKIP", ASCIIWord); end >= 0 && KeywordBoundary("xSKIP", 1, end) {
		t.Error("identifier suffix acquired keyword identity")
	}
	if KeywordEnd("SKIPx", 0, "SKIP", ASCIIWord) != -1 {
		t.Error("identifier prefix acquired keyword identity")
	}
	if end := KeywordEnd("$SKIP", 1, "SKIP", ASCIIWord); end >= 0 && KeywordBoundary("$SKIP", 1, end) {
		t.Error("parameter-name suffix acquired keyword identity")
	}
	if KeywordEnd("SKIP", -1, "SKIP", ASCIIWord) != -1 || KeywordEnd("SKIP", 0, "SKIP", CypherWord) != 4 {
		t.Error("invalid lexical span was accepted")
	}
}
