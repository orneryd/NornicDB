package fn

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-lowercase-final-sigma).
// Test-side only. It logs the decisive values of the two mechanisms read in
// golang.org/x/text v0.44.0:
//  1. transform.String feeds the caser 128-byte chunks (initialBufSize), and
//     each Transform call restarts with isInterWord=true, so a Σ that starts a
//     later chunk has lost the preceding cased letter.
//  2. finalSigmaBody looks ahead at most maxIgnorable(30)+1 runes.
// The corrected variant transforms the whole input in one Transform call.

import (
	"strings"
	"testing"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func rcaLowerOneCall(t *testing.T, s string) string {
	t.Helper()
	c := cases.Lower(language.Und)
	dst := make([]byte, 4*len(s)+16)
	nDst, nSrc, err := c.Transform(dst, []byte(s), true)
	if err != nil || nSrc != len(s) {
		t.Fatalf("one-call transform: nSrc=%d len=%d err=%v", nSrc, len(s), err)
	}
	return string(dst[:nDst])
}

func rcaLastRune(s string) string {
	r := []rune(s)
	return string(r[len(r)-1])
}

func TestRCAFinalSigmaChunkBoundary(t *testing.T) {
	acute := "́"
	for _, tc := range []struct {
		name, in string
	}{
		{"ascii126+Sigma(128B)", strings.Repeat("a", 126) + "Σ"},
		{"ascii127+Sigma(129B)", strings.Repeat("a", 127) + "Σ"},
		{"ascii255+Sigma", strings.Repeat("a", 255) + "Σ"},
		{"A+acute62+Sigma(127B)", "A" + strings.Repeat(acute, 62) + "Σ"},
		{"A+acute63+Sigma(129B)", "A" + strings.Repeat(acute, 63) + "Σ"},
	} {
		lib := LowerCaseText(tc.in)
		one := rcaLowerOneCall(t, tc.in)
		t.Logf("RCA_SIGMA_CHUNK case=%s bytes=%d LowerCaseText_last=%q oneCallTransform_last=%q", tc.name, len(tc.in), rcaLastRune(lib), rcaLastRune(one))
	}
}

func TestRCAFinalSigmaLookaheadLimit(t *testing.T) {
	acute := "́"
	for _, n := range []int{29, 30, 31, 32} {
		in := "AΣ" + strings.Repeat(acute, n) + "B"
		lib := []rune(LowerCaseText(in))
		one := []rune(rcaLowerOneCall(t, in))
		t.Logf("RCA_SIGMA_LOOKAHEAD acutes=%d bytes=%d LowerCaseText_sigma=%q oneCallTransform_sigma=%q (expected σ: a cased letter follows)", n, len(in), string(lib[1]), string(one[1]))
	}
}
