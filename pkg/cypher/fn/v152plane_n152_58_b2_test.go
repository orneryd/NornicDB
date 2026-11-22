package fn

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

// NOR-187 reproduction, owner layer. LowerCaseText is the one canonical owner
// behind lower() and toLower(); this test calls it directly so a boundary
// failure is attributed to that owner and not to expression plumbing. The
// error this issue records is a truncated observation: the last character was
// never inspected. Every row therefore compares the complete output string and
// the final rune separately.
//
// Expected values are the Unicode default lowercase mappings (SpecialCasing.txt
// Final_Sigma context: a capital sigma is final when it is preceded by a cased
// letter and then zero or more case-ignorable characters, and is not followed
// by zero or more case-ignorable characters and then a cased letter). The
// NornicDB product contract is Neo4j's Java/Unicode contract; those reference
// readings have not yet been recorded for these rows (the opt-in oracle lives
// with the public-path test in pkg/cypher). Rows here are the Unicode contract,
// not a claim that Neo4j was executed.
func TestCaseOwnerFinalSigmaBoundary(t *testing.T) {
	for _, row := range nor187PortedFinalSigmaRows() {
		t.Run(row.name, func(t *testing.T) {
			got := LowerCaseText(row.input)
			assert.Equal(t, row.want, got,
				"complete lowercase output: input %d bytes/%d runes, got %d bytes/%d runes",
				len(row.input), utf8.RuneCountInString(row.input), len(got), utf8.RuneCountInString(got))
			wantLast, _ := utf8.DecodeLastRuneInString(row.want)
			gotLast, gotLastSize := utf8.DecodeLastRuneInString(got)
			assert.Equal(t, wantLast, gotLast,
				"final rune U+%04X, want U+%04X (got last %d bytes % X)",
				gotLast, wantLast, gotLastSize, got[len(got)-gotLastSize:])
			assert.Equal(t, utf8.RuneCountInString(row.want), utf8.RuneCountInString(got),
				"rune count: truncation is the historical misreading, not the contract")
			assert.Equal(t, len(row.want), len(got), "byte length")
		})
	}
}

// nor187PortedRow is one exact input string and its exact Unicode lowercase output.
type nor187PortedRow struct {
	name  string
	input string
	want  string
}

// nor187PortedFinalSigmaRows returns the reproduction matrix: the recorded historical
// boundary, adjacent lengths on both sides of the 128-byte transform chunk,
// long combining-acute runs, the 30-mark forward-lookahead limit, U+0345
// cased/case-ignorable precedence, short cased and uncased left/right context
// controls, and empty/non-Greek preservation. Inputs are exact strings; long
// cases use counts small enough to be deterministic (127/128/257 marks), not
// the 65,537-mark class reported in the design note.
func nor187PortedFinalSigmaRows() []nor187PortedRow {
	const (
		sigma         = "\u03A3" // GREEK CAPITAL LETTER SIGMA
		finalSigma    = "\u03C2" // GREEK SMALL LETTER FINAL SIGMA
		medialSigma   = "\u03C3" // GREEK SMALL LETTER SIGMA
		acute         = "\u0301" // COMBINING ACUTE ACCENT (case-ignorable)
		ypogegrammeni = "\u0345" // COMBINING GREEK YPOGEGRAMMENI (cased + case-ignorable)
	)
	reps := func(text string, count int) string { return strings.Repeat(text, count) }

	rows := []nor187PortedRow{
		// The exact historical case: 127 ASCII letters followed by a capital
		// sigma, expected to end in final sigma.
		{name: "ascii127+Sigma_historical", input: reps("a", 127) + sigma, want: reps("a", 127) + finalSigma},

		// U+0345 is both cased and case-ignorable. Unicode Final_Sigma treats a
		// preceding cased character as qualifying, so sigma is final in both.
		{name: "U0345+Sigma", input: ypogegrammeni + sigma, want: ypogegrammeni + finalSigma},
		{name: "A+U0345+Sigma", input: "A" + ypogegrammeni + sigma, want: "a" + ypogegrammeni + finalSigma},

		// Short cased left/right context controls: cased before sigma makes it
		// final at end of input or before an uncased non-ignorable character;
		// a cased character after sigma makes it medial.
		{name: "a+Sigma", input: "a" + sigma, want: "a" + finalSigma},
		{name: "A+Sigma", input: "A" + sigma, want: "a" + finalSigma},
		{name: "Sigma+a", input: sigma + "a", want: medialSigma + "a"},
		{name: "Sigma+A", input: sigma + "A", want: medialSigma + "a"},
		{name: "A+Sigma+space", input: "A" + sigma + " ", want: "a" + finalSigma + " "},
		{name: "A+Sigma+digit", input: "A" + sigma + "1", want: "a" + finalSigma + "1"},
		{name: "A+Sigma+period", input: "A" + sigma + ".", want: "a" + finalSigma + "."},
		{name: "A+Sigma+acute", input: "A" + sigma + acute, want: "a" + finalSigma + acute},

		// Uncased left context controls: no preceding cased character, so
		// sigma is medial regardless of what follows.
		{name: "Sigma_alone", input: sigma, want: medialSigma},
		{name: "space+Sigma", input: " " + sigma, want: " " + medialSigma},
		{name: "digit+Sigma", input: "1" + sigma, want: "1" + medialSigma},
		{name: "hiragana+Sigma", input: "\u3042" + sigma, want: "\u3042" + medialSigma},
		{name: "A+space+Sigma", input: "A " + sigma, want: "a " + medialSigma},

		// Two sigmas: the first has no cased left context, the second does.
		{name: "Sigma+Sigma", input: sigma + sigma, want: medialSigma + finalSigma},
		{name: "Alpha+Sigma", input: "\u0391" + sigma, want: "\u03B1" + finalSigma},
		{name: "Alpha+Sigma+Alpha", input: "\u0391" + sigma + "\u0391", want: "\u03B1" + medialSigma + "\u03B1"},

		// Empty and non-Greek Unicode: existing correct behavior is retained.
		{name: "empty", input: "", want: ""},
		{name: "ascii", input: "ABC", want: "abc"},
		{name: "german_sharp_s", input: "Stra\u00DFe", want: "stra\u00DFe"},
		{name: "dotted_capital_I", input: "\u0130", want: "i\u0307"},
		{name: "title_digraph", input: "\u01C5", want: "\u01C6"},
		{name: "roman_numeral", input: "\u2167", want: "\u2177"},
		{name: "greek_caps", input: "\u0391\u0392\u0393\u0394", want: "\u03B1\u03B2\u03B3\u03B4"},
		{name: "cyrillic", input: "\u041F\u0420\u0418\u0412\u0415\u0422", want: "\u043F\u0440\u0438\u0432\u0435\u0442"},
		{name: "cjk", input: "\u65E5\u672C\u8A9E", want: "\u65E5\u672C\u8A9E"},
		{name: "emoji", input: "\U0001F642", want: "\U0001F642"},
	}

	// Adjacent ASCII lengths: 125-130 and 255-257 bytes of prefix plus the
	// two-byte sigma straddle the 128-byte first transform chunk.
	for _, length := range []int{125, 126, 127, 128, 129, 130, 255, 256, 257} {
		prefix := reps("a", length)
		rows = append(rows, nor187PortedRow{
			name:  fmt.Sprintf("ascii%d+Sigma", length),
			input: prefix + sigma,
			want:  prefix + finalSigma,
		})
	}

	// A + combining acute run + sigma across the 128-byte chunk. 'A' is the
	// only cased character before sigma, and sigma ends the input, so every
	// row is final sigma no matter how the chunk boundary falls inside the run.
	for _, marks := range []int{0, 1, 30, 31, 32, 63, 64, 127, 128, 257} {
		rows = append(rows, nor187PortedRow{
			name:  fmt.Sprintf("A+acute%d+Sigma", marks),
			input: "A" + reps(acute, marks) + sigma,
			want:  "a" + reps(acute, marks) + finalSigma,
		})
	}

	// Forward context past the library's 30-mark lookahead: A-sigma, a run of
	// case-ignorable acute marks, then cased B. Unicode says sigma is medial
	// because a cased letter follows the ignorable run; the lookahead limit
	// must not silently commit final sigma. 30 marks is the control within the
	// limit; 31 and above cross it.
	for _, marks := range []int{30, 31, 32, 63, 255} {
		rows = append(rows, nor187PortedRow{
			name:  fmt.Sprintf("A+Sigma+acute%d+B", marks),
			input: "A" + sigma + reps(acute, marks) + "B",
			want:  "a" + medialSigma + reps(acute, marks) + "b",
		})
	}

	return rows
}
