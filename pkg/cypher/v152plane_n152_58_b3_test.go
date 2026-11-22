package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// n15258b3nor187PublicQuery is the historical reproduction shape: the exact boundary
// case is passed as a parameter (repeat() is not a function on the recorded
// commits), the complete lowercase value is returned, and the final character
// is returned separately. The historical "FIXED" reading was wrong because the
// observation was truncated, so every row checks both projections.
const n15258b3nor187PublicQuery = "RETURN toLower($s) AS lowered, right(toLower($s), 1) AS last"

// TestN15258b3PortedPublicToLowerFinalSigmaBoundary reproduces NOR-187 on the public query
// path of both parsers (default and ANTLR). Parameters carry the exact input
// strings; expected values are the Unicode default lowercase contract
// (SpecialCasing.txt Final_Sigma), not a recorded Neo4j run. The live Neo4j
// reference comparison is opted into separately through
// NORNICDB_NEO4J_REFERENCE_URI in TestN15258b3PortedPublicToLowerFinalSigmaNeo4jReference.
func TestN15258b3PortedPublicToLowerFinalSigmaBoundary(t *testing.T) {
	rows := n15258b3nor187ContractRows()
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "n15258b3nor187_public"))
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				params := map[string]interface{}{"s": row.input}
				result, err := exec.Execute(context.Background(), n15258b3nor187PublicQuery, params)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 2)
				lowered, ok := result.Rows[0][0].(string)
				require.True(t, ok, "lowered projection is a string")
				last, ok := result.Rows[0][1].(string)
				require.True(t, ok, "last projection is a string")

				assert.Equal(t, row.want, lowered,
					"complete public toLower output: input %d bytes/%d runes, got %d bytes/%d runes",
					len(row.input), utf8.RuneCountInString(row.input), len(lowered), utf8.RuneCountInString(lowered))
				assert.Equal(t, n15258b3nor187WantLastRune(row.want), last, "public final rune")
				assert.Equal(t, utf8.RuneCountInString(row.want), utf8.RuneCountInString(lowered),
					"rune count: truncation is the historical misreading, not the contract")
			})
		}
	})
}

// TestN15258b3PortedPublicToLowerFinalSigmaNeo4jReference compares the same exact inputs with
// the supported Neo4j reference instance when NORNICDB_NEO4J_REFERENCE_URI is
// set. It is skipped by default: this test never creates a network connection on
// its own and never starts Docker or SSH. The reference version is logged, not
// pinned here; the parent supplies and owns the reference target. A reference
// disagreement with the Unicode contract is reported as a failure, not hidden.
func TestN15258b3PortedPublicToLowerFinalSigmaNeo4jReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to compare the NOR-187 casing contract with the live Neo4j reference")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	components, err := session.Run(ctx, "CALL dbms.components() YIELD versions RETURN versions", nil)
	require.NoError(t, err)
	records, err := components.Collect(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, records)
	t.Logf("Neo4j reference versions: %v", records[0].Values)

	for _, row := range n15258b3nor187ContractRows() {
		t.Run(row.name, func(t *testing.T) {
			result, err := session.Run(ctx, n15258b3nor187PublicQuery, map[string]any{"s": row.input})
			require.NoError(t, err)
			record, err := result.Single(ctx)
			require.NoError(t, err)
			require.Len(t, record.Values, 2)
			assert.Equal(t, row.want, record.Values[0], "Neo4j reference complete lowercase output")
			assert.Equal(t, n15258b3nor187WantLastRune(row.want), record.Values[1], "Neo4j reference final rune")
		})
	}
}

// n15258b3nor187ContractRow is one exact input string and its exact Unicode lowercase
// output.
type n15258b3nor187ContractRow struct {
	name  string
	input string
	want  string
}

// n15258b3nor187WantLastRune is the expected right(value, 1): the final rune of the
// complete expected value, or the empty string for empty input.
func n15258b3nor187WantLastRune(want string) string {
	if want == "" {
		return ""
	}
	last, _ := utf8.DecodeLastRuneInString(want)
	return string(last)
}

// n15258b3nor187ContractRows is the exact reproduction matrix shared by the public
// parser tests and the opt-in Neo4j reference: the recorded boundary, adjacent
// lengths around the 128-byte transform chunk, combining-acute runs, the
// 30-mark forward lookahead limit, U+0345 precedence, short cased/uncased
// context controls, and empty/non-Greek preservation. Expectations are the
// Unicode contract; they are not a claim that Neo4j was executed.
func n15258b3nor187ContractRows() []n15258b3nor187ContractRow {
	const (
		sigma         = "\u03A3" // GREEK CAPITAL LETTER SIGMA
		finalSigma    = "\u03C2" // GREEK SMALL LETTER FINAL SIGMA
		medialSigma   = "\u03C3" // GREEK SMALL LETTER SIGMA
		acute         = "\u0301" // COMBINING ACUTE ACCENT (case-ignorable)
		ypogegrammeni = "\u0345" // COMBINING GREEK YPOGEGRAMMENI (cased + case-ignorable)
	)
	reps := func(text string, count int) string { return strings.Repeat(text, count) }

	rows := []n15258b3nor187ContractRow{
		{name: "ascii127+Sigma_historical", input: reps("a", 127) + sigma, want: reps("a", 127) + finalSigma},

		{name: "U0345+Sigma", input: ypogegrammeni + sigma, want: ypogegrammeni + finalSigma},
		{name: "A+U0345+Sigma", input: "A" + ypogegrammeni + sigma, want: "a" + ypogegrammeni + finalSigma},

		{name: "a+Sigma", input: "a" + sigma, want: "a" + finalSigma},
		{name: "A+Sigma", input: "A" + sigma, want: "a" + finalSigma},
		{name: "Sigma+a", input: sigma + "a", want: medialSigma + "a"},
		{name: "Sigma+A", input: sigma + "A", want: medialSigma + "a"},
		{name: "A+Sigma+space", input: "A" + sigma + " ", want: "a" + finalSigma + " "},
		{name: "A+Sigma+digit", input: "A" + sigma + "1", want: "a" + finalSigma + "1"},
		{name: "A+Sigma+period", input: "A" + sigma + ".", want: "a" + finalSigma + "."},
		{name: "A+Sigma+acute", input: "A" + sigma + acute, want: "a" + finalSigma + acute},

		{name: "Sigma_alone", input: sigma, want: medialSigma},
		{name: "space+Sigma", input: " " + sigma, want: " " + medialSigma},
		{name: "digit+Sigma", input: "1" + sigma, want: "1" + medialSigma},
		{name: "hiragana+Sigma", input: "\u3042" + sigma, want: "\u3042" + medialSigma},
		{name: "A+space+Sigma", input: "A " + sigma, want: "a " + medialSigma},

		{name: "Sigma+Sigma", input: sigma + sigma, want: medialSigma + finalSigma},
		{name: "Alpha+Sigma", input: "\u0391" + sigma, want: "\u03B1" + finalSigma},
		{name: "Alpha+Sigma+Alpha", input: "\u0391" + sigma + "\u0391", want: "\u03B1" + medialSigma + "\u03B1"},

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

	for _, length := range []int{125, 126, 127, 128, 129, 130, 255, 256, 257} {
		prefix := reps("a", length)
		rows = append(rows, n15258b3nor187ContractRow{
			name:  fmt.Sprintf("ascii%d+Sigma", length),
			input: prefix + sigma,
			want:  prefix + finalSigma,
		})
	}

	for _, marks := range []int{0, 1, 30, 31, 32, 63, 64, 127, 128, 257} {
		rows = append(rows, n15258b3nor187ContractRow{
			name:  fmt.Sprintf("A+acute%d+Sigma", marks),
			input: "A" + reps(acute, marks) + sigma,
			want:  "a" + reps(acute, marks) + finalSigma,
		})
	}

	for _, marks := range []int{30, 31, 32, 63, 255} {
		rows = append(rows, n15258b3nor187ContractRow{
			name:  fmt.Sprintf("A+Sigma+acute%d+B", marks),
			input: "A" + sigma + reps(acute, marks) + "B",
			want:  "a" + medialSigma + reps(acute, marks) + "b",
		})
	}

	return rows
}
