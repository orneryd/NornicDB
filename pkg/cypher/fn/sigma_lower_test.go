package fn

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func readSigmaFixture(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	data, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The frozen Java oracle decides only sigma. The existing general mapper
// remains the independent witness for every other byte, including expansion
// and admitted lone UTF16 units represented internally as WTF8.
func TestLowerCaseSigmaFrozenOracle(t *testing.T) {
	var rows []struct {
		Input string `json:"input"`
		Final []bool `json:"final"`
	}
	raw := readSigmaFixture(t, "testdata/sigma/neo4j-sigma-decisions.json.gz")
	if err := jsontext.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20801 {
		t.Fatalf("oracle rows=%d", len(rows))
	}
	for i, row := range rows {
		var expected strings.Builder
		ordinal, cursor := 0, 0
		for {
			relative := strings.Index(row.Input[cursor:], "Σ")
			if relative < 0 {
				break
			}
			at := cursor + relative
			expected.WriteString(cases.Lower(language.Und, cases.HandleFinalSigma(false)).String(row.Input[cursor:at]))
			if ordinal >= len(row.Final) {
				t.Fatalf("row%d: unexpected Sigma", i)
			}
			if row.Final[ordinal] {
				expected.WriteString("ς")
			} else {
				expected.WriteString("σ")
			}
			ordinal++
			cursor = at + len("Σ")
		}
		expected.WriteString(cases.Lower(language.Und, cases.HandleFinalSigma(false)).String(row.Input[cursor:]))
		if ordinal != len(row.Final) {
			t.Fatalf("row%d: sigma count%d want%d", i, ordinal, len(row.Final))
		}

		got := LowerCaseText(row.Input)
		if got != expected.String() {
			t.Errorf("row%d input%q: got%q want%q", i, row.Input, got, expected.String())
			return
		}
	}
}

// The bulky reflected snapshots are review artifacts, not product data.
// Set this path for the generator/profile verification gate.
func TestLowerCaseSigmaPinnedProfile(t *testing.T) {
	path := os.Getenv("N15258_SIGMA_PROFILE_DIR")
	if path == "" {
		t.Skip("set N15258_SIGMA_PROFILE_DIR to pinned external profile snapshots")
	}
	if unicode.Version != "17.0.0" {
		t.Fatalf("Unicode%s requires regenerating and reviewing the context delta", unicode.Version)
	}
	java := readSigmaFixture(t, filepath.Join(path, "java-context.bin.gz"))
	categories := readSigmaFixture(t, filepath.Join(path, "java-break-context.bin.gz"))
	if len(java) != 0x110000 || len(categories) != len(java) {
		t.Fatal("truncated profile")
	}
	kinds := map[byte]byte{1: sigmaEnclosing, 4: sigmaLetter, 12: sigmaNumber, 11: sigmaEnclosing, 255: sigmaIgnore, 5: sigmaDanda, 2: sigmaBothPunctuation, 8: sigmaBothPunctuation, 18: sigmaWordPunctuation, 21: sigmaWordPunctuation, 19: sigmaNumberPunctuation, 6: sigmaNumberSuffix}
	for n := range java {
		r := rune(n)
		want := kinds[categories[n]]
		// Additional cased ranges are the pinned JDK21 ConditionalSpecialCasing
		// profile, distinct from its Character category bits captured in java.
		if java[n]&64 != 0 || r >= 0x2b0 && r <= 0x2b8 || r >= 0x2c0 && r <= 0x2c1 || r >= 0x2e0 && r <= 0x2e4 || r == 0x345 || r == 0x37a || r >= 0x1d2c && r <= 0x1d61 || r >= 0x2160 && r <= 0x217f || r >= 0x24b6 && r <= 0x24e9 {
			want |= sigmaCased
		}
		if got := sigmaContext(r); got != want {
			t.Fatalf("U+%04X context%d want%d", n, got, want)
		}
	}
}

func TestLowerCaseSigmaNoSigmaPreservesMapper(t *testing.T) {
	for _, s := range []string{"", "ABC", "İßǅ", "\xed\xa0\x80A", "\xffA", "\uffffA", strings.Repeat("A", 4096)} {
		if got, want := LowerCaseText(s), cases.Lower(language.Und).String(s); got != want {
			t.Errorf("input%q got%q want%q", s, got, want)
		}
	}
}

func TestLowerCaseSigmaProfileProvenance(t *testing.T) {
	var profile struct {
		Java      string `json:"java"`
		GoUnicode string `json:"go_unicode"`
	}
	data, err := os.ReadFile("testdata/sigma/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &profile); err != nil {
		t.Fatal(err)
	}
	if profile.Java != "21.0.12+8-LTS" || profile.GoUnicode != unicode.Version {
		t.Fatalf("profile drift: %+v runtimeUnicode%s", profile, unicode.Version)
	}
}
