package cypher

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// redactionLogCorpus pins what the slow-query log shows for statements with
// tricky literals. in is the statement as the client sent it; want is the
// log's query text (comments stripped, literals redacted).
var redactionLogCorpus = []struct {
	name string
	in   string
	want string
}{
	{
		"escaped_quotes",
		`MATCH (n:Person) WHERE n.name = 'O\'Brien zzs1' OR n.quote = "say \"hi zzs2\" \\" RETURN n.name AS who`,
		`MATCH (n:Person) WHERE n.name = <REDACTED> OR n.quote = <REDACTED> RETURN n.name AS who`,
	},
	{
		"backticks",
		"MATCH (`odd node`:`Odd Label` {`weird key`: 'zzs3'}) RETURN `odd node`.`a prop` AS `x``y`",
		"MATCH (`odd node`:`Odd Label` {`weird key`: <REDACTED>}) RETURN `odd node`.`a prop` AS `x``y`",
	},
	{
		"comments",
		"MATCH (n) /* block 'zzs4' */ WHERE n.url = 'http://zzs5.example/a' // line zzs6\nRETURN n",
		"MATCH (n)   WHERE n.url = <REDACTED> \nRETURN n",
	},
	{
		"multi_line_string",
		"CREATE (n:Note {body: 'line one zzs7\nline two zzs8', title: \"a\\nb zzs9\"}) RETURN n",
		"CREATE (n:Note {body: <REDACTED>, title: <REDACTED>}) RETURN n",
	},
	{
		"unicode",
		`MATCH (p:Città {nome: 'Zoë 東京 🎉 zzs10'}) WHERE p.größe > 1.5e3 RETURN p.größe AS größe`,
		`MATCH (p:Città {nome: <REDACTED>}) WHERE p.größe > <REDACTED> RETURN p.größe AS größe`,
	},
	{
		"parameters",
		`MATCH (n) WHERE n.id IN $ids AND n.v = $value RETURN n LIMIT $limit`,
		`MATCH (n) WHERE n.id IN $ids AND n.v = $value RETURN n LIMIT $limit`,
	},
	{
		"maps_and_numbers",
		`MERGE (n:Account {email: 'zzs11@example.com'}) SET n += {score: -42, ratio: .5, nested: {pin: 0xBEEF, code: 0o17}} RETURN n`,
		`MERGE (n:Account {email: <REDACTED>}) SET n += {score: -<REDACTED>, ratio: <REDACTED>, nested: {pin: <REDACTED>, code: <REDACTED>}} RETURN n`,
	},
	{
		"lists",
		`UNWIND [1, 2.5, 'zzs12', ["zzs13", [3]], 1e-3] AS v RETURN v[0] AS a, v[1..2] AS b`,
		`UNWIND [<REDACTED>, <REDACTED>, <REDACTED>, [<REDACTED>, [<REDACTED>]], <REDACTED>] AS v RETURN v[<REDACTED>] AS a, v[<REDACTED>..<REDACTED>] AS b`,
	},
}

// captureSlowQueryLog returns an executor whose slow-query records are
// collected, and a function returning the query text of each record.
func captureSlowQueryLog(t *testing.T) (*StorageExecutor, func() []string) {
	t.Helper()
	var buf bytes.Buffer
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	return exec, func() []string {
		var queries []string
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("decode log record %q: %v", line, err)
			}
			if rec["msg"] == "slow query" {
				queries = append(queries, rec["query"].(string))
			}
		}
		buf.Reset()
		return queries
	}
}

// TestSlowQueryLog_RedactionCorpus pins the logged text for the corpus on
// both parsers, through the log itself, the cached and uncached executor
// verdicts, and the package-level RedactLiterals.
func TestSlowQueryLog_RedactionCorpus(t *testing.T) {
	for _, tc := range redactionLogCorpus {
		t.Run(tc.name, func(t *testing.T) {
			exec, logged := captureSlowQueryLog(t)
			// The second emission finds the verdict the first one cached.
			exec.emitSlowQueryLog(tc.in, nil, time.Second)
			exec.emitSlowQueryLog(tc.in, nil, time.Second)
			got := logged()
			if len(got) != 2 {
				t.Fatalf("want 2 slow-query records, got %d", len(got))
			}
			for i, q := range got {
				if q != tc.want {
					t.Errorf("record %d:\n got %q\nwant %q", i, q, tc.want)
				}
			}
			if out := RedactLiterals(StripComments(tc.in)); out != tc.want {
				t.Errorf("RedactLiterals:\n got %q\nwant %q", out, tc.want)
			}
			if strings.Contains(strings.Join(got, " "), "zzs") {
				t.Errorf("literal leaked: %q", got)
			}
		})
	}
}

// countRedactGrammarParses replaces the redactor's grammar check with one
// that counts its calls.
func countRedactGrammarParses(t *testing.T) *int {
	t.Helper()
	calls := new(int)
	validate := redactGrammarValidate
	redactGrammarValidate = func(query string) error {
		*calls++
		return validate(query)
	}
	t.Cleanup(func() { redactGrammarValidate = validate })
	return calls
}

// TestSlowQueryLog_LargeStatementIsNotParsed checks that logging a statement
// above redactGrammarCheckMaxBytes runs no grammar parse, keeps only its
// structure, and leaks no literal.
func TestSlowQueryLog_LargeStatementIsNotParsed(t *testing.T) {
	query := loadSlowQueryLargeStatement(t)
	if len(query) <= redactGrammarCheckMaxBytes {
		t.Fatalf("fixture is %d bytes, not above the %d-byte bound", len(query), redactGrammarCheckMaxBytes)
	}
	calls := countRedactGrammarParses(t)
	exec, logged := captureSlowQueryLog(t)
	exec.emitSlowQueryLog(query, nil, time.Second)
	if *calls != 0 {
		t.Fatalf("slow-query log parsed the large statement %d times", *calls)
	}
	got := logged()
	if len(got) != 1 {
		t.Fatalf("want 1 slow-query record, got %d", len(got))
	}
	full := RedactLiterals(StripComments(query))
	if full == RedactedPlaceholder {
		t.Fatalf("large statement collapsed to the bare placeholder")
	}
	if want := truncateRuneSafe(full, 500); got[0] != want {
		t.Fatalf("logged text differs from RedactLiterals:\n got %q\nwant %q", got[0], want)
	}
	if strings.ContainsAny(full, `'"`) {
		t.Fatalf("a string literal survived redaction")
	}
	if !strings.Contains(full, "CALL") || !strings.Contains(full, RedactedPlaceholder) {
		t.Fatalf("structure lost: %.200q", full)
	}
	// Structure-only mode: the variables are redacted, label names stay.
	if strings.Contains(full, "eff_c") || !strings.Contains(full, ":KbCorrection") {
		t.Fatalf("large statement not redacted structurally: %.300q", full)
	}
}

// TestSlowQueryLog_ReusesGrammarVerdict checks that a statement the executor
// already found grammar-valid is not parsed again for the log: under the
// ANTLR parser Execute's validation supplies the verdict; under the default
// parser the first log record parses once and later ones reuse it. The
// logged text is the same on both parsers.
func TestSlowQueryLog_ReusesGrammarVerdict(t *testing.T) {
	const query = `MATCH (n:Person) WHERE n.name = 'zzs14' RETURN n.name AS who`
	const want = `MATCH (n:Person) WHERE n.name = <REDACTED> RETURN n.name AS who`
	exec, logged := captureSlowQueryLog(t)
	calls := countRedactGrammarParses(t)
	if _, err := exec.Execute(t.Context(), query, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	wantParses := 1
	if config.IsANTLRParser() {
		wantParses = 0
	}
	if *calls != wantParses {
		t.Fatalf("first slow-query record ran %d grammar parses, want %d", *calls, wantParses)
	}
	exec.emitSlowQueryLog(query, nil, time.Second)
	if *calls != wantParses {
		t.Fatalf("repeated slow-query record parsed again: %d parses, want %d", *calls, wantParses)
	}
	for i, q := range logged() {
		if q != want {
			t.Errorf("record %d:\n got %q\nwant %q", i, q, want)
		}
	}
}

// TestSlowQueryLog_ExtensionStatementKeepsStructureOnly checks that a
// NornicDB extension statement, which the ANTLR parser's validation marks
// valid without the grammar accepting it, is still redacted structurally on
// both parsers.
func TestSlowQueryLog_ExtensionStatementKeepsStructureOnly(t *testing.T) {
	query := "CREATE PROCEDURE zzproc.one() MODE READ AS RETURN 1 AS zzout"
	if !isNornicExtensionStatement(query) {
		t.Fatalf("%q is not an extension statement", query)
	}
	exec, logged := captureSlowQueryLog(t)
	exec.markCachedValidSyntax(query, syntaxValidANTLR)
	exec.emitSlowQueryLog(query, nil, time.Second)
	got := logged()
	if len(got) != 1 {
		t.Fatalf("want 1 slow-query record, got %d", len(got))
	}
	if want := RedactLiterals(query); got[0] != want {
		t.Fatalf("got %q, want %q", got[0], want)
	}
	if strings.Contains(got[0], "zzproc") || strings.Contains(got[0], "zzout") {
		t.Fatalf("extension statement not redacted structurally: %q", got[0])
	}
}

// TestRedactLiterals_LongValidStatementKeepsStructureOnly checks the bound:
// a grammar-valid statement just above redactGrammarCheckMaxBytes is redacted
// structurally, and one at the bound keeps its identifiers.
func TestRedactLiterals_LongValidStatementKeepsStructureOnly(t *testing.T) {
	build := func(size int) string {
		var b strings.Builder
		b.WriteString("MATCH (n:Person) WHERE n.name = 'zzs15'")
		for b.Len() < size-len(" RETURN n")-len(" OR n.k = 1") {
			b.WriteString(" OR n.k = 1")
		}
		for b.Len() < size-len(" RETURN n") {
			b.WriteString(" ")
		}
		b.WriteString(" RETURN n")
		return b.String()
	}
	atBound := build(redactGrammarCheckMaxBytes)
	if len(atBound) != redactGrammarCheckMaxBytes {
		t.Fatalf("built %d bytes, want %d", len(atBound), redactGrammarCheckMaxBytes)
	}
	if out := RedactLiterals(atBound); !strings.HasPrefix(out, "MATCH (n:Person) WHERE n.name = <REDACTED> OR n.k = <REDACTED>") {
		t.Fatalf("statement at the bound lost its identifiers: %.120q", out)
	}
	above := atBound + " "
	out := RedactLiterals(above)
	if !strings.HasPrefix(out, "MATCH (<REDACTED>:Person) WHERE <REDACTED>.<REDACTED> = <REDACTED> OR") {
		t.Fatalf("statement above the bound not redacted structurally: %.120q", out)
	}
	if strings.Contains(out, "zzs") {
		t.Fatalf("literal leaked: %.120q", out)
	}
}

// TestSlowQueryLog_ReusesGrammarVerdictForNormalizedText checks the same
// reuse for a statement that normalization rewrites (a comment, repeated
// whitespace, a trailing semicolon): the log redacts the canonical text the
// executor validated, so under the ANTLR parser it parses nothing.
func TestSlowQueryLog_ReusesGrammarVerdictForNormalizedText(t *testing.T) {
	if !config.IsCypherQueryNormalizationEnabled() {
		t.Skip("query normalization disabled")
	}
	const query = "MATCH (n:Person)   /* who */ WHERE n.name = 'zzs16'\n\tRETURN n.name AS who ;"
	exec, logged := captureSlowQueryLog(t)
	calls := countRedactGrammarParses(t)
	if _, err := exec.Execute(t.Context(), query, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	wantParses := 1
	if config.IsANTLRParser() {
		wantParses = 0
	}
	if *calls != wantParses {
		t.Fatalf("slow-query record ran %d grammar parses, want %d", *calls, wantParses)
	}
	got := logged()
	if len(got) != 1 {
		t.Fatalf("want 1 slow-query record, got %d", len(got))
	}
	if strings.Contains(got[0], "zzs16") || strings.Contains(got[0], "who */") {
		t.Fatalf("literal or comment leaked: %q", got[0])
	}
	if !strings.Contains(got[0], "MATCH (n:Person)") || !strings.Contains(got[0], "RETURN n.name AS who") {
		t.Fatalf("statement structure lost: %q", got[0])
	}
}
