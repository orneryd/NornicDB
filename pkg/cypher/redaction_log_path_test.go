package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
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
	forceSlowQueryLogging(t, exec)
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
			exec.emitSlowQueryLog(t.Context(), tc.in, nil, time.Second)
			exec.emitSlowQueryLog(t.Context(), tc.in, nil, time.Second)
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
	redactGrammarValidate = func(ctx context.Context, query string) error {
		*calls++
		return validate(ctx, query)
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
	exec.emitSlowQueryLog(t.Context(), query, nil, time.Second)
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

// TestSlowQueryLog_ReusesGrammarVerdict covers both first consumers of the
// exact-text verdict. With tracing disabled, ANTLR execution seeds it before
// logging. With tracing enabled, span projection precedes execution validation
// and seeds it once on either parser. Later spans and logs must reuse it.
// A canonical execution shape must never share a grammar verdict across literal text.
func TestSlowQueryLog_ReusesGrammarVerdict(t *testing.T) {
	const query = `MATCH (n:Person) WHERE n.name = 'zzs14' RETURN n.name AS who`
	const want = `MATCH (n:Person) WHERE n.name = <REDACTED> RETURN n.name AS who`
	for _, scenario := range []struct {
		name               string
		tracing, exactText bool
	}{
		{"tracing_disabled", false, false}, {"tracing_enabled", true, false}, {"exact_text", false, true},
	} {
		tracing := scenario.tracing
		t.Run(scenario.name, func(t *testing.T) {
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(noop.NewTracerProvider())
			defer otel.SetTracerProvider(previous)
			if tracing {
				_, restore := fixtureowner.RecordSpans(t)
				defer restore()
			}
			exec, logged := captureSlowQueryLog(t)
			calls := countRedactGrammarParses(t)
			if scenario.exactText {
				const first = "MATCH (n:Person) WHERE n.name = 'first' RETURN n.name"
				const second = "MATCH (n:Person) WHERE n.name = 'second' RETURN n.name"
				exec.emitSlowQueryLog(t.Context(), first, nil, time.Second)
				if exec.hasCachedValidSyntax(second, syntaxGrammarAccepted) {
					t.Fatal("grammar acceptance escaped the exact statement text")
				}
				exec.emitSlowQueryLog(t.Context(), second, nil, time.Second)
				exec.emitSlowQueryLog(t.Context(), second, nil, time.Second)
				if *calls != 2 {
					t.Fatalf("want one grammar parse per exact text, got %d", *calls)
				}
				for _, query := range logged() {
					if query != "MATCH (n:Person) WHERE n.name = <REDACTED> RETURN n.name" {
						t.Fatalf("unexpected log text: %q", query)
					}
				}
				return
			}
			if _, err := exec.Execute(t.Context(), query, nil); err != nil {
				t.Fatalf("execute: %v", err)
			}
			// Statement admission owns the grammar verdict on either parser, so
			// only a span redacted before admission parses for the redactor.
			wantParses := 0
			if tracing {
				wantParses = 1
			}
			if *calls != wantParses {
				t.Fatalf("first execution ran %d redactor grammar parses, want %d", *calls, wantParses)
			}
			if !exec.hasCachedValidSyntax(query, syntaxGrammarAccepted) {
				t.Fatal("first execution did not retain the exact-text grammar verdict")
			}
			exec.emitSlowQueryLog(t.Context(), query, nil, time.Second)
			if _, err := exec.Execute(t.Context(), query, nil); err != nil {
				t.Fatalf("repeat execute: %v", err)
			}
			if *calls != wantParses {
				t.Fatalf("repeated span or slow-query log parsed again: %d parses, want %d", *calls, wantParses)
			}
			queries := logged()
			if len(queries) != 3 {
				t.Fatalf("want 3 slow-query records, got %d", len(queries))
			}
			for i, q := range queries {
				if q != want {
					t.Errorf("record %d:\n got %q\nwant %q", i, q, want)
				}
			}
		})
	}
}

// TestSlowQueryLog_ExtensionStatementKeepsStructureOnly checks that a
// NornicDB extension statement, which statement admission marks valid
// without the grammar accepting it, is still redacted structurally on
// both parsers.
func TestSlowQueryLog_ExtensionStatementKeepsStructureOnly(t *testing.T) {
	query := "CREATE PROCEDURE zzproc.one() MODE READ AS RETURN 1 AS zzout"
	if !isNornicExtensionStatement(query) {
		t.Fatalf("%q is not an extension statement", query)
	}
	exec, logged := captureSlowQueryLog(t)
	exec.markCachedValidSyntax(query, syntaxStatementAccepted)
	exec.emitSlowQueryLog(t.Context(), query, nil, time.Second)
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
