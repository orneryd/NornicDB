package cypher

import (
	"regexp"
	"strings"
	"testing"
)

// redactionWordRE matches every bare word or number in a redaction output.
var redactionWordRE = regexp.MustCompile(`[A-Za-z0-9_]+`)

// TestRedactLiterals_InvalidStatementsNeverEmitInputWords covers statements
// that lex cleanly, keep their brackets balanced by count, and still fail the
// grammar. For each one, no bare word or literal from the input may appear in
// the output, except words on the allow list (Cypher keywords, which the lexer
// tokenizes on their own, and the placeholder itself).
func TestRedactLiterals_InvalidStatementsNeverEmitInputWords(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		secret []string
	}{
		{"bare_word_then_comma", `MATCH (n {name: John, Secret}) RETURN n`, []string{"John", "Secret"}},
		{"bare_word_then_keyword", `MATCH (n {pw: hunter2 AND}) RETURN n`, []string{"hunter2"}},
		{"mismatched_bracket_types", `MATCH (n {pw: Secret) RETURN n}`, []string{"Secret"}},
		{"truncated_where", `MATCH (n) WHERE n.pw = hunter2 RETURN`, []string{"hunter2"}},
		{"semicolon_inside_map", `MATCH (n {pw: John; Secret}) RETURN n`, []string{"John", "Secret"}},
		{"bare_word_before_literal", `MATCH (n {name: John 'x'}) RETURN n`, []string{"John", "x"}},
		{"literal_before_bare_word", `MATCH (n {name: 'x' Secret}) RETURN n`, []string{"Secret", "x"}},
		{"dotted_bare_word_in_invalid_map", `MATCH (n {pw: John.Secret, }) RETURN n`, []string{"John", "Secret"}},
		{"comment_in_invalid_statement", `MATCH (n {pw: /* Secret */ x y}) RETURN n`, []string{"Secret"}},
		{"hex_in_invalid_statement", `MATCH (n {pw: 0xBEEF y z}) RETURN n`, []string{"0xBEEF", "BEEF"}},
	}
	keywords := map[string]bool{
		"MATCH": true, "RETURN": true, "WHERE": true, "AND": true,
		"REDACTED": true,
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := RedactLiterals(tc.in)
			for _, s := range tc.secret {
				if strings.Contains(out, s) {
					t.Fatalf("input word %q leaked: input=%q output=%q", s, tc.in, out)
				}
			}
			for _, w := range redactionWordRE.FindAllString(out, -1) {
				if !keywords[w] {
					t.Errorf("non-keyword word %q emitted: input=%q output=%q", w, tc.in, out)
				}
			}
		})
	}
}

// TestRedactLiterals_MismatchedBracketTypeFailsClosed checks that a closer of
// the wrong type fails the whole statement closed, even when the per-type
// counts would balance.
func TestRedactLiterals_MismatchedBracketTypeFailsClosed(t *testing.T) {
	for _, in := range []string{
		`MATCH (n {pw: Secret) RETURN n}`,
		`MATCH (n [pw: 'Secret')] RETURN n`,
		`MATCH (n) WHERE n.x IN [1, 2) RETURN n]`,
	} {
		if out := RedactLiterals(in); out != RedactedPlaceholder {
			t.Errorf("expected fail-closed %q for %q, got %q", RedactedPlaceholder, in, out)
		}
	}
}

// TestRedactLiterals_InvalidStatementKeepsStructure checks that a statement
// the grammar rejects still keeps its keywords, punctuation, labels inside a
// node or relationship pattern, and parameter names, while every identifier
// and literal is replaced.
func TestRedactLiterals_InvalidStatementKeepsStructure(t *testing.T) {
	in := `CALL { MATCH (n:Foo)-[:REL]->(m) WHERE n.name = "alice" RETURN n AS x UNION MATCH (m:Bar) RETURN m AS x } RETURN x LIMIT $limit`
	out := RedactLiterals(in)
	want := `CALL { MATCH (<REDACTED>:Foo)-[:REL]->(<REDACTED>) WHERE <REDACTED>.<REDACTED> = <REDACTED> RETURN <REDACTED> AS <REDACTED> UNION MATCH (<REDACTED>:Bar) RETURN <REDACTED> AS <REDACTED> } RETURN <REDACTED> LIMIT $limit`
	if out != want {
		t.Fatalf("structural redaction mismatch:\n got %q\nwant %q", out, want)
	}
}

// TestRedactLiterals_ValidStatementsKeepIdentifiers checks that statements the
// grammar accepts keep today's output: literals redacted, identifiers kept.
func TestRedactLiterals_ValidStatementsKeepIdentifiers(t *testing.T) {
	cases := []struct{ in, want string }{
		{`MATCH (n:Person {name: 'alice'}) RETURN n.name AS who`, `MATCH (n:Person {name: <REDACTED>}) RETURN n.name AS who`},
		{`CALL db.labels() YIELD label RETURN label`, `CALL db.labels() YIELD label RETURN label`},
		{`MATCH (n) RETURN n ORDER BY n.age DESC SKIP 5 LIMIT 10`, `MATCH (n) RETURN n ORDER BY n.age DESC SKIP <REDACTED> LIMIT <REDACTED>`},
		{`MATCH (n) WHERE n.name STARTS WITH 'a' RETURN n`, `MATCH (n) WHERE n.name STARTS WITH <REDACTED> RETURN n`},
		{"MATCH (n) RETURN n.x AS `alias with space`", "MATCH (n) RETURN n.x AS `alias with space`"},
		{`MERGE (n:P {id: 1}) ON CREATE SET n.c = 2 ON MATCH SET n.m = 3`, `MERGE (n:P {id: <REDACTED>}) ON CREATE SET n.c = <REDACTED> ON MATCH SET n.m = <REDACTED>`},
		{`RETURN apoc.coll.sum([1, 2]) AS s`, `RETURN apoc.coll.sum([<REDACTED>, <REDACTED>]) AS s`},
		{`CALL db.index.vector.queryNodes('idx', 5, $v) YIELD node, score RETURN node, score`, `CALL db.index.vector.queryNodes(<REDACTED>, <REDACTED>, $v) YIELD node, score RETURN node, score`},
	}
	for _, tc := range cases {
		if out := RedactLiterals(tc.in); out != tc.want {
			t.Errorf("input %q:\n got %q\nwant %q", tc.in, out, tc.want)
		}
	}
}

// TestRedactLiterals_ValidShapesDoNotCollapse checks that valid shapes the
// executor accepts, including ones the grammar rejects, never collapse to the
// bare placeholder and never leak a literal.
func TestRedactLiterals_ValidShapesDoNotCollapse(t *testing.T) {
	for _, in := range []string{
		`MATCH (n) WITH n AS m RETURN m`,
		`CALL db.labels() YIELD label, count RETURN label`,
		`MATCH (n) RETURN n ORDER BY n.x ASCENDING SKIP 1 LIMIT 2`,
		`MATCH (n) WHERE n.a ENDS WITH 'zz9' OR n.b CONTAINS 'zz8' RETURN n`,
		`MATCH (n) RETURN n {.name, .age, k: 'zz7'}`,
		`MATCH (n) RETURN n {.*}`,
		`RETURN apoc.periodic.iterate('zz6', 'zz5', {batchSize: 100})`,
		`MATCH (n) RETURN CASE WHEN n.a = 'zz4' THEN 1 ELSE 2 END`,
		`MATCH (n) WHERE COUNT { (n)--() } > 3 RETURN n`,
		`MATCH (n:A|B) RETURN n`,
		`CALL { MATCH (n:Foo) RETURN n UNION MATCH (n:Bar) RETURN n } RETURN n LIMIT $limit`,
	} {
		out := RedactLiterals(in)
		if out == RedactedPlaceholder {
			t.Errorf("valid shape collapsed: %q", in)
		}
		if strings.Contains(out, "zz") || strings.Contains(out, "100") {
			t.Errorf("literal leaked: input=%q output=%q", in, out)
		}
	}
}
