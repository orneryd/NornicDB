// Wave-0 RED tests for cypher.RedactLiterals (D-04 ANTLR4 listener).
//
// These tests reference RedactLiterals + RedactedPlaceholder which do not yet
// exist; the package must fail to compile until the GREEN task ships
// pkg/cypher/redaction.go.
package cypher

import (
	"fmt"
	"strings"
	"testing"
)

// TestRedactLiterals_StringLiterals — string literals MUST be replaced; identifier
// 'n', property names 'name'/'password', and 'RETURN n' clause preserved.
func TestRedactLiterals_StringLiterals(t *testing.T) {
	in := `MATCH (n {name: "alice", password: "hunter2"}) RETURN n`
	out := RedactLiterals(in)
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected output to contain %q, got %q", RedactedPlaceholder, out)
	}
	if strings.Contains(out, "alice") {
		t.Fatalf("string literal 'alice' leaked: %q", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("string literal 'hunter2' leaked: %q", out)
	}
	for _, ident := range []string{"MATCH", "n", "name", "password", "RETURN"} {
		if !strings.Contains(out, ident) {
			t.Errorf("identifier/keyword %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_NumberLiterals — INTEGER + FLOAT redacted; identifiers preserved.
func TestRedactLiterals_NumberLiterals(t *testing.T) {
	in := `MATCH (n) WHERE n.age > 25 AND n.score >= 9.5 RETURN n`
	out := RedactLiterals(in)
	if strings.Contains(out, "25") {
		t.Fatalf("integer literal '25' leaked: %q", out)
	}
	if strings.Contains(out, "9.5") {
		t.Fatalf("float literal '9.5' leaked: %q", out)
	}
	for _, ident := range []string{"MATCH", "WHERE", "age", "score", "RETURN"} {
		if !strings.Contains(out, ident) {
			t.Errorf("identifier %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_PreservesIdentifiers — only the email string literal redacted.
func TestRedactLiterals_PreservesIdentifiers(t *testing.T) {
	in := `MATCH (alice:Person {email: "a@b.com"}) RETURN alice.email`
	out := RedactLiterals(in)
	if strings.Contains(out, "a@b.com") {
		t.Fatalf("string literal 'a@b.com' leaked: %q", out)
	}
	for _, ident := range []string{"alice", "Person", "email"} {
		if !strings.Contains(out, ident) {
			t.Errorf("identifier %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_PreservesParamNames — $id is a parameter REFERENCE, not a literal value.
func TestRedactLiterals_PreservesParamNames(t *testing.T) {
	in := `MATCH (n {id: $id}) RETURN n`
	out := RedactLiterals(in)
	if !strings.Contains(out, "$id") {
		t.Fatalf("parameter reference '$id' not preserved in %q", out)
	}
}

// TestRedactLiterals_PasswordHunter2 — LOG-08 acceptance criterion from ROADMAP Phase 2 SC #3.
func TestRedactLiterals_PasswordHunter2(t *testing.T) {
	in := `CREATE (u:User {password: "hunter2"})`
	out := RedactLiterals(in)
	if strings.Contains(out, "hunter2") {
		t.Fatalf("LOG-08 violation: 'hunter2' leaked through redaction: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected output to contain %q, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_ParseFailureReturnsRedacted — fail-closed per RESEARCH Pattern 5 line 660.
func TestRedactLiterals_ParseFailureReturnsRedacted(t *testing.T) {
	// Deeply broken — ANTLR will report syntax errors; our redactor must fall back
	// to the conservative "return placeholder" path rather than leaking partial input.
	in := `MATCH ((((`
	out := RedactLiterals(in)
	if out != RedactedPlaceholder {
		t.Fatalf("expected fail-closed = %q, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_EmptyQuery — defensive: empty input returns placeholder (parse errors out).
func TestRedactLiterals_EmptyQuery(t *testing.T) {
	out := RedactLiterals("")
	if out != RedactedPlaceholder {
		t.Fatalf("expected %q for empty input, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_CallUnionSubquery_PreservesStructure — NornicDB issue #563 defect 3:
// a CALL { ... UNION ... } subquery is a valid, executable Cypher shape (Eshu's
// entity-label lookups use it) but was collapsing to the whole-statement
// RedactedPlaceholder fallback instead of redacting only its literals.
func TestRedactLiterals_CallUnionSubquery_PreservesStructure(t *testing.T) {
	in := `CALL { MATCH (n:Foo) WHERE n.name = "alice" RETURN n AS x UNION MATCH (m:Bar) WHERE m.name = "bob" RETURN m AS x } RETURN x`
	out := RedactLiterals(in)
	if out == RedactedPlaceholder {
		t.Fatalf("CALL{UNION} subquery collapsed to whole-statement redaction: %q", out)
	}
	if strings.Contains(out, "alice") || strings.Contains(out, "bob") {
		t.Fatalf("literal leaked: %q", out)
	}
	for _, ident := range []string{"CALL", "UNION", "MATCH", "Foo", "Bar", "RETURN"} {
		if !strings.Contains(out, ident) {
			t.Errorf("structure token %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_CallUnion22Branches_PreservesStructure — the 22-branch
// CALL{UNION} entity-label lookup shape from the #7014 regression scan.
func TestRedactLiterals_CallUnion22Branches_PreservesStructure(t *testing.T) {
	var b strings.Builder
	b.WriteString("CALL { ")
	for i := 0; i < 22; i++ {
		if i > 0 {
			b.WriteString(" UNION ")
		}
		fmt.Fprintf(&b, `MATCH (n:Label%d) WHERE n.name CONTAINS "needle%d" RETURN n.id AS id, 'Label%d' AS label`, i, i, i)
	}
	b.WriteString(" } RETURN id, label LIMIT 2")
	in := b.String()
	out := RedactLiterals(in)
	if out == RedactedPlaceholder {
		t.Fatalf("22-branch CALL{UNION} collapsed to whole-statement redaction")
	}
	if strings.Contains(out, "needle0") {
		t.Fatalf("literal leaked: %q", out)
	}
	if !strings.Contains(out, "Label0") || !strings.Contains(out, "Label21") {
		t.Errorf("branch labels not preserved: %q", out)
	}
}

// TestRedactLiterals_SingleQuotedStringLiterals — Cypher (and NornicDB's own
// grammar, see CHAR_LITERAL in CypherLexer.g4) accepts single-quoted string
// literals as an alternate spelling of STRING_LITERAL. Discovered live while
// verifying the NornicDB issue #563 defect-3 fix against a running server: a
// CALL{UNION} branch with `m.name = 'trigger'` leaked "trigger" verbatim into
// the slow-query log because the redaction switch only covered the
// double-quoted STRING_LITERAL token type, never CHAR_LITERAL.
func TestRedactLiterals_SingleQuotedStringLiterals(t *testing.T) {
	in := `MATCH (n {name: 'alice', password: 'hunter2'}) RETURN n`
	out := RedactLiterals(in)
	if strings.Contains(out, "alice") {
		t.Fatalf("single-quoted string literal 'alice' leaked: %q", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("LOG-08 violation: single-quoted string literal 'hunter2' leaked: %q", out)
	}
	if !strings.Contains(out, RedactedPlaceholder) {
		t.Fatalf("expected output to contain %q, got %q", RedactedPlaceholder, out)
	}
	for _, ident := range []string{"MATCH", "n", "name", "password", "RETURN"} {
		if !strings.Contains(out, ident) {
			t.Errorf("identifier/keyword %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_ListLiterals — INTEGER elements inside an inline list
// literal must still be redacted individually; the list brackets and the
// surrounding structure are preserved.
func TestRedactLiterals_ListLiterals(t *testing.T) {
	in := `MATCH (n) WHERE n.id IN [111, 222, 333] RETURN n`
	out := RedactLiterals(in)
	if out == RedactedPlaceholder {
		t.Fatalf("list-literal query collapsed to whole-statement redaction: %q", out)
	}
	for _, leaked := range []string{"111", "222", "333"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("list element %q leaked: %q", leaked, out)
		}
	}
	for _, ident := range []string{"MATCH", "WHERE", "IN", "[", "]", "RETURN"} {
		if !strings.Contains(out, ident) {
			t.Errorf("structure token %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_UnterminatedStringDoesNotLeak — defect-3 fix removed the
// full cantlr.Parse gate; this proves the replacement lexer-level signals
// (ERRCHAR token + unbalanced brace depth) still fail-closed on a truncated
// string literal instead of leaking its partial content.
func TestRedactLiterals_UnterminatedStringDoesNotLeak(t *testing.T) {
	in := `MATCH (n {name: "ali`
	out := RedactLiterals(in)
	if out != RedactedPlaceholder {
		t.Fatalf("expected fail-closed = %q for unterminated string, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_VarLengthTraversalWithListParams_PreservesStructure —
// a three-MATCH chain with a *0..4 variable-length hop and only list-typed
// params ($ids/$kinds/$limit), the other #563-scan shape that collapsed —
// specifically because of the `$limit`-named parameter, not the *0..4
// variable-length traversal syntax itself (see RedactLiterals's doc comment).
func TestRedactLiterals_VarLengthTraversalWithListParams_PreservesStructure(t *testing.T) {
	in := `MATCH (a) WHERE a.id IN $ids MATCH p = (a)-[:CALLS*0..4]->(b) MATCH (b)-[:SINKS]->(c) WHERE c.kind IN $kinds RETURN c LIMIT $limit`
	out := RedactLiterals(in)
	if out == RedactedPlaceholder {
		t.Fatalf("varlen traversal collapsed to whole-statement redaction: %q", out)
	}
	for _, ident := range []string{"$ids", "$kinds", "$limit", "CALLS", "SINKS", "MATCH"} {
		if !strings.Contains(out, ident) {
			t.Errorf("token %q not preserved in %q", ident, out)
		}
	}
}

// TestRedactLiterals_BareWordValueFailsClosed — NornicDB issue #563 review
// finding C1: `MATCH (n {name: John Secret})` is not valid Cypher (an
// unquoted bare-word value), but it lexes clean — no ERRCHAR, balanced
// braces/parens — so before this fix it round-tripped through the token
// walk verbatim instead of failing closed like main did. Two ID tokens
// back to back with nothing between them (signal 5) must still fail closed
// even though every bracket balances.
func TestRedactLiterals_BareWordValueFailsClosed(t *testing.T) {
	in := `MATCH (n {name: John Secret}) RETURN n`
	out := RedactLiterals(in)
	if out != RedactedPlaceholder {
		t.Fatalf("expected fail-closed = %q for unquoted bare-word value, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_NegativeDepthThenBalancedFailsClosed — NornicDB issue
// #563 review finding C1: `) MATCH (n {name:'secret'}) (` has a closer
// before its matching opener. Depth dips to -1 and climbs back to exactly 0
// by EOF, so a check that only looks at the final depth (`depth != 0`)
// passes it through with the literal redacted but the invalid structure
// intact. Depth must never be allowed to go negative, checked as it happens.
func TestRedactLiterals_NegativeDepthThenBalancedFailsClosed(t *testing.T) {
	in := `) MATCH (n {name:'secret'}) (`
	out := RedactLiterals(in)
	if out != RedactedPlaceholder {
		t.Fatalf("expected fail-closed = %q for a closer preceding its opener, got %q", RedactedPlaceholder, out)
	}
}

// TestRedactLiterals_ERRCHARGuardCatchesUnterminatedStringWithBalancedBrackets
// — NornicDB issue #563 review finding C2: the ERRCHAR check is load-bearing
// on its own, separately from listener.hadError, the depth check, and the
// bare-word-adjacency check. This query's only brackets are the balanced
// `(n)` (no depth signal), the trailing text after the unclosed quote is a
// single word with no adjacent ID pair (no signal-5 overlap), and the
// lexer's error listener never fires for an unterminated CHAR_LITERAL (the
// unclosed quote routes the lexer into an ERRCHAR token instead — see the
// doc comment above). Verified by mutation: with the
// `ttype == cantlr.CypherLexerERRCHAR` guard removed, this is the case that
// leaks the partial secret text verbatim while every other existing test
// keeps passing.
func TestRedactLiterals_ERRCHARGuardCatchesUnterminatedStringWithBalancedBrackets(t *testing.T) {
	in := `MATCH (n) WHERE n.name = 'unterminatedSecretXYZ RETURN n`
	out := RedactLiterals(in)
	if out != RedactedPlaceholder {
		t.Fatalf("expected fail-closed = %q for an unterminated CHAR_LITERAL with balanced brackets, got %q", RedactedPlaceholder, out)
	}
	if strings.Contains(out, "unterminatedSecretXYZ") {
		t.Fatalf("LOG-08 violation: unterminated string content leaked: %q", out)
	}
}

// TestRedactLiterals_MalformedInputsNeverLeak — a fuzz-ish table of
// malformed/adversarial inputs, each carrying a unique secret-shaped marker
// that must never appear in the output, regardless of which fail-closed
// signal (or combination) catches it.
func TestRedactLiterals_MalformedInputsNeverLeak(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		secret string
	}{
		{
			name:   "bare_word_value",
			in:     `MATCH (n {name: John Secret9931}) RETURN n`,
			secret: "Secret9931",
		},
		{
			name:   "negative_depth_then_balanced",
			in:     `) MATCH (n {name:'Secret9932'}) (`,
			secret: "Secret9932",
		},
		{
			name:   "unterminated_char_literal_balanced_brackets",
			in:     `MATCH (n) WHERE n.name = 'Secret9933 RETURN n`,
			secret: "Secret9933",
		},
		{
			name:   "unterminated_string_literal_with_open_brace",
			in:     `MATCH (n {name: "Secret9934`,
			secret: "Secret9934",
		},
		{
			name:   "deeply_unbalanced_open_parens",
			in:     `MATCH (((((n {name:'Secret9935'`,
			secret: "Secret9935",
		},
		{
			name:   "trailing_orphan_closer",
			in:     `MATCH (n {name:'Secret9936'}) RETURN n)`,
			secret: "Secret9936",
		},
		{
			name:   "three_bare_words_in_a_row",
			in:     `MATCH (n {note: alpha Secret9937 gamma}) RETURN n`,
			secret: "Secret9937",
		},
		{
			name:   "unbalanced_bracket_in_list",
			in:     `MATCH (n) WHERE n.tags = [1, 2, "Secret9938" RETURN n`,
			secret: "Secret9938",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := RedactLiterals(tc.in)
			if strings.Contains(out, tc.secret) {
				t.Fatalf("secret-shaped token %q leaked through redaction: input=%q output=%q", tc.secret, tc.in, out)
			}
		})
	}
}
