// Wave-0 RED tests for cypher.RedactLiterals (D-04 ANTLR4 listener).
//
// These tests reference RedactLiterals + RedactedPlaceholder which do not yet
// exist; the package must fail to compile until the GREEN task ships
// pkg/cypher/redaction.go.
package cypher

import (
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
