package fgac

import (
	"strings"
	"testing"
)

func TestCheckExpectation(t *testing.T) {
	allow := Expectation{Kind: "allow", Rows: [][]any{{"doc-a", int64(1)}}}
	if err := CheckExpectation(allow, Outcome{Rows: [][]any{{"doc-a", int64(1)}}}); err != nil {
		t.Fatalf("allow: %v", err)
	}
	if err := CheckExpectation(allow, Outcome{Rows: [][]any{{"doc-b", int64(1)}}}); err == nil {
		t.Fatal("allow accepted different rows")
	}
	if err := CheckExpectation(allow, Outcome{Code: "Neo.ClientError.Security.Forbidden"}); err == nil {
		t.Fatal("allow accepted a denial")
	}

	deny := Expectation{Kind: "deny", Code: "Neo.ClientError.Security.Forbidden"}
	if err := CheckExpectation(deny, Outcome{Code: "Neo.ClientError.Security.Forbidden"}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	if err := CheckExpectation(deny, Outcome{}); err == nil {
		t.Fatal("deny accepted success")
	}
	if err := CheckExpectation(deny, Outcome{Code: "Neo.ClientError.Statement.SyntaxError"}); err == nil {
		t.Fatal("deny accepted the wrong code")
	}

	family := Expectation{Kind: "deny", Code: "Neo.ClientError.Security."}
	if err := CheckExpectation(family, Outcome{Code: "Neo.ClientError.Security.AuthorizationExpired"}); err != nil {
		t.Fatalf("family: %v", err)
	}
}

func TestCompareOutcomes(t *testing.T) {
	reference := Outcome{Columns: []string{"id"}, Rows: [][]any{{"doc-a"}}}
	if err := CompareOutcomes(reference, Outcome{Columns: []string{"id"}, Rows: [][]any{{"doc-a"}}}); err != nil {
		t.Fatalf("equal outcomes: %v", err)
	}
	if err := CompareOutcomes(reference, Outcome{Columns: []string{"name"}, Rows: [][]any{{"doc-a"}}}); err == nil {
		t.Fatal("CompareOutcomes ignored columns")
	}
	if err := CompareOutcomes(reference, Outcome{Columns: []string{"id"}, Rows: [][]any{{"doc-b"}}}); err == nil {
		t.Fatal("CompareOutcomes ignored rows")
	}
	if err := CompareOutcomes(Outcome{Code: "Neo.ClientError.Security.Forbidden"}, Outcome{Code: "Neo.ClientError.Security.Forbidden"}); err != nil {
		t.Fatalf("equal denials: %v", err)
	}
	if err := CompareOutcomes(Outcome{Code: "Neo.ClientError.Security.Forbidden"}, Outcome{Code: "Neo.ClientError.Database.DatabaseNotFound"}); err == nil {
		t.Fatal("CompareOutcomes ignored the error code")
	}
}

func TestCodeMatches(t *testing.T) {
	if !CodeMatches("Neo.ClientError.Security.Forbidden", "Neo.ClientError.Security.Forbidden") {
		t.Error("exact code did not match")
	}
	if !CodeMatches("Neo.ClientError.Security.AuthorizationExpired", "Neo.ClientError.Security.") {
		t.Error("family did not match")
	}
	if CodeMatches("Neo.ClientError.Statement.SyntaxError", "Neo.ClientError.Security.") {
		t.Error("family matched the wrong code")
	}
	if CodeMatches("Neo.ClientError.SecurityX", "Neo.ClientError.Security") {
		t.Error("non-prefix expectation accepted a longer code")
	}
}

func TestNormalizeBoltValue(t *testing.T) {
	value, err := normalizeBoltValue([]any{"a", int64(1), map[string]any{"b": true}})
	if err != nil {
		t.Fatalf("normalizeBoltValue: %v", err)
	}
	want := []any{"a", int64(1), map[string]any{"b": true}}
	if !RowsEqual([][]any{value.([]any)}, [][]any{want}) {
		t.Fatalf("normalizeBoltValue = %#v", value)
	}
	if _, err := normalizeBoltValue(struct{}{}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("normalizeBoltValue unsupported error = %v", err)
	}
}

func TestNormalizeFixtureValue(t *testing.T) {
	if got := normalizeFixtureValue(float64(3)); got != int64(3) {
		t.Fatalf("integral float = %#v", got)
	}
	if got := normalizeFixtureValue(float64(3.5)); got != float64(3.5) {
		t.Fatalf("fractional float = %#v", got)
	}
	if got := normalizeFixtureValue([]any{float64(1), "x"}); !RowsEqual([][]any{got.([]any)}, [][]any{{int64(1), "x"}}) {
		t.Fatalf("list = %#v", got)
	}
}
