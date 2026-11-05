package cypher

import (
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func ownedLegacyHeaderColumns(t *testing.T, query string, incoming ...string) []string {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "migrated-headers"))
	columns, err := e.ResultColumns(t.Context(), query, resultmetadata.KnownScope(incoming), false, RawSource)
	if err != nil || len(columns) == 0 {
		return nil
	}
	return columns
}

func TestInferReturnColumnsFromQuery(t *testing.T) {
	query := `CALL {
  USE translations.tr
  MATCH (t:MongoDocument)
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
}
RETURN coalesce(textKey, textKey128) AS textKey, textKey128, count(*) AS c
ORDER BY textKey128
LIMIT 25`
	got := ownedLegacyHeaderColumns(t, query)
	if len(got) != 3 || got[0] != "textKey" || got[1] != "textKey128" || got[2] != "c" {
		t.Fatalf("unexpected inferred columns: %#v", got)
	}
}

func TestInferReturnColumnsFromQuery_NoReturn(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "MATCH (n) WHERE n.id = 1")
	if got != nil {
		t.Fatalf("expected nil for no RETURN, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_EmptyReturn(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "MATCH (n) RETURN")
	if got != nil {
		t.Fatalf("expected nil for empty RETURN clause, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_WithSemicolon(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "RETURN a, b;", "a", "b")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected [a b], got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_NestedParens(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "RETURN coalesce(a, b) AS x, c ORDER BY x", "a", "b", "c")
	if len(got) != 2 || got[0] != "x" || got[1] != "c" {
		t.Fatalf("expected [x c], got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_QuotedCommas(t *testing.T) {
	// Commas inside single-quoted strings.
	got := ownedLegacyHeaderColumns(t, "RETURN 'a,b' AS x, c", "c")
	if len(got) != 2 || got[0] != "x" || got[1] != "c" {
		t.Fatalf("expected [x c], got %v", got)
	}
	// Commas inside double-quoted strings.
	got = ownedLegacyHeaderColumns(t, `RETURN "a,b" AS x, c`, "c")
	if len(got) != 2 || got[0] != "x" || got[1] != "c" {
		t.Fatalf("expected [x c] with double quotes, got %v", got)
	}
	// Commas inside backtick-quoted.
	got = ownedLegacyHeaderColumns(t, "RETURN `a,b` AS x, c", "a,b", "c")
	if len(got) != 2 || got[0] != "x" || got[1] != "c" {
		t.Fatalf("expected [x c] with backticks, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_BracketsAndBraces(t *testing.T) {
	// Commas inside brackets should not split.
	got := ownedLegacyHeaderColumns(t, "RETURN [1,2,3] AS arr, b", "b")
	if len(got) != 2 || got[0] != "arr" || got[1] != "b" {
		t.Fatalf("expected [arr b], got %v", got)
	}
	// Commas inside braces should not split.
	got = ownedLegacyHeaderColumns(t, "RETURN {x: 1, y: 2} AS m, c", "c")
	if len(got) != 2 || got[0] != "m" || got[1] != "c" {
		t.Fatalf("expected [m c], got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_SkipAndLimit(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "RETURN a, b SKIP 5", "a", "b")
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected [a b] ignoring SKIP, got %v", got)
	}
	got = ownedLegacyHeaderColumns(t, "RETURN a LIMIT 10", "a")
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a] ignoring LIMIT, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_NoAlias(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "RETURN n.name", "n")
	if len(got) != 1 || got[0] != "n.name" {
		t.Fatalf("expected [n.name] with no alias, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_BacktickAlias(t *testing.T) {
	got := ownedLegacyHeaderColumns(t, "RETURN n.name AS `my col`", "n")
	if len(got) != 1 || got[0] != "my col" {
		t.Fatalf("expected [my col] stripped of backticks, got %v", got)
	}
}

func TestInferReturnColumnsFromQuery_EmptyItemAfterComma(t *testing.T) {
	// The removed text inferrer skipped malformed items. The statement owner
	// must reject the query rather than publish a partial authoritative header.
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "migrated-invalid-header"))
	_, err := executor.ResultColumns(t.Context(), "RETURN a, ", resultmetadata.KnownScope([]string{"a"}), false, RawSource)
	if err == nil {
		t.Fatal("a trailing comma must not produce an authoritative header")
	}
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func BenchmarkInferReturnColumnsFromQuery(b *testing.B) {
	query := `CALL {
  USE translations.tr
  MATCH (t:Translation)
  RETURN t.id AS id, t.textKey AS textKey, t.textKey128 AS textKey128, t.lang AS lang
}
RETURN id, textKey, textKey128, lang
ORDER BY textKey128
LIMIT 25`
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "migrated-header-bench"))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = exec.StatementColumns(query)
	}
}
