package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNormalizeLiteralText pins the canonical-form contract of the compile
// caches: statements that differ only in literal values share one form, and
// every literal context is respected.
func TestNormalizeLiteralText(t *testing.T) {
	same := func(a, b string) {
		t.Helper()
		ca, changedA := normalizeLiteralText(a)
		cb, changedB := normalizeLiteralText(b)
		require.True(t, changedA && changedB, "expected literals in %q / %q", a, b)
		require.Equal(t, ca, cb, "%q and %q must share a canonical form", a, b)
	}
	differs := func(a, b string) {
		t.Helper()
		ca, _ := normalizeLiteralText(a)
		cb, _ := normalizeLiteralText(b)
		require.NotEqual(t, ca, cb, "%q and %q must normalize differently", a, b)
	}
	unchanged := func(a string) {
		t.Helper()
		ca, changed := normalizeLiteralText(a)
		require.False(t, changed, "%q must report no literals", a)
		require.Equal(t, a, ca)
	}

	same("CREATE (n:Node {id: 5})", "CREATE (n:Node {id: 6})")
	same("RETURN 1 AS x", "RETURN 999 AS x")
	same("RETURN 'a' AS x", "RETURN 'zzz' AS x")
	same("RETURN 1.5 AS x", "RETURN 2.25 AS x")
	same("RETURN 1e3 AS x", "RETURN 9E-2 AS x")
	same("RETURN 0x1F AS x", "RETURN 0x2A AS x")
	differs("RETURN 0x7FFFFFFFFFFFFFFF AS x", "RETURN 0x8000000000000000 AS x")
	same("RETURN {id: $a} AS x", "RETURN {id: $zz} AS x")
	same("RETURN -1 AS x", "RETURN -2 AS x")

	differs("RETURN 'a' + 'b' AS x", "RETURN 'ab' AS x")
	differs("RETURN 1 AS x", "RETURN '1' AS x")
	differs("RETURN 1e3 AS x", "RETURN 1e AS x")
	differs("RETURN 1.5 AS x", "RETURN 1. AS x")
	differs("RETURN 1 // c1", "RETURN 1 // c2")
	differs("RETURN $(['A'])", "RETURN $(['B'])")

	// Quoting and escaped text: a doubled quote is two adjacent strings, not
	// an escape (Cypher has none; the validator rejects the pair), and
	// backslash escapes are copied verbatim (their validity can depend on the
	// escape itself).
	differs("RETURN 'it''s' AS x", "RETURN 'other' AS x")
	same("RETURN 'it''s' AS x", "RETURN 'a''b' AS x")
	differs(`RETURN "say ""hi""" AS x`, `RETURN "say" AS x`)
	unchanged(`RETURN '\'' AS x`)
	unchanged("RETURN \"a\\\"b\" AS x")

	// No literals: exact text, changed=false.
	unchanged("RETURN n")
	same("CREATE (n:Node {id: $p}) RETURN n", "CREATE (n:Node {id: $qq}) RETURN n")
}

func TestNormalizeLiteralTextParamsAndIdentifiers(t *testing.T) {
	// Digit-bearing identifiers are not literals.
	ca, changedA := normalizeLiteralText("RETURN n1 AS x")
	require.False(t, changedA)
	require.Equal(t, "RETURN n1 AS x", ca)

	// Backticked digit names are copied verbatim.
	cb, changedB := normalizeLiteralText("RETURN `1` AS x")
	require.False(t, changedB)
	require.Equal(t, "RETURN `1` AS x", cb)

	// Comments with quotes do not leak their quote state.
	cc, changedC := normalizeLiteralText("RETURN 1 // 'unterminated")
	require.True(t, changedC)
	require.Contains(t, cc, "// 'unterminated")
	require.NotContains(t, cc, normStringMarker)
}

// TestSemanticValidationCanonicalLiteralCache pins the #6 behavior: accepted
// literal-bearing statements share one compile verdict through the canonical
// form, and rejected statements are never cached.
func TestSemanticValidationCanonicalLiteralCache(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "RETURN 1 AS x", nil)
	require.NoError(t, err)
	canonical, changed := normalizeLiteralText("RETURN 1 AS x")
	require.True(t, changed)
	require.True(t, exec.semanticValidationCache.contains(canonical),
		"accepted literal-bearing statements are cached under their canonical form")

	// A different literal value shares the cached verdict.
	_, err = exec.Execute(ctx, "RETURN 2 AS x", nil)
	require.NoError(t, err)
	_, changed = normalizeLiteralText("RETURN 2 AS x")
	require.True(t, changed)

	// Invalid statements are still rejected and never cached.
	_, err = exec.Execute(ctx, "RETURN 1 AS x, 2 AS x", nil)
	require.Error(t, err)
	badCanonical, badChanged := normalizeLiteralText("RETURN 1 AS x, 2 AS x")
	require.True(t, badChanged)
	require.False(t, exec.semanticValidationCache.contains(badCanonical))
}

// TestMergeSemanticCanonicalLiteralCache pins the same canonical sharing for
// MERGE validation.
func TestMergeSemanticCanonicalLiteralCache(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical_merge"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "MERGE (n:Cached {id: 1}) RETURN n", nil)
	require.NoError(t, err)
	canonical, changed := normalizeLiteralText("MERGE (n:Cached {id: 1}) RETURN n")
	require.True(t, changed)
	require.True(t, exec.mergeSemanticValidationCache.contains(canonical))

	_, err = exec.Execute(ctx, "MERGE (n:Cached {id: 2}) RETURN n", nil)
	require.NoError(t, err)
}

// TestSyntaxCacheCanonicalLiteralSharing pins that parser-acceptance verdicts
// share across literal variants.
func TestSyntaxCacheCanonicalLiteralSharing(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical_syntax"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Node {id: 1})", nil)
	require.NoError(t, err)
	canonical, changed := normalizeLiteralText("CREATE (n:Node {id: 2})")
	require.True(t, changed)
	require.True(t, exec.hasCachedValidSyntax("CREATE (n:Node {id: 2})", syntaxStatementAccepted),
		"a literal variant of an accepted statement hits the syntax cache")
	_ = canonical
}

// TestCanonicalCacheDoesNotMaskHexOverflow pins the TCK Literals3 scenarios:
// a valid integer's canonical verdict must not suppress the IntegerOverflow
// of a too-large hexadecimal literal (whose validity depends on its value).
func TestCanonicalCacheDoesNotMaskHexOverflow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "hexovf"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "RETURN 1 AS literal", nil)
	require.NoError(t, err)
	assertIntegerOverflow := func(query string) {
		t.Helper()
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err)
		var detailed interface{ BoltErrorDetail() string }
		require.ErrorAs(t, err, &detailed, "expected a detail-carrying Cypher error, got: %v", err)
		require.Equal(t, "IntegerOverflow", detailed.BoltErrorDetail(), "got: %v", err)
	}
	assertIntegerOverflow("RETURN 0x8000000000000000 AS literal")
	assertIntegerOverflow("RETURN -0x8000000000000001 AS literal")
}

// TestCanonicalCacheDoesNotMaskInvalidPagination pins monster's retest part 3:
// SKIP / LIMIT bounds are value-dependent, so a valid sibling's canonical
// verdict must not suppress the rejection of a negative or non-integer bound
// that shares the same canonical form.
func TestCanonicalCacheDoesNotMaskInvalidPagination(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pagination"))
	ctx := context.Background()

	// Warm the canonical form with a valid bound, then probe an invalid one
	// that normalizes to the same form.
	warm := []string{
		"UNWIND range(1, 10) AS x RETURN x LIMIT 5-1",
		"UNWIND range(1, 10) AS x RETURN x SKIP 5-1",
		"UNWIND range(1, 10) AS x RETURN x LIMIT toInteger('3')",
	}
	probes := []string{
		"UNWIND range(1, 10) AS x RETURN x LIMIT 2-3",
		"UNWIND range(1, 10) AS x RETURN x SKIP 2-3",
		"UNWIND range(1, 10) AS x RETURN x LIMIT toInteger('x')",
	}
	for _, q := range warm {
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "warm-up %q", q)
		require.NotEmpty(t, res.Rows, "warm-up %q must return rows", q)
	}
	for _, q := range probes {
		_, err := exec.Execute(ctx, q, nil)
		require.Error(t, err, "probe %q must fail after the warm-up", q)
	}
}
