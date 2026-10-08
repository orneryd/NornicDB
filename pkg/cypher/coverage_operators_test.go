package cypher

import (
	"context"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// All tests in this file target deterministic, side-effect-free helper
// functions in operators.go, explain.go, executor_match_vector_cosine_fastpath.go,
// unwind_relationship_merge_batch.go, and pipeline_executor.go. None of them
// touch the storage engine; they're pure functions over strings, scalars,
// and small slices.

// ----------------------------------------------------------------------------
// freshExecutorForOperators returns a minimal in-memory executor sufficient
// for the operator helpers, which only depend on parseValue / property
// lookups when the expression actually references nodes or relationships.
// ----------------------------------------------------------------------------

func TestGh728TopLevelOperatorUsesSharedQuoteScanner(t *testing.T) {
	tests := []struct {
		name, expr, op string
		fold, brackets bool
		want           int
	}{
		{name: "backtick property", expr: "n.`a+b` + 1", op: "+", want: strings.LastIndex("n.`a+b` + 1", "+")},
		{name: "escaped backtick", expr: "n.`a``+b` + 1", op: "+", want: strings.LastIndex("n.`a``+b` + 1", "+")},
		{name: "escaped single quote", expr: `'a\' + b' + 1`, op: "+", want: strings.LastIndex(`'a\' + b' + 1`, "+")},
		{name: "escaped double quote", expr: `"a\" + b" + 1`, op: "+", want: strings.LastIndex(`"a\" + b" + 1`, "+")},
		{name: "doubled quote", expr: `'a'' + b' + 1`, op: "+", want: strings.LastIndex(`'a'' + b' + 1`, "+")},
		{name: "utf8 byte index", expr: "\u540d\u524d.\u6570 + 1", op: "+", want: strings.LastIndex("\u540d\u524d.\u6570 + 1", "+")},
		{name: "bracket isolation", expr: "[1 + 2] + 3", op: "+", brackets: true, want: strings.LastIndex("[1 + 2] + 3", "+")},
		{name: "bracket option", expr: "[1 + 2] + 3", op: "+", want: strings.Index("[1 + 2] + 3", "+")},
		{name: "folded keyword", expr: "a And b", op: " AND ", fold: true, want: 1},
		{name: "folded letter prefix", expr: "a And b", op: "AND", fold: true, want: 2},
		{name: "folded lowercase prefix", expr: "a AND b", op: "and", fold: true, want: 2},
		{name: "single letter prefix", expr: "a", op: "A", fold: true, want: 0},
		{name: "parenthesis isolation", expr: "(1 + 2) + 3", op: "+", want: strings.LastIndex("(1 + 2) + 3", "+")},
		{name: "brace isolation", expr: "{x:1 + 2} + 3", op: "+", brackets: true, want: strings.LastIndex("{x:1 + 2} + 3", "+")},
		{name: "case sensitive keyword", expr: "a And b", op: " AND ", want: -1},
		{name: "punctuation must not fold", expr: "n.x ~ 1", op: "^", fold: true, want: -1},
		{name: "compound comparison", expr: "n.age >= 3", op: "=", want: -1},
		{name: "regex comparison", expr: "n.name =~ 'a'", op: "=", want: -1},
		{name: "unterminated quoted name", expr: "n.`a+b + 1", op: "+", want: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, findTopLevelOperator(test.expr, test.op, test.fold, test.brackets))
		})
	}
	t.Run("empty operator", func(t *testing.T) {
		require.NotPanics(t, func() {
			require.Equal(t, -1, findTopLevelOperator("", "", false, true))
		})
	})
}

func TestGh728StatementClockPrefixScan(t *testing.T) {
	for _, test := range []struct {
		expression string
		want       bool
	}{
		{"SIZE(n.name) + ABS(n.count) + COALESCE(n.count, 0)", false},
		{"date()", true},
		{"DaTeTiMe.statement ()", true},
		{"LOCALDATETIME.transaction()", true},
		{"TiMe.realtime()", true},
		{"LOCALTIME ()", true},
		{"DeCaYScore(n)", true},
		{"DECAY (n)", true},
		{"datetimey() + timezone() + localtimex()", false},
		{"apoc.date() + my.datetime()", false},
		{"(1 + 2) + ()", false},
		{"x.datetime + y.time", false},
		{"DATEDIFF(n.created, n.updated)", false},
	} {
		t.Run(test.expression, func(t *testing.T) {
			require.Equal(t, test.want, containsStatementClockCall(test.expression))
		})
	}
}

func BenchmarkGh728StatementClockPrefixScan(b *testing.B) {
	for _, test := range []struct {
		name, expression string
		want             bool
	}{
		{"lowercase", "size(n.name) + abs(n.count) + coalesce(n.count, 0)", false},
		{"uppercase", "SIZE(n.name) + ABS(n.count) + COALESCE(n.count, 0)", false},
		{"clock", "size(n.name) + DATETIME.statement ()", true},
		{"qualified", "apoc.date(n.created) + my.datetime(n.updated)", false},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if got := containsStatementClockCall(test.expression); got != test.want {
					b.Fatalf("expected clock detection %t, got %t", test.want, got)
				}
			}
		})
	}
}

func TestGh728ArithmeticTierBitsetAndSharedQuotes(t *testing.T) {
	for _, test := range []struct {
		expr, operators, left, right string
		operator                     byte
		ok                           bool
	}{
		{expr: "'a\\' + b' + 'z'", operators: "+-", left: "'a\\' + b'", right: "'z'", operator: '+', ok: true},
		{expr: "n.`a+b` + 1", operators: "+-", left: "n.`a+b`", right: "1", operator: '+', ok: true},
		{expr: "[1+2][0] + {x:3}.x", operators: "+-", left: "[1+2][0]", right: "{x:3}.x", operator: '+', ok: true},
		{expr: "(1+2) ^ 3", operators: "^", left: "(1+2)", right: "3", operator: '^', ok: true},
		{expr: "1e-3 + -2", operators: "+-", left: "1e-3", right: "-2", operator: '+', ok: true},
		{expr: "-1", operators: "+-"},
		{expr: "n.x", operators: ""},
		{expr: "a + b", operators: "+\u540d", left: "a", right: "b", operator: '+', ok: true},
		{expr: string([]byte{'a', 0xff, 'b'}), operators: "\u00ff", left: "a", right: "b", operator: 0xff, ok: true},
	} {
		t.Run(test.expr, func(t *testing.T) {
			left, right, operator, ok := splitRowArithmeticTier(test.expr, test.operators)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.left, left)
			require.Equal(t, test.right, right)
			require.Equal(t, test.operator, operator)
		})
	}
}

func BenchmarkTopLevelOperatorPipelineScan(b *testing.B) {
	for _, workload := range []struct{ name, expr, op string }{
		{"short", "size(n.name) + n.count >= 0", ">="},
		{"long", strings.Repeat("n.name + ", 64) + "n.count >= 0", ">="},
		{"quoted", "n.name = '" + strings.Repeat("letters AND words ", 128) + "' AND true", " AND "},
	} {
		b.Run(workload.name, func(b *testing.B) {
			want := strings.LastIndex(workload.expr, workload.op)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if got := findTopLevelOperator(workload.expr, workload.op, true, true); got != want {
					b.Fatalf("expected byte %d, got %d", want, got)
				}
			}
		})
	}
}

func freshExecutorForOperators(t *testing.T) *StorageExecutor {
	t.Helper()
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "operators-test")
	return NewStorageExecutor(ns)
}

// ============================================================================
// hasLogicalOperator — top-level vs quoted vs parenthesised.
// ============================================================================

func TestHasLogicalOperator_TopLevelAndQuoteAndParenIsolation(t *testing.T) {
	e := freshExecutorForOperators(t)

	require.True(t, e.hasLogicalOperator("a = 1 AND b = 2", " AND "))
	require.True(t, e.hasLogicalOperator("a = 1 OR b = 2", " OR "))
	require.False(t, e.hasLogicalOperator("name CONTAINS 'AND ME'", " AND "),
		"AND inside single quotes must not register")
	require.False(t, e.hasLogicalOperator(`name = "AND IT WAS"`, " AND "),
		"AND inside double quotes must not register")
	require.False(t, e.hasLogicalOperator("(a AND b) OR c", " AND "),
		"AND inside parens is not top-level")
	require.False(t, e.hasLogicalOperator("a = 1", " AND "),
		"missing operator returns false")
}

// ============================================================================
// hasComparisonOperator — every documented operator + negative.
// ============================================================================

func TestHasComparisonOperator_AllOperatorsAndNegative(t *testing.T) {
	e := freshExecutorForOperators(t)
	ops := []struct {
		name string
		expr string
	}{
		{"<>", "a <> b"},
		{"<=", "a <= b"},
		{">=", "a >= b"},
		{"=~", `a =~ '.*'`},
		{"!=", "a != b"},
		{"=", "a = b"},
		{"<", "a < b"},
		{">", "a > b"},
	}
	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, e.hasComparisonOperator(tc.expr))
		})
	}
	require.False(t, e.hasComparisonOperator("a + b"))
	require.False(t, e.hasComparisonOperator("name CONTAINS 'foo'"))
}

// ============================================================================
// hasOperatorOutsideQuotes — every short-circuit branch.
// ============================================================================

func TestHasOperatorOutsideQuotes(t *testing.T) {
	e := freshExecutorForOperators(t)
	require.False(t, e.hasOperatorOutsideQuotes("ab", "abcd"),
		"operator longer than expression short-circuits to false")
	require.False(t, e.hasOperatorOutsideQuotes("a b c", "="),
		"expression containing none of the operator characters")
	require.True(t, e.hasOperatorOutsideQuotes("a = b", "="))
	require.False(t, e.hasOperatorOutsideQuotes("a <= b", "="),
		"= is part of <=, must NOT register")
	require.True(t, e.hasOperatorOutsideQuotes(`name = "a=b"`, "="),
		"the first '=' outside the quotes is detected even when a literal '=' also appears inside quotes",
	)
	require.False(t, e.hasOperatorOutsideQuotes(`name "a=b"`, "="),
		"the only '=' is inside double quotes — outside-quotes detector returns false",
	)
}

// ============================================================================
// hasArithmeticOperator — every supported arithmetic operator including
// space-padded + and - variants; unary minus must not register.
// ============================================================================

func TestHasArithmeticOperator_AllVariants(t *testing.T) {
	e := freshExecutorForOperators(t)
	cases := []struct {
		name string
		expr string
		want bool
	}{
		{"plus with spaces", "a + b", true},
		{"plus no spaces", "a+b", true},
		{"multiply", "a*b", true},
		{"divide", "a/b", true},
		{"modulo", "a%b", true},
		{"minus with spaces", "a - b", true},
		{"minus no spaces", "a-b", true},
		{"unary minus alone returns true (operator char present)", "-5", true},
		{"no arithmetic operator", "name", false},
		{"comparison only", "a < b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, e.hasArithmeticOperator(tc.expr))
		})
	}
}

// ============================================================================
// splitByOperator — wraps splitByOperatorWithOptions and returns
// []string{expr} when no match found.
// ============================================================================

func TestSplitByOperator_FoundAndNotFound(t *testing.T) {
	e := freshExecutorForOperators(t)
	out := e.splitByOperator("a + b", " + ")
	require.Equal(t, []string{"a", "b"}, out)
	out = e.splitByOperator("'a + b' + c", " + ")
	require.Equal(t, []string{"'a + b'", "c"}, out)
	out = e.splitByOperator("(a + b) + c", " + ")
	require.Equal(t, []string{"(a + b)", "c"}, out)

	// Not found → returns the entire expr unchanged.
	out = e.splitByOperator("a b c", " + ")
	require.Equal(t, []string{"a b c"}, out)
}

// ============================================================================
// hasStringPredicate — CONTAINS / STARTS WITH / ENDS WITH (case-insensitive,
// bracket-aware via findTopLevelOperator).
// ============================================================================

func TestHasStringPredicate(t *testing.T) {
	e := freshExecutorForOperators(t)
	require.True(t, e.hasStringPredicate("n.name CONTAINS 'foo'", " CONTAINS "))
	require.True(t, e.hasStringPredicate("n.name STARTS WITH 'pre'", " STARTS WITH "))
	require.True(t, e.hasStringPredicate("n.name ENDS WITH 'suf'", " ENDS WITH "))
	require.False(t, e.hasStringPredicate("'CONTAINS' = name", " CONTAINS "),
		"CONTAINS inside quotes must not register")
	require.False(t, e.hasStringPredicate("n.name STARTS WITHTH 'pre'", " STARTS WITH "),
		"longer trailing operator should not match")
}

// ============================================================================
// isAllDigits — boundary cases.
// ============================================================================

func TestIsAllDigits(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"0", true},
		{"123", true},
		{"012", true},
		{"-1", false},
		{"1.5", false},
		{"1a2", false},
		{"a", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.want, isAllDigits(tc.in))
		})
	}
}

// ============================================================================
// formatInlineFloat32Vector — empty + populated.
// ============================================================================

func TestFormatInlineFloat32Vector(t *testing.T) {
	require.Equal(t, "[]", formatInlineFloat32Vector(nil))
	require.Equal(t, "[]", formatInlineFloat32Vector([]float32{}))
	require.Equal(t, "[1,2.5,3]", formatInlineFloat32Vector([]float32{1, 2.5, 3}))
	require.Equal(t, "[-0.5,0.5]", formatInlineFloat32Vector([]float32{-0.5, 0.5}))
}

// ============================================================================
// compareScore — every documented operator + default fallthrough.
// ============================================================================

func TestCompareScore(t *testing.T) {
	require.True(t, compareScore(1.5, ">", 1.0))
	require.False(t, compareScore(1.5, ">", 2.0))
	require.True(t, compareScore(1.5, ">=", 1.5))
	require.True(t, compareScore(1.5, "<", 2.0))
	require.False(t, compareScore(1.5, "<", 1.5))
	require.True(t, compareScore(1.5, "<=", 1.5))
	require.False(t, compareScore(1.5, "==", 1.5), "unknown operator falls through to false")
	require.False(t, compareScore(1.5, "", 1.5), "empty operator returns false")
}

// ============================================================================
// relationshipBatchEdgeKey — deterministic concatenation shape.
// ============================================================================

func TestRelationshipBatchEdgeKey_FormatIsDeterministic(t *testing.T) {
	key := relationshipBatchEdgeKey(
		storage.NodeID("start-1"),
		storage.NodeID("end-1"),
		"KNOWS",
		map[string]interface{}{"since": int64(2024)},
	)
	// Format: "<start>\x00<end>\x00<type>\x00<unwindMergeKey>". We assert
	// the prefix and that it is non-empty after the third separator; the
	// exact unwindMergeKey internals are tested elsewhere.
	parts := strings.SplitN(key, "\x00", 4)
	require.Len(t, parts, 4)
	require.Equal(t, "start-1", parts[0])
	require.Equal(t, "end-1", parts[1])
	require.Equal(t, "KNOWS", parts[2])
	require.NotEmpty(t, parts[3])

	// Determinism: identical inputs yield identical keys.
	other := relationshipBatchEdgeKey(
		storage.NodeID("start-1"),
		storage.NodeID("end-1"),
		"KNOWS",
		map[string]interface{}{"since": int64(2024)},
	)
	require.Equal(t, key, other)
}

// ============================================================================
// appendRelationshipBatchScalarKeyValue — every type arm.
// ============================================================================

func TestAppendRelationshipBatchScalarKeyValue_AllTypes(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
		ok   bool
	}{
		{"nil", nil, "nil", true},
		{"string", "abc", "string:3:abc", true},
		{"bool true", true, "bool:true", true},
		{"bool false", false, "bool:false", true},
		{"int", int(7), "int:7", true},
		{"int8", int8(7), "int8:7", true},
		{"int16", int16(7), "int16:7", true},
		{"int32", int32(7), "int32:7", true},
		{"int64", int64(7), "int64:7", true},
		{"uint", uint(7), "uint:7", true},
		{"uint8", uint8(7), "uint8:7", true},
		{"uint16", uint16(7), "uint16:7", true},
		{"uint32", uint32(7), "uint32:7", true},
		{"uint64", uint64(7), "uint64:7", true},
		{"float32", float32(1.5), "float32:1.5", true},
		{"float64", float64(2.25), "float64:2.25", true},
		{"NaN", math.NaN(), "float64:NaN", true},
		{"unsupported slice", []int{1, 2}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			ok := appendRelationshipBatchScalarKeyValue(&b, tc.in)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.want, b.String())
			}
		})
	}

	t.Run("float32 with no decimal still parses back", func(t *testing.T) {
		var b strings.Builder
		require.True(t, appendRelationshipBatchScalarKeyValue(&b, float32(3)))
		out := b.String()
		// "float32:3" — verify the trailing number parses back.
		parts := strings.SplitN(out, ":", 2)
		require.Len(t, parts, 2)
		_, err := strconv.ParseFloat(parts[1], 32)
		require.NoError(t, err)
	})
}

// ============================================================================
// toAnySlice — every supported slice type + reflect fallback.
// ============================================================================

func TestToAnySlice_EveryBranch(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want []interface{}
	}{
		{"[]interface{} passes through",
			[]interface{}{"a", 1}, []interface{}{"a", 1}},
		{"[]map[string]interface{} wraps each",
			[]map[string]interface{}{{"k": "v"}}, []interface{}{map[string]interface{}{"k": "v"}}},
		{"[]string", []string{"a", "b"}, []interface{}{"a", "b"}},
		{"[]int promotes to int64", []int{1, 2}, []interface{}{int64(1), int64(2)}},
		{"[]int64", []int64{3, 4}, []interface{}{int64(3), int64(4)}},
		{"[]float64", []float64{1.5}, []interface{}{1.5}},
		{"[]float32 widens to float64", []float32{1.5}, []interface{}{float64(1.5)}},
		{"[]bool", []bool{true, false}, []interface{}{true, false}},
		{"non-slice returns nil", "scalar", nil},
		{"nil returns nil", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := toAnySlice(tc.in)
			require.True(t, reflect.DeepEqual(tc.want, got),
				"want=%v got=%v", tc.want, got)
		})
	}

	t.Run("reflect fallback for custom slice type", func(t *testing.T) {
		type custom []uint32
		got := toAnySlice(custom{1, 2, 3})
		require.Equal(t, []interface{}{uint32(1), uint32(2), uint32(3)}, got)
	})
}

// ============================================================================
// resolveCosineQueryVector — every type arm reachable without an embedder.
// ============================================================================

func TestResolveCosineQueryVector_AllNonEmbedderArms(t *testing.T) {
	e := freshExecutorForOperators(t)
	ctx := context.Background()

	t.Run("float32 slice literal as []", func(t *testing.T) {
		got, ok := e.resolveCosineQueryVector(ctx, "[0.1, 0.2, 0.3]")
		require.True(t, ok)
		require.Len(t, got, 3)
		// Values should match within float32 precision.
		require.InDelta(t, 0.1, got[0], 1e-6)
	})

	t.Run("empty literal vector returns empty,false", func(t *testing.T) {
		got, ok := e.resolveCosineQueryVector(ctx, "[]")
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("empty string returns nil,false (no embedder)", func(t *testing.T) {
		got, ok := e.resolveCosineQueryVector(ctx, `""`)
		require.False(t, ok)
		require.Nil(t, got)
	})

	t.Run("non-empty string with no embedder returns nil,false", func(t *testing.T) {
		got, ok := e.resolveCosineQueryVector(ctx, `"some query text"`)
		require.False(t, ok)
		require.Nil(t, got)
	})
}

// ============================================================================
// buildNegatedCosineQueryExpr — happy path negates each component.
// ============================================================================

func TestBuildNegatedCosineQueryExpr(t *testing.T) {
	e := freshExecutorForOperators(t)
	ctx := context.Background()

	t.Run("empty resolved vector returns false", func(t *testing.T) {
		got, ok := e.buildNegatedCosineQueryExpr(ctx, `""`)
		require.False(t, ok)
		require.Empty(t, got)
	})

	t.Run("populated vector emits negated literal", func(t *testing.T) {
		got, ok := e.buildNegatedCosineQueryExpr(ctx, "[0.1, -0.2, 0.3]")
		require.True(t, ok)
		// formatInlineFloat32Vector emits "[a,b,c]" with shortest float form.
		require.True(t, strings.HasPrefix(got, "["))
		require.True(t, strings.HasSuffix(got, "]"))
		require.Equal(t, 3, strings.Count(got, ",")+1, "must emit 3 components")
		// Sign-flip: first component was 0.1 (positive), so the first
		// element of the negated vector must start with a minus sign.
		first := strings.SplitN(strings.TrimPrefix(got, "["), ",", 2)[0]
		require.True(t, strings.HasPrefix(first, "-"),
			"negated vector should start with -; got %q", got)
	})
}
