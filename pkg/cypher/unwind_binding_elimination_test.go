package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// admitUnwindStatement is the one admission path the set-based rewrite needs:
// the statement program's binding identities and reads. Every direct test
// below runs the same preparation dispatch runs before a physical operator.
func admitUnwindStatement(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) (context.Context, topLevelUnwindPlan, string, bool) {
	t.Helper()
	clauses, ok := canExecuteAsPipeline(t.Context(), statement)
	require.True(t, ok, statement)
	ctx, err := exec.bindPipelineProgram(ctx, clauses, nil)
	require.NoError(t, err)
	plan, err := exec.prepareTopLevelUnwind(ctx, statement)
	require.NoError(t, err)
	rewritten, admitted := exec.rewriteUnwindCorrelations(ctx, plan)
	return ctx, plan, rewritten, admitted
}

func TestUnwindBodySpans(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	source := "  UNWIND [1] AS row MATCH (n:X) WHERE n.id = row RETURN count(n)  "
	plan, err := exec.prepareTopLevelUnwind(ctx, source)
	require.NoError(t, err)
	require.Equal(t, "row", source[plan.aliasStart:plan.aliasEnd])
	require.Equal(t, "MATCH (n:X) WHERE n.id = row RETURN count(n)", source[plan.bodyOffset:plan.bodyOffset+len(plan.remainder)])

	// A body that is not the statement's trimmed suffix has no spans, so the
	// elimination proof cannot relate body offsets to program reads.
	_, _, _, located := unwindBodySpans(source, "MATCH (n:Other) RETURN count(n)", "row")
	require.False(t, located)
	_, _, _, located = unwindBodySpans("UNWIND [1] AS row", "", "row")
	require.False(t, located)
}

// unwindCorrelationProductOf reads the correlations the set-based rewrite would
// select, through the same admission and reader the rewrite runs.
func unwindCorrelationProductOf(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) (topLevelUnwindPlan, unwindCorrelationProduct) {
	t.Helper()
	clauses, ok := canExecuteAsPipeline(ctx, statement)
	require.True(t, ok, statement)
	ctx, err := exec.bindPipelineProgram(ctx, clauses, nil)
	require.NoError(t, err)
	plan, err := exec.prepareTopLevelUnwind(ctx, statement)
	require.NoError(t, err)
	program, ok := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	require.True(t, ok, statement)
	declaration, ok := unwindDeclaration(program, plan.aliasStart, plan.aliasEnd)
	require.True(t, ok, statement)
	return plan, readUnwindCorrelations(program.syntax, declaration, plan.remainder, plan.bodyOffset)
}

// A correlation's spans are statement-source offsets that end at the binding
// read itself, so the rewrite replaces `lhs = binding` and nothing after it.
func TestUnwindCorrelationSpans(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		statement := "UNWIND ['shared'] AS joinKey MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey AND t.note = 'x = joinKey' RETURN count(*) AS c"
		plan, product := unwindCorrelationProductOf(t, exec, ctx, statement)
		require.Len(t, product.correlations, 2, "the quoted text is not a comparison")
		require.Equal(t, "o.joinKey", product.correlations[0].lhs)
		require.Equal(t, "t.joinKey", product.correlations[1].lhs)
		for _, correlation := range product.correlations {
			require.Equal(t, "joinKey", statement[correlation.readStart:correlation.readEnd])
			require.Equal(t, correlation.readEnd, correlation.end)
		}
		require.Equal(t,
			"MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE ",
			statement[plan.bodyOffset:product.correlations[0].start])

		rewritten, ok := product.rewrite("__unwind_items")
		require.True(t, ok)
		require.Equal(t,
			"MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey IN $__unwind_items AND t.joinKey = o.joinKey AND t.note = 'x = joinKey' RETURN count(*) AS c",
			rewritten)

		// A correlation set with no body address cannot splice the body.
		product.bodyOffset = -1
		_, ok = product.rewrite("__unwind_items")
		require.False(t, ok)
	})
}

// The set-based rewrite is admitted only when the admitted statement program
// proves every read of the UNWIND binding is one of the equalities the rewrite
// replaces. A wildcard projection, an explicit projection or any other read
// keeps the canonical row plan.
func TestUnwindCorrelationEliminationProof(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		admitted  bool
	}{
		{
			name:      "equality only",
			statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN b, count(*) AS total",
			admitted:  true,
		},
		{
			name:      "correlated join",
			statement: "UNWIND ['k1'] AS joinKey MATCH (o:OriginalText) MATCH (t:TranslatedText) WHERE o.joinKey = joinKey AND t.joinKey = joinKey RETURN count(*) AS c",
			admitted:  true,
		},
		{
			name:      "create reads the equality only",
			statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k CREATE (m:Copy {n: b.n}) RETURN count(m) AS c",
			admitted:  true,
		},
		{
			name:      "rebound after with",
			statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k WITH b, 2 AS k RETURN b, k AS k, count(*) AS total",
			admitted:  true,
		},
		{name: "wildcard projection", statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN *, count(*) AS total"},
		{name: "explicit projection", statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN b, k, count(*) AS total"},
		{name: "read outside the equalities", statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k AND k > 0 RETURN b, count(*) AS total"},
		{name: "read in a create map", statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k CREATE (m:Copy {viewed: k}) RETURN count(m) AS c"},
		{name: "aggregated read", statement: "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN count(k) AS c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, _, rewritten, admitted := admitUnwindStatement(t, exec, ctx, test.statement)
				require.Equal(t, test.admitted, admitted, test.statement)
				if test.admitted {
					require.Contains(t, rewritten, "IN $__unwind_items")
				} else {
					require.Empty(t, rewritten)
				}
			})
		})
	}
}

// An unavailable proof declines instead of assuming the binding is dead.
func TestUnwindCorrelationProofRequiresStatementProgram(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	statement := "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN b, count(*) AS total"
	_, plan, _, admitted := admitUnwindStatement(t, exec, ctx, statement)
	require.True(t, admitted)

	_, admitted = exec.rewriteUnwindCorrelations(context.Background(), plan)
	require.False(t, admitted, "no admitted statement program means no proof")

	unlocated := plan
	unlocated.aliasStart, unlocated.aliasEnd, unlocated.bodyOffset = 0, 0, -1
	_, admitted = exec.rewriteUnwindCorrelations(ctx, unlocated)
	require.False(t, admitted, "a plan without statement spans proves nothing")

	foreign := plan
	foreign.aliasStart, foreign.aliasEnd = plan.aliasStart+1, plan.aliasEnd+1
	_, admitted = exec.rewriteUnwindCorrelations(ctx, foreign)
	require.False(t, admitted, "an alias span the program does not hold proves nothing")
}

// Neighboring public controls: a binding that a wildcard, an explicit
// projection or another predicate still needs keeps the canonical row plan and
// the statement's columns, while the proven case keeps its columns too.
func TestUnwindCorrelationNeighborReadsKeepBinding(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		_, err := exec.Execute(ctx, "CREATE (:Target {n:1}),(:Target {n:2})", nil)
		require.NoError(t, err)

		t.Run("wildcard with extra predicate", func(t *testing.T) {
			result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k AND k > 0 RETURN *, count(*) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"b", "k", "total"}, result.Columns)
			require.Len(t, result.Rows, 1)
			node, ok := result.Rows[0][0].(*storage.Node)
			require.True(t, ok)
			require.Equal(t, map[string]any{"n": int64(1)}, node.Properties)
			require.Equal(t, int64(1), result.Rows[0][1])
			require.Equal(t, int64(1), result.Rows[0][2])
		})

		t.Run("explicit binding projection", func(t *testing.T) {
			result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN b, k, count(*) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"b", "k", "total"}, result.Columns)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(1), result.Rows[0][1])
			require.Equal(t, int64(1), result.Rows[0][2])
		})

		t.Run("read outside the equalities", func(t *testing.T) {
			result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k AND k > 0 RETURN b, count(*) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"b", "total"}, result.Columns)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(1), result.Rows[0][1])
		})

		t.Run("proven elimination keeps columns", func(t *testing.T) {
			result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k RETURN b, count(*) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"b", "total"}, result.Columns)
			require.Len(t, result.Rows, 1)
			node, ok := result.Rows[0][0].(*storage.Node)
			require.True(t, ok)
			require.Equal(t, map[string]any{"n": int64(1)}, node.Properties)
			require.Equal(t, int64(1), result.Rows[0][1])
		})

		t.Run("rebound binding after with", func(t *testing.T) {
			result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k WITH b, 2 AS k RETURN b, k AS k, count(*) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"b", "k", "total"}, result.Columns)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(2), result.Rows[0][1])
			require.Equal(t, int64(1), result.Rows[0][2])
		})
	})
}

// The create operator converges on the same proof, not a second scanner: it
// rewrites only when the CREATE reads nothing but the replaced equality.
func TestSetBasedUnwindCreateOperatorUsesEliminationProof(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    string
		creates bool
	}{
		{name: "create reads the equality only", body: "MATCH (b:Target) WHERE b.n=k CREATE (m:Copy {n: b.n}) RETURN count(m) AS c", creates: true},
		{name: "create reads the binding", body: "MATCH (b:Target) WHERE b.n=k CREATE (m:Copy {viewed: k}) RETURN count(m) AS c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				exec, base := newUnitExecutor(t)
				_, err := exec.Execute(base, "CREATE (:Target {n:1})", nil)
				require.NoError(t, err)
				ctx, plan, rewritten, admitted := admitUnwindStatement(t, exec, base, "UNWIND [1] AS k "+test.body)
				require.Equal(t, test.creates, admitted, rewritten)
				result, handled, err := exec.executeSetBasedUnwindCreateOperator(ctx, plan)
				require.NoError(t, err)
				require.Equal(t, test.creates, handled)
				if handled {
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				} else {
					require.Nil(t, result)
				}
			})
		})
	}
}

// The create operator converges on the same proof: a CREATE that reads the
// binding keeps the row plan, so the write still sees the value.
func TestUnwindCorrelationCreateControlsKeepBinding(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		_, err := exec.Execute(ctx, "CREATE (:Target {n:1})", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "UNWIND [1] AS k MATCH (b:Target) WHERE b.n=k CREATE (m:Copy {viewed: k}) RETURN count(m) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

		copies, err := exec.Execute(ctx, "MATCH (m:Copy) RETURN m.viewed AS viewed", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, copies.Rows)
	})
}

// Every right side that is not exactly one read of the declared binding, and
// every comparison an OR, XOR or NOT decides, is not a correlation the rewrite
// owns: substituting there would replace a comparison that means something
// else.
func TestUnwindCorrelationRejectsComparisonsItDoesNotOwn(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "binding on the left", body: "MATCH (b:Target) WHERE joinKey = b.joinKey RETURN count(*) AS c"},
		{name: "longer right side name", body: "UNWIND ['other'] AS joinKeyExtra MATCH (b:Target) WHERE b.joinKey = joinKeyExtra RETURN count(*) AS c"},
		{name: "arithmetic right side", body: "MATCH (b:Target) WHERE b.n = joinKey + 1 RETURN count(*) AS c"},
		{name: "left arithmetic operand", body: "MATCH (b:Target) WHERE b.n = 1 + joinKey RETURN count(*) AS c"},
		{name: "property right side", body: "MATCH (b:Target) WHERE b.n = joinKey.value RETURN count(*) AS c"},
		{name: "subscript right side", body: "MATCH (b:Target) WHERE b.n = joinKey[0] RETURN count(*) AS c"},
		{name: "function right side", body: "MATCH (b:Target) WHERE b.n = toString(joinKey) RETURN count(*) AS c"},
		{name: "chained comparison", body: "MATCH (b:Target) WHERE b.n = joinKey = 1 RETURN count(*) AS c"},
		{name: "not equal", body: "MATCH (b:Target) WHERE b.n <> joinKey RETURN count(*) AS c"},
		{name: "case-different name", body: "UNWIND ['other'] AS JoinKey MATCH (b:Target) WHERE b.n = JoinKey RETURN count(*) AS c"},
		{name: "or alternative", body: "MATCH (b:Target) WHERE b.n = joinKey OR b.n = 1 RETURN count(*) AS c"},
		{name: "xor alternative", body: "MATCH (b:Target) WHERE b.n = joinKey XOR b.n = 1 RETURN count(*) AS c"},
		{name: "negated equality", body: "MATCH (b:Target) WHERE NOT b.n = joinKey RETURN count(*) AS c"},
		{name: "parenthesised negation", body: "MATCH (b:Target) WHERE NOT (b.n = joinKey) RETURN count(*) AS c"},
		{name: "left side is not a property", body: "MATCH (b:Target) WHERE coalesce(b.n, 0) = joinKey RETURN count(*) AS c"},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				statement := "UNWIND ['shared'] AS joinKey " + test.body
				_, product := unwindCorrelationProductOf(t, exec, ctx, statement)
				require.Empty(t, product.correlations, statement)
				_, ok := product.rewrite("__unwind_items")
				require.False(t, ok, statement)
			})
		})
	}
}

// The rows below are read off the fixture by hand (Target n=1,2,3), so they are
// independent of the rewrite. The equalities the rewrite owns keep producing
// them optimized, and every neighbouring comparison keeps the canonical row
// plan's rows instead of a substituted one.
func TestUnwindCorrelationPublicResults(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		rows      [][]interface{}
	}{
		{
			name:      "nested predicate preserves unwind multiplicity",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE EXISTS { MATCH (c:Target) WHERE c.n = k RETURN c } RETURN count(*) AS c",
			rows:      [][]interface{}{{int64(6)}},
		},
		{
			name:      "equality only",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE b.n = k RETURN b.n AS n ORDER BY n",
			rows:      [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name:      "equality only count",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE b.n = k RETURN count(*) AS c",
			rows:      [][]interface{}{{int64(2)}},
		},
		{
			name:      "arithmetic right side",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE b.n = k + 1 RETURN b.n AS n ORDER BY n",
			rows:      [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			name:      "arithmetic right side count",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE b.n = k + 1 RETURN count(*) AS c",
			rows:      [][]interface{}{{int64(2)}},
		},
		{
			name:      "property right side",
			statement: "UNWIND [{v: 1}, {v: 2}] AS k MATCH (b:Target) WHERE b.n = k.v RETURN b.n AS n ORDER BY n",
			rows:      [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name:      "subscript right side",
			statement: "UNWIND [[1], [2]] AS k MATCH (b:Target) WHERE b.n = k[0] RETURN b.n AS n ORDER BY n",
			rows:      [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name:      "case-different declaration",
			statement: "UNWIND [1] AS k UNWIND [2] AS K MATCH (b:Target) WHERE b.n = K RETURN b.n AS n ORDER BY n",
			rows:      [][]interface{}{{int64(2)}},
		},
		{
			name:      "or alternative",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE b.n = k OR b.n = 3 RETURN count(*) AS c",
			rows:      [][]interface{}{{int64(4)}},
		},
		{
			name:      "negated equality",
			statement: "UNWIND [1, 2] AS k MATCH (b:Target) WHERE NOT (b.n = k) RETURN count(*) AS c",
			rows:      [][]interface{}{{int64(4)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE (:Target {n:1}),(:Target {n:2}),(:Target {n:3})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, test.statement, nil)
				require.NoError(t, err)
				require.Equal(t, test.rows, result.Rows, test.statement)
			})
		})
	}
}
