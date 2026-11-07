package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A registered WRITE procedure changes state through no clause kind, so the
// AST's read-only answer comes from the statement-effect owner
// (readStatementExecutionContract) rather than from a scan of the clauses the
// builder emits.
func registerASTEffectProcedure(t *testing.T, registry *ProcedureRegistry, name string, mode ProcedureMode) {
	t.Helper()
	handler := func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		t.Fatal("effect metadata must never execute a procedure")
		return nil, nil
	}
	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: name, Mode: mode}, handler))
}

func TestASTEffectMetadataRegisteredWriteProcedure(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	const query = "CALL ast_effect_fixture.write() YIELD value RETURN value"
	registerASTEffectProcedure(t, registry, "ast_effect_fixture.write", ProcedureModeWrite)

	built, err := NewASTBuilder().Build(t.Context(), query)
	require.NoError(t, err)
	require.False(t, built.IsReadOnly, "a registered WRITE procedure is a write however its clauses look")

	analyzer := NewQueryAnalyzer(8)
	info := mustAnalyze(t, analyzer, t.Context(), query)
	require.True(t, info.Writes())
	cached, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.False(t, cached.IsReadOnly, "the cached AST reports the same effect owner's answer")
	require.Equal(t, built.IsReadOnly, cached.IsReadOnly)
}

func TestASTEffectMetadataRegistryReplacementRepublishesAST(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	const query = "CALL ast_effect_fixture.swap() YIELD value RETURN value"
	registerASTEffectProcedure(t, registry, "ast_effect_fixture.swap", ProcedureModeRead)

	analyzer := NewQueryAnalyzer(8)
	info := mustAnalyze(t, analyzer, t.Context(), query)
	before, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.True(t, before.IsReadOnly)
	warm, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.Same(t, before, warm, "an unchanged warm hit keeps the published AST")

	registerASTEffectProcedure(t, registry, "ast_effect_fixture.swap", ProcedureModeWrite)
	require.Same(t, info, mustAnalyze(t, analyzer, t.Context(), query), "the registry generation refreshes the cached analysis")
	require.True(t, info.Writes())

	after, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.False(t, after.IsReadOnly)
	require.NotSame(t, before, after, "a changed effect publishes a new AST")
	require.True(t, before.IsReadOnly, "a retained AST keeps the answer it was published with")
	require.Equal(t, before.Clauses, after.Clauses, "the structural result is unchanged")

	replaced, err := NewASTBuilder().Build(t.Context(), query)
	require.NoError(t, err)
	require.False(t, replaced.IsReadOnly)

	registerASTEffectProcedure(t, registry, "ast_effect_fixture.swap", ProcedureModeRead)
	mustAnalyze(t, analyzer, t.Context(), query)
	restored, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.True(t, restored.IsReadOnly)
	require.False(t, after.IsReadOnly, "the write AST stays immutable")
}

func TestASTEffectMetadataCategories(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	registerASTEffectProcedure(t, registry, "ast_effect_fixture.read", ProcedureModeRead)
	registerASTEffectProcedure(t, registry, "ast_effect_fixture.schema", ProcedureModeSchema)

	analyzer := NewQueryAnalyzer(32)
	for _, tc := range []struct {
		name     string
		query    string
		readOnly bool
	}{
		{"write clause", "CREATE (n:ASTEffect {id: 1})", false},
		{"read clause", "MATCH (n:ASTEffect) RETURN n", true},
		{"schema ddl", "CREATE INDEX ast_effect_idx FOR (n:ASTEffect) ON (n.id)", false},
		{"schema procedure", "CALL ast_effect_fixture.schema() YIELD value RETURN value", false},
		{"read procedure", "CALL ast_effect_fixture.read() YIELD value RETURN value", true},
		{"unknown procedure", "CALL ast_effect_fixture.missing() YIELD value RETURN value", false},
		{"quoted write keywords", "RETURN 'CREATE (n) DELETE n SET n.x = 1' AS text", true},
		{"quoted keyword in predicate", "MATCH (n) WHERE n.`create` = 'DELETE' RETURN n", true},
		{"foreach write", "MATCH (n) FOREACH (x IN [1, 2] | CREATE (:ASTEffectForeach {v: x}))", false},
		{"read then write", "MATCH (n:ASTEffect) SET n.touched = true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := NewASTBuilder().Build(t.Context(), tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.readOnly, built.IsReadOnly, "public Build reports the effect owner's answer")

			info := mustAnalyze(t, analyzer, t.Context(), tc.query)
			cached, err := info.GetAST(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.readOnly, cached.IsReadOnly, "the cached AST reports the effect owner's answer")
			require.Equal(t, !tc.readOnly, info.Writes())
		})
	}
}

func TestASTEffectMetadataUnreadableTextStaysConservative(t *testing.T) {
	// The effect owner cannot read this text; its syntax diagnostic belongs to
	// admission, so the structural result is preserved with a write effect.
	built, err := NewASTBuilder().Build(t.Context(), "MATCH (n:ASTEffectPartial")
	require.NoError(t, err)
	require.NotEmpty(t, built.Clauses, "the structural result is preserved")
	require.False(t, built.IsReadOnly, "unreadable text is never published as a read")
}

func TestASTEffectMetadataCancellation(t *testing.T) {
	const query = "MATCH (n:ASTEffectCancel) RETURN n"

	t.Run("cold GetAST", func(t *testing.T) {
		info := &QueryInfo{rawQuery: query}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		ast, err := info.GetAST(ctx)
		require.Same(t, context.Canceled, err)
		require.Nil(t, ast)
		require.False(t, info.HasAST())

		healthy, err := info.GetAST(t.Context())
		require.NoError(t, err)
		require.True(t, healthy.IsReadOnly)
		require.True(t, info.HasAST())
	})

	t.Run("warm GetAST", func(t *testing.T) {
		info := &QueryInfo{rawQuery: query}
		built, err := info.GetAST(t.Context())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		ast, err := info.GetAST(ctx)
		require.Same(t, context.Canceled, err)
		require.Nil(t, ast)
		again, err := info.GetAST(t.Context())
		require.NoError(t, err)
		require.Same(t, built, again, "a canceled read leaves the published AST in place")
	})

	t.Run("public Build", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		ast, err := NewASTBuilder().Build(ctx, query)
		require.Same(t, context.Canceled, err)
		require.Nil(t, ast)
	})
}

func TestASTEffectMetadataCachedASTKeepsResolvedStateAndWarmAllocations(t *testing.T) {
	analyzer := NewQueryAnalyzer(8)
	const query = "MATCH (n:ASTEffectCold) RETURN n LIMIT 2"
	info := mustAnalyze(t, analyzer, t.Context(), query)
	effect := info.effect.Load()
	require.NotNil(t, effect)

	built, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.Same(t, effect, info.effect.Load(), "GetAST reuses the analysis's resolved effect")

	warm, err := info.GetAST(t.Context())
	require.NoError(t, err)
	require.Same(t, built, warm, "an unchanged warm hit returns the published AST")
	require.Same(t, effect, info.effect.Load())

	allocs := testing.AllocsPerRun(20, func() {
		_, _ = info.GetAST(t.Context())
	})
	require.Zero(t, allocs, "an unchanged warm hit allocates nothing")
}
