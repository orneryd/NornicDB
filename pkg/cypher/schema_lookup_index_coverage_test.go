package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResidualLookupIndexParserModes(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := framingExec(t, "lookup")
			for _, query := range []string{
				"DROP INDEX " + storage.DefaultNodeLookupIndexName,
				"DROP INDEX " + storage.DefaultRelationshipLookupIndexName,
				"CREATE LOOKUP INDEX `lookup_node` FOR (n) ON EACH labels(n)",
				"CREATE LOOKUP INDEX lookup_relationship FOR ()-[r]-() ON EACH type(r)",
				"CREATE LOOKUP INDEX lookup_node IF NOT EXISTS FOR (n) ON EACH labels(n)",
				"CREATE LOOKUP INDEX lookup_relationship IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)",
			} {
				_, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
			}
			result, err := exec.Execute(context.Background(), "SHOW INDEXES YIELD name, type, entityType WHERE type = 'LOOKUP' RETURN name, entityType ORDER BY name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"lookup_node", "NODE"}, {"lookup_relationship", "RELATIONSHIP"}}, result.Rows)
		})
	}
}

// TestCreateLookupIndexNamesAndConflicts: CREATE LOOKUP INDEX takes a
// backtick-quoted name with escaped backticks; a name another index has is
// IndexWithNameAlreadyExists, which IF NOT EXISTS makes a no-op, as in
// Neo4j 5.
func TestCreateLookupIndexNamesAndConflicts(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	run := func(query string) (*ExecuteResult, error) { return exec.Execute(ctx, query, nil) }

	_, err := run("DROP INDEX " + storage.DefaultNodeLookupIndexName)
	require.NoError(t, err)
	_, err = run("CREATE LOOKUP INDEX `node``lookup` FOR (n) ON EACH labels(n)")
	require.NoError(t, err)
	result, err := run("SHOW LOOKUP INDEXES YIELD name, entityType WHERE entityType = 'NODE'")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"node`lookup", "NODE"}}, result.Rows)

	_, err = run("DROP INDEX " + storage.DefaultRelationshipLookupIndexName)
	require.NoError(t, err)
	_, err = run("CREATE INDEX taken FOR (n:Person) ON (n.name)")
	require.NoError(t, err)
	_, err = run("CREATE LOOKUP INDEX taken FOR ()-[r]-() ON EACH type(r)")
	require.Error(t, err)
	require.Contains(t, statusText(err), "IndexWithNameAlreadyExists")
	_, err = run("CREATE LOOKUP INDEX taken IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)")
	require.NoError(t, err)
	result, err = run("SHOW LOOKUP INDEXES YIELD name")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"node`lookup"}}, result.Rows)
}

func TestMonster531LookupIndexAdmission(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			for _, test := range []struct {
				name, setup, indexName, pattern, code string
			}{
				{"equivalent node lookup", "", storage.DefaultNodeLookupIndexName, "(n) ON EACH labels(n)", "IndexAlreadyExists"},
				{"lookup name on another entity", "", storage.DefaultNodeLookupIndexName, "()-[r]-() ON EACH type(r)", "IndexAlreadyExists"},
				{"constraint name", "CREATE CONSTRAINT taken FOR (n:T) REQUIRE n.id IS UNIQUE", "taken", "(n) ON EACH labels(n)", "ConstraintWithNameAlreadyExists"},
			} {
				t.Run(test.name, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
					ctx := context.Background()
					if test.setup != "" {
						_, err := exec.Execute(ctx, test.setup, nil)
						require.NoError(t, err)
					}
					before := exec.storage.GetSchema().GetIndexes()
					_, err := exec.Execute(ctx, "CREATE LOOKUP INDEX "+test.indexName+" FOR "+test.pattern, nil)
					require.Error(t, err)
					require.Contains(t, statusText(err), test.code)
					require.ElementsMatch(t, before, exec.storage.GetSchema().GetIndexes())
					_, err = exec.Execute(ctx, "CREATE LOOKUP INDEX "+test.indexName+" IF NOT EXISTS FOR "+test.pattern, nil)
					require.NoError(t, err)
					require.ElementsMatch(t, before, exec.storage.GetSchema().GetIndexes())
				})
			}
		})
	}
}

// TestCreateLookupIndexReportsPersistFailure: a schema that can't be
// persisted fails CREATE LOOKUP INDEX with the persist error.
func TestCreateLookupIndexReportsPersistFailure(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "DROP INDEX "+storage.DefaultNodeLookupIndexName, nil)
	require.NoError(t, err)
	failed := errors.New("persist failed")
	exec.storage.GetSchema().SetPersister(func(*storage.SchemaDefinition) error { return failed })
	_, err = exec.Execute(ctx, "CREATE LOOKUP INDEX FOR (n) ON EACH labels(n)", nil)
	require.ErrorIs(t, err, failed)
}

// TestParseCreateLookupIndexRejects: statements parseCreateLookupIndex does
// not read as CREATE LOOKUP INDEX.
func TestParseCreateLookupIndexRejects(t *testing.T) {
	for _, statement := range []string{
		"CREATE INDEX FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX `unterminated FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX `` FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX -x FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX IF EXISTS FOR (n) ON EACH labels(n)",
		"CREATE LOOKUP INDEX FOR (n) ON EACH labels(m)",
	} {
		_, _, _, ok := parseCreateLookupIndex(statement)
		require.False(t, ok, statement)
	}
	name, ifNotExists, entityType, ok := parseCreateLookupIndex("CREATE LOOKUP INDEX `a``b` IF NOT EXISTS FOR ()-[r]-() ON EACH type(r)")
	require.True(t, ok)
	require.Equal(t, "a`b", name)
	require.True(t, ifNotExists)
	require.Equal(t, storage.ConstraintEntityRelationship, entityType)
}

// TestRowPredicatePlanEdges: operands and predicates the row predicate plan
// doesn't plan, and parts it hands back to the row evaluator.
func TestRowPredicatePlanEdges(t *testing.T) {
	for _, text := range []string{"", "[1, 2]", "$1x", "1 + 2"} {
		_, ok := parseRowOperand(text)
		require.False(t, ok, text)
	}
	operand, ok := parseRowOperand("n.a")
	require.True(t, ok)
	_, bound := operand.resolve(map[string]interface{}{})
	require.False(t, bound)

	_, planned := planRowPredicatePart("  ")
	require.False(t, planned)
	require.Nil(t, planRowPredicate("a = 1 XOR b = 2"))

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	plan := planRowPredicate("x IN list")
	require.NotNil(t, plan)
	require.True(t, exec.evaluateRowPredicatePlan(ctx, plan, map[string]interface{}{"x": int64(1), "list": []interface{}{int64(1)}}))
	// The haystack isn't bound in the row: the part is evaluated as text.
	require.False(t, exec.evaluateRowPredicatePlan(ctx, plan, map[string]interface{}{"x": int64(1)}))
}
