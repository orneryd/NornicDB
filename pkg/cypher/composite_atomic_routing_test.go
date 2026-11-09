package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCompositeSchemaTargetPrecedesAtomicCapability(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			exec, mgr := compositeTestFixture(t)
			root := newCompositeTestExecutor(t, mgr, "comp", &testDatabaseManagerAdapter{manager: mgr})
			constituent, err := mgr.GetStorage("shard_a")
			require.NoError(t, err)
			before := constituent.GetSchema().GetIndexes()
			queries := []string{
				"CREATE INDEX root_idx FOR (n:Item) ON (n.id)",
				"CREATE RANGE INDEX root_range FOR (n:Item) ON (n.id)",
				"CREATE TEXT INDEX root_text FOR (n:Item) ON (n.name)",
				"CREATE POINT INDEX root_point FOR (n:Item) ON (n.location)",
				"CREATE VECTOR INDEX root_vector FOR (n:Item) ON (n.embedding)",
				"CREATE FULLTEXT INDEX root_fulltext FOR (n:Item) ON EACH [n.name]",
				"CREATE LOOKUP INDEX root_lookup FOR (n) ON EACH labels(n)",
				"CREATE CONSTRAINT root_unique FOR (n:Item) REQUIRE n.id IS UNIQUE",
				"DROP INDEX root_idx",
				"DROP CONSTRAINT root_unique",
			}
			for _, query := range queries {
				t.Run(query, func(t *testing.T) {
					result, err := root.Execute(context.Background(), query, nil)
					require.ErrorContains(t, err, "Neo.ClientError.Statement.NotAllowed")
					require.ErrorContains(t, err, "constituent target")
					require.Nil(t, result)
					require.ElementsMatch(t, before, constituent.GetSchema().GetIndexes(), "rejection must not mutate a constituent")
					count, err := constituent.NodeCount(t.Context())
					require.NoError(t, err)
					require.Zero(t, count)
				})
			}
			deniedCalls := 0
			denied := WithPermissionChecker(context.Background(), func(string) bool { deniedCalls++; return false })
			result, err := root.Execute(denied, queries[0], nil)
			var permission *PermissionDeniedError
			require.ErrorAs(t, err, &permission, "authorization must precede target and capability errors")
			require.Nil(t, result)
			require.Positive(t, deniedCalls)
			require.ElementsMatch(t, before, constituent.GetSchema().GetIndexes())
			unsupported := &capabilityHiddenFailingCreateEngine{Engine: constituent}
			_, err = NewStorageExecutor(unsupported).Execute(denied, "CREATE (:DeniedAtomicWrite)", nil)
			require.ErrorAs(t, err, &permission, "authorization must also precede unsupported local write capability")
			require.Zero(t, unsupported.calls)
			count, err := constituent.NodeCount(t.Context())
			require.NoError(t, err)
			require.Zero(t, count)

			// The same operation remains executable on the selected real constituent.
			_, err = exec.Execute(context.Background(), "USE comp.a CREATE INDEX constituent_idx FOR (n:Item) ON (n.id)", nil)
			require.NoError(t, err)
			_, exists := constituent.GetSchema().GetCompositeIndex("constituent_idx")
			require.True(t, exists)
			_, err = exec.Execute(context.Background(), "USE comp.a DROP INDEX constituent_idx", nil)
			require.NoError(t, err)
			_, exists = constituent.GetSchema().GetCompositeIndex("constituent_idx")
			require.False(t, exists)
		})
	}
}
