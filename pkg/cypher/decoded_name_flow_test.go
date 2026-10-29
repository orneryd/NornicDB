package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameVectorRegistry(t *testing.T) {
	for _, tc := range []struct {
		name, labelA, propertyA, labelB, propertyB string
	}{
		{"surrounding property spaces", "Doc", "embedding", "Doc", " embedding "},
		{"whitespace label", "Doc", "embedding", " ", "embedding"},
		{"whitespace property", "Doc", "embedding", "Doc", " "},
		{"two spaces and tab", "  ", "\t", "\t", "  "},
		{"delimiter key framing", "Doc|part", "embedding", "Doc", "part|embedding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_registry"))
			ctx := context.Background()
			for _, index := range []struct{ name, label, property string }{{"plain", tc.labelA, tc.propertyA}, {"spaced", tc.labelB, tc.propertyB}} {
				query := fmt.Sprintf("CREATE VECTOR INDEX %s FOR (n:%s) ON (n.%s) OPTIONS {indexConfig: {`vector.dimensions`: 2}}", index.name, cyphertext.QuoteName(index.label), cyphertext.QuoteName(index.property))
				_, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			shown, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name, labelsOrTypes, properties RETURN name, labelsOrTypes, properties ORDER BY name", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"plain", []string{tc.labelA}, []string{tc.propertyA}}, {"spaced", []string{tc.labelB}, []string{tc.propertyB}}}, shown.Rows)
			stats := exec.GetVectorRegistry().Stats()
			require.Len(t, stats, 2, "distinct decoded identities need distinct registry spaces")
			require.NotEqual(t, stats[0].KeyHash, stats[1].KeyHash, "the public key representation must distinguish the exact tuple")
			actual := [][2]string{{stats[0].Key.Type, stats[0].Key.VectorName}, {stats[1].Key.Type, stats[1].Key.VectorName}}
			require.ElementsMatch(t, [][2]string{{tc.labelA, tc.propertyA}, {tc.labelB, tc.propertyB}}, actual)
			_, err = exec.Execute(ctx, "DROP INDEX plain", nil)
			require.NoError(t, err)
			stats = exec.GetVectorRegistry().Stats()
			require.Len(t, stats, 1, "dropping one schema index must retain the other space")
			require.Equal(t, tc.labelB, stats[0].Key.Type)
			require.Equal(t, tc.propertyB, stats[0].Key.VectorName)
			_, exists := exec.storage.GetSchema().GetVectorIndex("spaced")
			require.True(t, exists)
		})
	}
}

func TestDecodedNameConstraintRegistry(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "decoded_constraint_key"))
	ctx := context.Background()
	for _, query := range []string{
		"CREATE CONSTRAINT left_unique FOR (n:`A:B`) REQUIRE n.C IS UNIQUE",
		"CREATE CONSTRAINT right_unique FOR (n:A) REQUIRE n.`B:C` IS UNIQUE",
		"CREATE (:`A:B` {C:1})",
		"CREATE (:A {`B:C`:1})",
		"DROP CONSTRAINT left_unique",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	_, err = exec.Execute(ctx, "CREATE (:A {`B:C`:1})", nil)
	require.Error(t, err, "the surviving exact constraint still enforces its own property")
	_, err = exec.Execute(ctx, "CREATE (:`A:B` {C:1})", nil)
	require.NoError(t, err, "the dropped, differently framed identity no longer enforces")
}

func TestDecodedNameExplainIndexComparison(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_explain"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX exact_space_label FOR (n:`Doc `) ON (n.value)", nil)
	require.NoError(t, err)
	for _, tc := range []struct{ label, status string }{{"Doc ", "available"}, {"Doc", "no_index_for_label"}, {" ", "no_index_for_label"}} {
		t.Run(tc.label, func(t *testing.T) {
			result, err := exec.Execute(ctx, "EXPLAIN MATCH (n:"+cyphertext.QuoteName(tc.label)+") RETURN n", nil)
			require.NoError(t, err)
			plan, ok := result.Metadata["plan"].(*ExecutionPlan)
			require.True(t, ok)
			var scan *PlanOperator
			var visit func(*PlanOperator)
			visit = func(op *PlanOperator) {
				if op.Arguments["label"] == tc.label {
					scan = op
				}
				for _, child := range op.Children {
					visit(child)
				}
			}
			visit(plan.Root)
			require.NotNil(t, scan, "public EXPLAIN must retain the decoded query label")
			require.Equal(t, tc.status, scan.Arguments["indexStatus"])
		})
	}
}

// DatabaseManager admits exact nonempty database names; NamespacedEngine
// supplies those identities to the shared registry used by public DDL.
func TestDecodedNameVectorRegistryDatabaseIdentity(t *testing.T) {
	registry := vectorspace.NewIndexRegistry()
	names := []string{"db", " db ", "DB", " ", "\t"}
	var executors []*StorageExecutor
	for _, name := range names {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), name))
		executor.SetVectorRegistry(registry)
		_, err := executor.Execute(context.Background(), "CREATE VECTOR INDEX own FOR (n:DirectDB) ON (n.embedding) OPTIONS {indexConfig:{`vector.dimensions`:2}}", nil)
		require.NoError(t, err)
		executors = append(executors, executor)
	}
	stats := registry.Stats()
	require.Len(t, stats, len(names), "independent database identities need independent spaces")
	var actual []string
	for _, space := range stats {
		actual = append(actual, space.Key.DB)
	}
	require.ElementsMatch(t, names, actual)
	_, err := executors[0].Execute(context.Background(), "DROP INDEX own", nil)
	require.NoError(t, err)
	stats = registry.Stats()
	require.Len(t, stats, len(names)-1, "dropping one database index must retain the other database spaces")
	actual = nil
	for _, space := range stats {
		actual = append(actual, space.Key.DB)
	}
	require.ElementsMatch(t, names[1:], actual)
}
