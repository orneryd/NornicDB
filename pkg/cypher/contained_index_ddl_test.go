package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// showIndexRow returns SHOW INDEXES' row for name as column → value.
func showIndexRow(t *testing.T, exec *StorageExecutor, ctx context.Context, name string) map[string]interface{} {
	t.Helper()
	result, err := exec.Execute(ctx, "SHOW INDEXES YIELD *", nil)
	require.NoError(t, err)
	for _, row := range result.Rows {
		entry := make(map[string]interface{}, len(result.Columns))
		for i, column := range result.Columns {
			entry[column] = row[i]
		}
		if entry["name"] == name {
			return entry
		}
	}
	return nil
}

// CREATE CONTAINED INDEX, a NornicDB extension accepted by both parsers,
// fills the index from the label's existing nodes, is listed by SHOW
// INDEXES with a createStatement that recreates it, follows IF NOT EXISTS
// and the index-name conflict codes, and is dropped by DROP INDEX; in
// auto-commit and in an explicit schema transaction.
func TestCreateContainedIndexDDL(t *testing.T) {
	const create = "CREATE CONTAINED INDEX identity_keys FOR (n:Identity) ON EACH [n.name_key, n.alias_keys]"
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "contained_ddl"))
			ctx := context.Background()
			run := func(query string) error {
				t.Helper()
				_, err := exec.Execute(ctx, query, nil)
				return err
			}
			ddl := func(query string) error {
				t.Helper()
				if !explicit {
					return run(query)
				}
				require.NoError(t, run("BEGIN"))
				if err := run(query); err != nil {
					_ = run("ROLLBACK")
					return err
				}
				return run("COMMIT")
			}
			require.NoError(t, run("CREATE (:Identity {name_key: 'acme', alias_keys: ['acme corp', 'ac']}), (:Identity {name_key: 'globex'}), (:Doc {name_key: 'acme'})"))
			require.NoError(t, ddl(create))

			idx, ok := exec.storage.GetSchema().GetContainedIndex("identity_keys")
			require.True(t, ok)
			require.True(t, idx.Filled())
			require.Len(t, idx.LookupNodes("the acme corp office"), 1, "the existing Identity node is filled, the Doc node isn't")

			row := showIndexRow(t, exec, ctx, "identity_keys")
			require.NotNil(t, row)
			require.Equal(t, "CONTAINED", row["type"])
			require.Equal(t, "NODE", row["entityType"])
			require.Equal(t, []string{"Identity"}, row["labelsOrTypes"])
			require.Equal(t, []string{"name_key", "alias_keys"}, row["properties"])
			require.Equal(t, "contained-1.0", row["indexProvider"])
			statement, _ := row["createStatement"].(string)
			require.Equal(t, "CREATE CONTAINED INDEX `identity_keys` FOR (n:`Identity`) ON EACH [n.`name_key`, n.`alias_keys`]", statement)

			require.NoError(t, ddl("CREATE CONTAINED INDEX identity_keys IF NOT EXISTS FOR (n:Identity) ON EACH [n.name_key, n.alias_keys]"))
			err := ddl(create)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists")
			err = ddl("CREATE CONTAINED INDEX identity_keys FOR (n:Identity) ON EACH [n.other]")
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Schema.IndexWithNameAlreadyExists")

			require.NoError(t, ddl("DROP INDEX identity_keys"))
			require.Nil(t, showIndexRow(t, exec, ctx, "identity_keys"))
			require.NoError(t, ddl(statement), "the createStatement recreates the index")
			idx, ok = exec.storage.GetSchema().GetContainedIndex("identity_keys")
			require.True(t, ok)
			require.Len(t, idx.LookupNodes("acme"), 1)

			require.NoError(t, ddl("CREATE CONTAINED INDEX FOR (n:Identity) ON EACH [n.name_key]"))
			require.NotNil(t, showIndexRow(t, exec, ctx, "index_Identity_name_key_contained"), "an unnamed index gets the default name")

			for _, invalid := range []string{
				"CREATE CONTAINED INDEX rel_keys FOR ()-[r:LINK]-() ON EACH [r.key]",
				"CREATE CONTAINED INDEX two_labels FOR (n:Identity|Doc) ON EACH [n.name_key]",
				"CREATE CONTAINED INDEX no_props FOR (n:Identity) ON EACH []",
			} {
				require.Error(t, ddl(invalid), invalid)
			}
		})
	}
}

// A contained index created and rolled back in an explicit transaction is
// gone after the rollback.
func TestCreateContainedIndexRolledBack(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "contained_rollback"))
	ctx := context.Background()
	for _, query := range []string{"BEGIN", "CREATE CONTAINED INDEX k FOR (n:Identity) ON EACH [n.name_key]", "ROLLBACK"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	_, ok := exec.storage.GetSchema().GetContainedIndex("k")
	require.False(t, ok)
}
