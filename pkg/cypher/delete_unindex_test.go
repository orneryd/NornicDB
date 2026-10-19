package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node deleted through Cypher leaves the property index, on the
// auto-commit and the explicit-transaction delete paths, under a plain index
// and a uniqueness constraint. Index-backed reads, MERGE and re-creating the
// value then answer as if the node never existed.
func TestDeletedNodeLeavesPropertyIndex(t *testing.T) {
	for _, schema := range []string{
		"CREATE INDEX del_l_p FOR (n:L) ON (n.p)",
		"CREATE CONSTRAINT del_l_p_unique FOR (n:L) REQUIRE n.p IS UNIQUE",
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", schema, explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delidx"))
				ctx := context.Background()
				run := func(query string) [][]interface{} {
					t.Helper()
					result, err := exec.Execute(ctx, query, map[string]interface{}{"v": "v1"})
					require.NoError(t, err, query)
					return result.Rows
				}
				run(schema)
				run("CREATE (:L {p: 'v1'}), (:L {p: 'v2'})")
				indexSchema := exec.storage.GetSchema()
				require.Len(t, indexSchema.PropertyIndexLookup("L", "p", "v1"), 1)

				if explicit {
					run("BEGIN")
				}
				run("MATCH (n:L {p: $v}) DELETE n")
				if explicit {
					run("COMMIT")
				}

				assert.Empty(t, indexSchema.PropertyIndexLookup("L", "p", "v1"), "the deleted node stays in the index")
				assert.Empty(t, run("MATCH (n:L {p: $v}) RETURN n.p"))
				assert.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH (n:L {p: $v}) RETURN count(n)"))
				assert.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:L) WHERE n.p STARTS WITH 'v' RETURN count(n)"))
				assert.Equal(t, [][]interface{}{{"v2"}}, run("MATCH (n:L) WHERE n.p IS NOT NULL RETURN n.p ORDER BY n.p"))
				assert.Equal(t, [][]interface{}{{"v1"}}, run("MERGE (n:L {p: $v}) RETURN n.p"))
				assert.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:L {p: $v}) RETURN count(n)"))
			})
		}
	}
}
