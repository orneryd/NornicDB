package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDirectNameIndexedPropertyCase(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "scan", true: "index"}[indexed], func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "direct-index-case")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:DirectCase {id:1,RANK:1}), (:DirectCase {id:2,rank:2,RANK:2})", nil)
			require.NoError(t, err)
			if indexed {
				_, err = exec.Execute(ctx, "CREATE RANGE INDEX direct_case_rank FOR (n:DirectCase) ON (n.rank)", nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, "MATCH (n:DirectCase) WHERE n.RANK IS NOT NULL RETURN n.id AS id ORDER BY n.rank LIMIT 10", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2, "non-null RANK must not prove non-null rank")
			result, err = exec.Execute(ctx, "MATCH (n:DirectCase) WHERE n.RANK IS NOT NULL AND n.rank IS NOT NULL RETURN n.id AS id ORDER BY n.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		})
	}
}

func TestDirectNameTraversalWildcard(t *testing.T) {
	for _, name := range []string{"ordinary", " ", "  ", "\t", "a`b", "`a`"} {
		t.Run(name, func(t *testing.T) {
			exec := setupTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:DirectWild)-[:WILD]->(:DirectEnd)", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH ("+cyphertext.QuoteName(name)+":DirectWild)-[:WILD]->(end:DirectEnd) RETURN *", nil)
			require.NoError(t, err)
			require.Contains(t, result.Columns, name)
			require.Len(t, result.Rows, 1)
			for i, column := range result.Columns {
				if column == name {
					require.IsType(t, &storage.Node{}, result.Rows[0][i])
				}
			}
		})
	}
}

func TestDirectNameCompoundScopeCase(t *testing.T) {
	exec := setupTestExecutor(t)
	ctx := context.Background()
	params := map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": int64(1)}}, "ROWS": []interface{}{map[string]interface{}{"id": int64(2)}}}
	_, err := exec.Execute(ctx, "UNWIND $rows AS row MERGE (:DirectLower {id:row.id}) WITH $ROWS AS items UNWIND items AS row MERGE (:DirectUpper {id:row.id})", params)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:DirectUpper) RETURN n.id AS id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

func TestDirectNameCreateLabelIdentity(t *testing.T) {
	for _, label := range []string{"Review", "review", " Review ", " "} {
		t.Run(label, func(t *testing.T) {
			exec := setupTestExecutor(t)
			ctx := context.Background()
			result, err := exec.Execute(ctx, "CREATE (a:Review), (b:DirectTarget) RETURN id(a) AS a,id(b) AS b", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (a:"+cyphertext.QuoteName(label)+"),(b:DirectTarget) WHERE id(a)=$a AND id(b)=$b CREATE (a)-[:DIRECT_CHECK]->(b)", map[string]interface{}{"a": result.Rows[0][0], "b": result.Rows[0][1]})
			require.NoError(t, err)
			result, err = exec.Execute(ctx, "MATCH ()-[r:DIRECT_CHECK]->() RETURN count(r) AS c", nil)
			require.NoError(t, err)
			want := int64(0)
			if label == "Review" {
				want = 1
			}
			require.Equal(t, [][]interface{}{{want}}, result.Rows)
		})
	}
}

func TestDirectNameGDSCatalogTuple(t *testing.T) {
	ctx := context.Background()
	names := []struct{ database, name string }{{"direct-gds", "boundary\x00projection"}, {"direct-gds\x00boundary", "projection"}}
	for _, entry := range names {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), entry.database))
		_, err := exec.Execute(ctx, "CREATE (:DirectGDS)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CALL gds.graph.project($name, '*', '*')", map[string]interface{}{"name": entry.name})
		require.NoError(t, err, "distinct database/name tuple must not be an existing graph")
		t.Cleanup(func() {
			_, _ = exec.Execute(ctx, "CALL gds.graph.drop($name)", map[string]interface{}{"name": entry.name})
		})
	}
}
