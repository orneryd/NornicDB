package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieve's and db.rretrieve's where reads a pattern element's own
// WHERE ((m:Coll WHERE m.id IN $ids)) as a statement does, inside EXISTS and
// COUNT, with literals, parameters and the calling row's variables.
func TestRetrieveWhereReadsPatternElementWhere(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { _ = svc.Close() })
	exec.SetSearchService(svc)
	_, err := exec.Execute(ctx, `CREATE (:Doc {id: 'a'}), (b:Doc {id: 'b'}), (:Src {id: 'sa', document_id: 'a', content: 'walrus notes'}),
		(:Src {id: 'sb', document_id: 'b', content: 'walrus notes'}), (b)-[:MEMBER_OF]->(:Coll {id: 'old'})`, nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))
	params := map[string]interface{}{"ids": []interface{}{"sb", "old"}}

	for _, where := range []string{
		"EXISTS { (:Doc {id: node.document_id})-[:MEMBER_OF]->(m:Coll WHERE m.id IN $ids) }",
		"EXISTS { (:Doc {id: node.document_id})-[:MEMBER_OF]->(m:Coll WHERE m.id IN ['old']) }",
		"EXISTS { MATCH (d:Doc WHERE d.id = node.document_id)-[:MEMBER_OF]->(m:Coll) }",
		"COUNT { (:Doc {id: node.document_id})-[r:MEMBER_OF WHERE r.since IS NULL]->(m:Coll) } > 0",
		"EXISTS { (:Doc {id: node.document_id})-[:MEMBER_OF]->(m:Coll) WHERE m.id IN $ids }",
	} {
		for _, statement := range []string{
			"CALL db.retrieve({query: 'walrus', types: ['Src'], limit: 10, where: $w}) YIELD node RETURN collect(node.id) AS ids",
			"CALL db.rretrieve({query: 'walrus', types: ['Src'], limit: 10, n: 10, max_results: 10, where: $w}) YIELD page RETURN [x IN page.results | x.node.id] AS ids",
		} {
			arguments := map[string]interface{}{"w": where}
			for name, value := range params {
				arguments[name] = value
			}
			result, err := exec.Execute(ctx, statement, arguments)
			require.NoError(t, err, where)
			require.Equal(t, [][]interface{}{{[]interface{}{"sb"}}}, result.Rows, "%s | %s", statement, where)
		}
	}

	// The calling row's variables, inside a pattern element's WHERE too.
	result, err := exec.Execute(ctx, "MATCH (c:Coll {id: 'old'}) WITH c, [c.id] AS scope CALL (c, scope) { CALL db.retrieve({query: 'walrus', types: ['Src'], limit: 10, where: 'EXISTS { (:Doc {id: node.document_id})-[:MEMBER_OF]->(m:Coll WHERE m.id IN scope) }'}) YIELD node RETURN collect(node.id) AS ids } RETURN ids", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"sb"}}}, result.Rows)
}
