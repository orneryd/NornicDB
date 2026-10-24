package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Physical predicate pushdown may omit terms that read the incoming row. The
// shared access owner must retain that row's original site across each public
// MATCH, correlated CALL and graph-expression entry, including nested scopes.
func TestRelationshipAccessProgramPublicRowScopes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_row_scopes"))
	exec.cache = nil
	for _, query := range []string{
		"CREATE (:RowKey {key:'a'})-[:RowScope {key:'x'}]->(:RowEnd {key:'x'})",
		"MATCH (a:RowKey) CREATE (a)-[:RowScope {key:'y'}]->(:RowEnd {key:'y'})",
		"CREATE RANGE INDEX public_row_scope_key FOR ()-[r:RowScope]-() ON (r.key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, test := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"match", "UNWIND $keys AS k MATCH (a:RowKey {key:'a'}) WITH a,k MATCH (a)-[r:RowScope]->() WHERE r.key=k RETURN k,r.key ORDER BY k", [][]interface{}{{"x", "x"}, {"y", "y"}}},
		{"count", "UNWIND $keys AS k MATCH (a:RowKey {key:'a'}) RETURN k,COUNT { MATCH (a)-[r:RowScope]->() WHERE r.key=k } AS n ORDER BY k", [][]interface{}{{"missing", int64(0)}, {"x", int64(1)}, {"y", int64(1)}}},
		{"call", "UNWIND $keys AS k MATCH (a:RowKey {key:'a'}) CALL (a,k) { MATCH (a)-[r:RowScope]->() WHERE r.key=k RETURN r.key AS value } RETURN k,value ORDER BY k", [][]interface{}{{"x", "x"}, {"y", "y"}}},
		{"nested_identity", "WITH $wanted AS k MATCH (a:RowKey {key:'a'}) RETURN k,COUNT { MATCH (a)-[r:RowScope]->() WHERE r.key=k },COUNT { MATCH (a)-[r:RowScope]->() }", [][]interface{}{{"x", int64(1), int64(2)}}},
	} {
		for _, mode := range []string{"", "PROFILE "} {
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), mode+test.query, map[string]interface{}{"keys": []interface{}{"x", "y", "missing"}, "wanted": "x"})
				require.NoError(t, err)
				require.Equal(t, test.want, result.Rows)
			})
		}
	}
	query := "MATCH (a:RowKey {key:'a'}) RETURN COUNT { MATCH (a)-[r:RowScope]->() WHERE r.key=$wanted }"
	for _, wanted := range []string{"x", "missing", "y", "x"} {
		result, err := exec.Execute(context.Background(), query, map[string]interface{}{"wanted": wanted})
		require.NoError(t, err)
		want := int64(1)
		if wanted == "missing" {
			want = 0
		}
		require.Equal(t, [][]interface{}{{want}}, result.Rows)
	}
}
