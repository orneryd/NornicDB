package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The public query must select the relationship index, including a key bound
// by UNWIND. Correct rows alone did not catch the repeated type/graph scans.
func TestRelationshipPropertySeek(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE (:Probe)-[:KEYED {_ci_key:'a'}]->(:Probe)", nil)
	run("CREATE RANGE INDEX rel_key FOR ()-[r:KEYED]-() ON (r._ci_key)", nil)
	for _, query := range []string{
		"MATCH ()-[r:KEYED {_ci_key:'a'}]->() RETURN r._ci_key AS key",
		"MATCH ()-[r:KEYED]->() WHERE r._ci_key = 'a' RETURN r._ci_key AS key",
		"UNWIND $keys AS k MATCH ()-[r:KEYED {_ci_key:k}]->() RETURN r._ci_key AS key",
		"UNWIND $keys AS k MATCH ()-[r:KEYED]->() WHERE r._ci_key = k RETURN r._ci_key AS key",
	} {
		params := map[string]interface{}{"keys": []interface{}{"a", "missing", "a"}}
		profile := run("PROFILE "+query, params)
		require.NotEmpty(t, profile.Rows)
		require.Contains(t, planStringFromResult(t, profile), "RelationshipIndexSeek", query)
		explain := run("EXPLAIN "+query, params)
		require.Empty(t, explain.Rows)
		require.Contains(t, planStringFromResult(t, explain), "RelationshipIndexSeek", query)
	}
}
