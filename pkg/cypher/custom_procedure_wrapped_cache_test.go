package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCustomProcedureWrappedCacheExclusion(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	for _, route := range []struct {
		name, query string
		fabric      bool
	}{
		{"direct", "CALL custom.CacheTick() YIELD v RETURN v", false},
		{"literal", "CALL apoc.cypher.run('CALL custom.CacheTick() YIELD v RETURN v', {}) YIELD value RETURN value.v AS v", false},
		{"parameter", "CALL apoc.cypher.run($statement, {}) YIELD value RETURN value.v AS v", false},
		{"bound expression", "WITH $statement AS body CALL apoc.cypher.run(body, {}) YIELD value RETURN value.v AS v", false},
		{"nested wrapper", "CALL apoc.cypher.run('CALL apoc.cypher.run($statement, {}) YIELD value RETURN value.v AS v', {statement: $statement}) YIELD value RETURN value.v AS v", false},
		{"Fabric parameter", "CALL { USE pcomp.other CALL apoc.cypher.run($statement, {}) YIELD value RETURN value.v AS v } RETURN v", true},
	} {
		t.Run(route.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cached"), QueryCachePolicy{MaxEntries: 100, TTL: time.Minute})
			if route.fabric {
				executor = newCompositeFixture(t)
			}
			var calls int64
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{
				Name: "custom.CacheTick", Mode: ProcedureModeRead, MaxArgs: 0,
				Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}},
			}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				calls++
				return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{calls}}}, nil
			}))
			params := map[string]interface{}{"statement": "CALL custom.CacheTick() YIELD v RETURN v"}
			for attempt := int64(1); attempt <= 3; attempt++ {
				result, err := executor.Execute(context.Background(), route.query, params)
				require.NoError(t, err)
				require.Equal(t, attempt, calls)
				require.Equal(t, [][]interface{}{{attempt}}, result.Rows)
			}
		})
	}
}
