package cypher

// Batch 2c items 5 and 6 (research only, untracked; same text on both builds):
// entry 27 through the database API with non-fatal checks, so every statement
// runs and is logged.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzei2cRows(result *ExecuteResult) any {
	if result == nil {
		return nil
	}
	return result.Rows
}

// Item 5: entry 27.A with and without the index, with and without BEGIN; all
// four statements always run.
func TestZZEI2cEndSeedRepeatedAll(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed=%v/tx=%v", indexed, explicit), func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cendseed"))
				ctx := context.Background()
				_, err := e.Execute(ctx, `CREATE (a:P {id:1})-[:SELF]->(b:P {id:2}), (c:P {id:3})-[:SELF]->(c)`, nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, `UNWIND range(4, 250) AS id CREATE (:P {id:id})`, nil)
				require.NoError(t, err)
				if indexed {
					_, err = e.Execute(ctx, `CREATE INDEX p_id FOR (p:P) ON (p.id)`, nil)
					require.NoError(t, err)
				}
				if explicit {
					_, err = e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer e.Execute(ctx, "ROLLBACK", nil)
				}
				for _, q := range []string{
					`MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id LIMIT 1`,
					`MATCH (p:P)-[:SELF]->(p) RETURN p.id ORDER BY p.id LIMIT 1`,
					`MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id`,
					`MATCH (p:P)-[:SELF]->(p) RETURN p.id ORDER BY p.id`,
				} {
					result, err := e.Execute(ctx, q, nil)
					t.Logf("ZZEI2c A indexed=%v tx=%v %q -> rows=%v err=%v (expected [[3]])", indexed, explicit, q, zzei2cRows(result), err)
				}
			})
		}
	}
}

// Item 6: entry 27.B, control first, then the reported form and two narrowing
// variants of the inner CALL.
func TestZZEI2cNestedLocalUnionAll(t *testing.T) {
	for _, q := range []string{
		"MATCH (n:ZZNested {v:1}) CALL { MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
		"MATCH (n:ZZNested {v:1}) CALL { CALL () { MATCH (n:ZZNested) RETURN count(n) AS k } MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
		"MATCH (n:ZZNested {v:1}) CALL { CALL () { MATCH (x:ZZNested) RETURN count(x) AS k } MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
		"MATCH (n:ZZNested {v:1}) CALL { CALL () { RETURN 1 AS k } MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
	} {
		t.Run(q, func(t *testing.T) {
			ctx := context.Background()
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cnested"))
			_, err := e.Execute(ctx, "CREATE (:ZZNested {v:1}), (:ZZNested {v:2})", nil)
			require.NoError(t, err)
			result, err := e.Execute(ctx, q, nil)
			t.Logf("ZZEI2c B %q -> rows=%v err=%v (expected [[1],[2]])", q, zzei2cRows(result), err)
		})
	}
}
