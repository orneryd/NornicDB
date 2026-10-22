package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Lowering inline properties must retain the admitted source without running
// a producer again when execution resolves its equality seek.
func TestRelationshipAccessProgramPreparedSourceProducer(t *testing.T) {
	var calls atomic.Int64
	withPluginFunctions(t, map[string]interface{}{
		"inspection.admissionkey": func() int64 { return calls.Add(1) },
	})
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prepared_source"))
	exec.cache = nil
	for _, query := range []string{
		"CREATE ()-[:PreparedKey {key:1}]->(), ()-[:PreparedKey {key:2}]->()",
		"CREATE RANGE INDEX prepared_source_key FOR ()-[r:PreparedKey]-() ON (r.key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct{ name, query string }{
		{"pipeline", "UNWIND [1,2] AS step MATCH ()-[r:PreparedKey {key:inspection.admissionkey()}]->() RETURN r.key AS key ORDER BY key"},
		{"nested_projection", "UNWIND [1,2] AS step RETURN head([()-[r:PreparedKey {key:inspection.admissionkey()}]->() | r.key]) AS key ORDER BY key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := tc.query
			calls.Store(0)
			plan, err := exec.Execute(context.Background(), "EXPLAIN "+query, nil)
			require.NoError(t, err)
			require.Zero(t, calls.Load(), "source discovery must not evaluate a property")
			require.Contains(t, planStringFromResult(t, plan), "RelationshipIndexSeek")
			for _, mode := range []string{"", "PROFILE "} {
				t.Run(map[string]string{"": "execute", "PROFILE ": "profile"}[mode], func(t *testing.T) {
					calls.Store(0)
					ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
					result, err := exec.Execute(ctx, mode+query, nil)
					require.NoError(t, err, mode+query)
					require.Equal(t, int64(2), calls.Load(), "one property evaluation per reached row: %s", mode+query)
					require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows, mode+query)
					if mode != "" {
						// PROFILE owns an inner collector. Its returned operator is
						// the public observation of those actual physical accesses.
						plan := result.Metadata["plan"].(*ExecutionPlan)
						seek := relationshipPlanOperator(plan.Root, "RelationshipIndexSeek")
						require.NotNil(t, seek, planStringFromResult(t, result))
						require.Equal(t, "prepared_source_key", seek.Arguments["index"])
						require.Equal(t, int64(2), seek.Arguments["indexLookups"])
						require.Equal(t, int64(2), seek.ActualRows)
						return
					}
					profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
					require.Empty(t, profile.unregistered)
					seek := profile.seeks["r\x00prepared_source_key"]
					require.NotNil(t, seek, "correct rows must still use the original admitted seek")
					require.Equal(t, int64(2), seek.lookups)
				})
			}
		})
	}
}
