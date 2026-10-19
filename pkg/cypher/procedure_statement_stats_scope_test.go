package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
 "github.com/stretchr/testify/assert"
)

func TestProcedureStatementStatsTransactionScope(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "stats_scope")
			exec := NewStorageExecutor(store)
			parent := &procedureStatementStats{Context: context.Background()}
			failure := errors.New("later row failed")
			_, err := exec.executeWithImplicitTransactionCallback(parent, "CREATE (:ScopedStats)", "CREATE (:ScopedStats)", func(ctx context.Context, tx *StorageExecutor) (*ExecuteResult, error) {
				result, err := tx.executeUserStatement(ctx, "CALL apoc.cypher.run('CREATE (:ScopedStats {p:1}) RETURN 1 AS v',{}) YIELD value RETURN value", nil)
				require.NoError(t, err)
				assert.Nil(t, parent.stats, "uncommitted rows must stay in the child scope")
				if !commit {
					return result, failure
				}
				return result, nil
			})
			physical, readErr := exec.Execute(context.Background(), "MATCH (n:ScopedStats) RETURN count(n)", nil)
			require.NoError(t, readErr)
			if commit {
				require.NoError(t, err)
				require.EqualValues(t, 1, physical.Rows[0][0])
				require.Equal(t, &QueryStats{NodesCreated: 1, PropertiesSet: 1, LabelsAdded: 1}, parent.stats)
			} else {
				require.ErrorIs(t, err, failure)
				require.EqualValues(t, 0, physical.Rows[0][0])
				require.Nil(t, parent.stats)
			}
		})
	}
}

func TestProcedureStatementStatsScopeNesting(t *testing.T) {
	parent := &procedureStatementStats{Context: context.Background()}
	ctx, outer := beginProcedureStatementStatsScope(parent)
	childCtx, inner := beginProcedureStatementStatsScope(ctx)
	totals := childCtx.Value(procedureStatementStatsKey{}).(*procedureStatementStats)
	totals.executed = true
	totals.stats = &QueryStats{NodesCreated: 1, NodesDeleted: 2, RelationshipsCreated: 3, RelationshipsDeleted: 4, PropertiesSet: 5, LabelsAdded: 6, LabelsRemoved: 7, IndexesAdded: 8, IndexesRemoved: 9, ConstraintsAdded: 10, ConstraintsRemoved: 11}
	inner.commit()
	inner.commit()
	require.Nil(t, parent.stats)
	want := *totals.stats
	outer.commit()
	outer.commit()
	require.Equal(t, &want, parent.stats, "a committed child contributes exactly once")
	_, discarded := beginProcedureStatementStatsScope(parent)
	discarded.executed = true
	discarded.stats = &QueryStats{NodesCreated: 100}
	discarded.discard()
	discarded.commit()
	require.Equal(t, &want, parent.stats)
	plain := context.Background()
	unchanged, absent := beginProcedureStatementStatsScope(plain)
	require.Equal(t, plain, unchanged)
	absent.commit()
	absent.discard()
}
