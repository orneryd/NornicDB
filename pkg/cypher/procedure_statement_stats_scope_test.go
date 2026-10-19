package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	totals.stats = &QueryStats{NodesCreated: 1, NodesDeleted: 2, RelationshipsCreated: 3, RelationshipsDeleted: 4, PropertiesSet: 5, LabelsAdded: 6, LabelsRemoved: 7, IndexesAdded: 8, IndexesRemoved: 9, ConstraintsAdded: 10, ConstraintsRemoved: 11}
	inner.commit(totals.stats)
	inner.commit(totals.stats)
	require.Nil(t, parent.stats)
	want := *totals.stats
	outer.commit(outer.summary())
	outer.commit(outer.summary())
	require.Equal(t, &want, parent.stats, "a committed child contributes exactly once")
	_, discarded := beginProcedureStatementStatsScope(parent)
	discarded.stats = &QueryStats{NodesCreated: 100}
	discarded.discard()
	discarded.commit(discarded.summary())
	require.Equal(t, &want, parent.stats)
	plain := context.Background()
	unchanged, absent := beginProcedureStatementStatsScope(plain)
	require.Equal(t, plain, unchanged)
	absent.commit(nil)
	absent.discard()
}

func TestProcedureStatementStatsNestedIndependentUnits(t *testing.T) {
	for _, commitOuter := range []bool{false, true} {
		t.Run(map[bool]string{false: "outerRollback", true: "outerCommit"}[commitOuter], func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested_units")
			failure := errors.New("outer batch failed after the inner commit")
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "nestedstats.ownedunits", Mode: ProcedureModeWrite, MinArgs: 0, MaxArgs: 0}, func(ctx context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				outerOwner := NewStorageExecutor(store)
				_, err := outerOwner.executeWithImplicitTransactionCallback(ctx, "CREATE (:PendingStats)", "CREATE (:PendingStats)", func(outerCtx context.Context, outer *StorageExecutor) (*ExecuteResult, error) {
					innerOwner := NewStorageExecutor(store)
					inner, err := innerOwner.executeWithImplicitTransactionCallback(outerCtx, "CREATE (:DurableStats)", "CREATE (:DurableStats)", func(innerCtx context.Context, inner *StorageExecutor) (*ExecuteResult, error) {
						return inner.executeUserStatement(innerCtx, "CREATE (:DurableStats {p:1})", nil)
					})
					require.NoError(t, err)
					pending, err := outer.executeUserStatement(outerCtx, "CREATE (:PendingStats {p:1})", nil)
					require.NoError(t, err)
					if !commitOuter {
						return nil, failure
					}
					return &ExecuteResult{Stats: mergeQueryStats(inner.Stats, pending.Stats)}, nil
				})
				if commitOuter {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, failure)
				}
				return &ExecuteResult{Stats: &QueryStats{}}, nil
			}))
			exec := NewStorageExecutor(store)
			result, err := exec.Execute(context.Background(), "CALL nestedstats.ownedunits()", nil)
			require.NoError(t, err)
			want := 1
			if commitOuter {
				want++
			}
			require.Equal(t, &QueryStats{NodesCreated: want, PropertiesSet: want, LabelsAdded: want}, result.Stats)
			for label, count := range map[string]int{"DurableStats": 1, "PendingStats": want - 1} {
				physical, err := exec.Execute(context.Background(), "MATCH (n:"+label+") RETURN count(n)", nil)
				require.NoError(t, err)
				require.EqualValues(t, count, physical.Rows[0][0])
			}
		})
	}
}

func TestProcedureStatementStatsReadIsolation(t *testing.T) {
	first, second := &procedureStatementStats{}, &procedureStatementStats{}
	first.record(nil)
	second.record(nil)
	first.record(&QueryStats{NodesCreated: 1})
	first.record(nil)
	require.Equal(t, &QueryStats{NodesCreated: 1}, first.summary())
	require.Nil(t, second.summary())
	require.Equal(t, QueryStats{}, procedureStatementReadStats)
}
