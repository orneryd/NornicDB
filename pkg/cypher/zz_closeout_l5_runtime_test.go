package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/trigger"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL5IndexSurfaces(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "closeout_l5_index"))
	run := func(t *testing.T, statement string) [][]any {
		t.Helper()
		result, err := exec.Execute(t.Context(), statement, nil)
		require.NoError(t, err)
		return result.Rows
	}
	run(t, "CREATE CONSTRAINT closeout_id FOR (n:Base) REQUIRE n.id IS UNIQUE")
	run(t, "CREATE INDEX closeout_alias FOR (n:Special) ON (n.aliases)")
	run(t, "CREATE (:Base:Special {id: 7, aliases: ['a', 'b']}), (:Base:Special {id: 8, aliases: ['c']})")
	for _, tc := range []struct {
		id        string
		statement string
	}{
		{"control", "MATCH (n:Base) WHERE n.id = 7 RETURN n.id"},
		{"H-178", "MATCH (n:Special) WHERE 'b' IN n.aliases RETURN n.id"},
		{"H-179", "MATCH (n:Special) WHERE n.id = 7 RETURN n.id"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			require.Equal(t, [][]any{{int64(7)}}, run(t, tc.statement))
			op, err := exec.analyzeNodeScan(t.Context(), tc.statement)
			require.NoError(t, err)
			if tc.id == "control" {
				require.Contains(t, op.OperatorType, "IndexSeek")
			}
			t.Logf("L5 %s operator=%s indexStatus=%v", tc.id, op.OperatorType, op.Arguments["indexStatus"])
		})
	}
	t.Run("H-177/scan-control", func(t *testing.T) {
		require.Equal(t, [][]any{{int64(7)}}, run(t, "MATCH (n:Special) WHERE ANY(a IN n.aliases WHERE 'banana' CONTAINS a) RETURN n.id"))
	})
}

func TestCloseoutL5Trigger(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "closeout_l5_trigger"))
	run := func(stmt string) [][]any {
		t.Helper()
		result, err := exec.Execute(t.Context(), stmt, nil)
		require.NoError(t, err, stmt)
		return result.Rows
	}
	const triggerName = "closeout_before"
	const action = "MATCH (n:CloseoutTriggered) SET n.fired = true"
	require.NoError(t, trigger.Before(triggerName, action))
	t.Cleanup(func() { _ = trigger.Remove(triggerName) })
	registered, err := trigger.Show(triggerName)
	require.NoError(t, err)
	require.Equal(t, "before", registered.Phase)
	require.Equal(t, action, registered.Statement)
	run("CREATE (:CloseoutTriggered)")
	require.Equal(t, [][]any{{false}}, run("MATCH (n:CloseoutTriggered) RETURN coalesce(n.fired, false)"))
	run(action)
	require.Equal(t, [][]any{{true}}, run("MATCH (n:CloseoutTriggered) RETURN n.fired"))
	t.Log("L5 H-224 before-phase registration is stored, autocommit does not dispatch its action, direct action works")
}
