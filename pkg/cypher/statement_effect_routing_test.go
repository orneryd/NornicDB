package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRoutingConsumersFollowRegisteredWriteEffect(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	const query = "MATCH (n:Doc) CALL kp.suppress(n) RETURN n"
	contract, err := readStatementExecutionContract(t.Context(), query)
	require.NoError(t, err)
	require.True(t, contract.writes)
	t.Run("Fabric fragment", func(t *testing.T) {
		fragment, err := fabric.NewFabricPlanner(nil, fabricStatementEffect).Plan(t.Context(), query, "nornic", RawSource)
		require.NoError(t, err)
		execution, ok := fragment.(*fabric.FragmentExec)
		require.True(t, ok)
		require.True(t, execution.IsWrite)
	})
	t.Run("storage routing analysis", func(t *testing.T) {
		info := storage.NewQueryAnalyzer().AnalyzeQuery(query, contract.effect() != statementEffectRead)
		require.True(t, info.IsWrite)
	})
}

func TestCatalogMutationAnalysisReportsWrite(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{"DROP DATABASE effect_fixture IF EXISTS", "DROP ALIAS effect_fixture IF EXISTS FOR DATABASE"} {
			t.Run(query, func(t *testing.T) {
				info, err := NewQueryAnalyzer(4).Analyze(t.Context(), query)
				require.NoError(t, err)
				require.True(t, info.Writes())
				require.False(t, info.ReadOnly())
			})
		}
	})
}

func TestCatalogWriteEffectPreservesTransactionOwnership(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tc := range []struct {
		query  string
		writes bool
	}{
		{"DROP DATABASE effect_fixture IF EXISTS", true},
		{"DROP ALIAS effect_fixture IF EXISTS FOR DATABASE", true},
		{"SHOW DATABASES", false},
		{"EXPLAIN DROP DATABASE effect_fixture IF EXISTS", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			contract, err := readStatementExecutionContract(t.Context(), tc.query)
			require.NoError(t, err)
			require.False(t, contract.transaction, "catalog owner must retain its transaction lifetime")
			require.False(t, contract.writes, "catalog mutation is not a graph-data WRITE privilege")
			require.Equal(t, tc.writes, contract.effect() != statementEffectRead)
			original := &fabric.FragmentExec{Query: tc.query, GraphName: "nornic"}
			resolved, err := exec.fabricProcedureWritePlan(t.Context(), original)
			require.NoError(t, err)
			require.Equal(t, tc.writes, resolved.(*fabric.FragmentExec).IsWrite)
			require.False(t, original.IsWrite, "cached structural fragment stays immutable")
		})
	}
}
