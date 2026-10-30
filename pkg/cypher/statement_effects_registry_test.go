package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatementEffectsResolveLiveProcedureModes(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	const query = "CALL mode_fixture.live()"
	syntax, err := cypherantlr.ParseSyntax(t.Context(), query, cypherantlr.FragmentEntry, false, cypherantlr.RawSource)
	require.NoError(t, err)
	require.Equal(t, "mode_fixture.live", syntax.Effects().Call(0).Name())
	handler := func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		t.Fatal("effect inspection must not execute a procedure")
		return nil, nil
	}
	for _, mode := range []ProcedureMode{ProcedureModeRead, ProcedureModeWrite, ProcedureModeRead} {
		require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "mode_fixture.live", Mode: mode}, handler))
		contract, err := readStatementExecutionContract(t.Context(), query)
		require.NoError(t, err)
		require.True(t, contract.calls)
		require.False(t, contract.unknownProcedure)
		require.Equal(t, mode == ProcedureModeWrite, contract.writes, "cached syntax cannot cache live registry policy")
	}
	require.False(t, syntax.Effects().Writes(), "the immutable grammar product contains no registry decision")
	require.False(t, syntax.Effects().RemoteReadOnly(), "remote registry is opaque even when local mode is READ")
}
