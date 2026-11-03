// Author: Codex, NOR-180 implementation lane, 2026-10-27.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCLIWALTerminalShellMessage(t *testing.T) {
	t.Setenv("NORNICDB_EMBEDDING_ENABLED", "false")
	t.Setenv("NORNICDB_TELEMETRY_ENABLED", "false")
	t.Setenv("NORNICDB_WAL_AUTO_COMPACTION_ENABLED", "false")
	closed := false
	const ordinaryDiagnostic = "NOR-180 ordinary query diagnostic control"
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "nor180.shellordinary", Signature: "nor180.shellordinary() :: (value :: INTEGER)", Mode: cypher.ProcedureModeRead, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		return nil, errors.New(ordinaryDiagnostic)
	}))
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "nor180.shellclose", Signature: "nor180.shellclose() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(_ context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		engine, err := exec.FunctionStorage("")
		if err != nil {
			return nil, err
		}
		for engine != nil {
			if provider, ok := engine.(interface{ GetWAL() *storage.WAL }); ok && provider.GetWAL() != nil {
				if err := provider.GetWAL().Close(); err != nil {
					return nil, err
				}
				closed = true
				return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
			}
			wrapper, ok := engine.(storage.EngineUnwrapper)
			if !ok {
				break
			}
			engine = wrapper.GetInnerEngine()
		}
		return nil, fmt.Errorf("test WAL provider unavailable")
	}))
	input, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = input.Close() })
	_, err = writer.WriteString("CALL nor180.shellordinary() YIELD value RETURN value\nCALL nor180.shellclose() YIELD value RETURN value\nexit\n")
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	original := os.Stdin
	os.Stdin = input
	t.Cleanup(func() { os.Stdin = original })
	var output bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.Flags().String("data-dir", t.TempDir(), "")
	require.NoError(t, runShell(cmd, nil))
	require.True(t, closed, "the real shell statement's WAL must be closed after implicit BEGIN")
	require.Contains(t, output.String(), storage.ErrWALClosed.Error())
	require.Contains(t, output.String(), "WAL tx abort")
	require.NotContains(t, output.String(), ordinaryDiagnostic, "ordinary shell query errors retain their existing generic response")
}
