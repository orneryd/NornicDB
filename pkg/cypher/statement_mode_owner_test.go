package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Mode is owned by the statement, including an executor reconstructed from its
// storage. The handler returns a constant and forwards no counters or metadata.
func TestStatementModeOwnerFreshContext(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, entrance := range []string{"supplied", "storage", "clone", "database_clone"} {
			for _, mode := range []ProcedureMode{ProcedureModeRead, ProcedureModeWrite} {
				t.Run(fmt.Sprintf("explicit=%v/%s/%s", explicit, entrance, mode), func(t *testing.T) {
					exec, _ := newProcedureAdmissionExecutor(t, false)
					name := "modeowner.fresh"
					require.NoError(t, RegisterUserProcedure(ProcedureSpec{
						Name: name, Signature: name + "(body :: STRING) :: (v :: INTEGER)", Mode: mode, ExecutesStatements: true,
						Params: []ProcedureParam{{Name: "body", Type: "STRING"}}, MinArgs: 1, MaxArgs: 1,
						Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}},
					}, func(_ context.Context, e *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
						switch entrance {
						case "storage":
							e = NewStorageExecutor(e.storage)
						case "clone":
							e = e.cloneWithStorage(e.storage)
						case "database_clone":
							e = e.cloneWithStorage(e.storage, selectedDatabaseStorage)
						}
						if _, err := e.Execute(context.Background(), args[0].(string), nil); err != nil {
							return nil, err
						}
						return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, nil
					}))
					t.Cleanup(ClearUserProcedures)
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					for _, writes := range []bool{true, false} {
						body, want := "RETURN 1", "r"
						if writes {
							body, want = "CREATE (:ModeOwner)", "w"
						}
						if mode == ProcedureModeWrite {
							want = "w"
						}
						result, err := exec.Execute(ctx, "CALL modeowner.fresh($body) YIELD v RETURN v", map[string]interface{}{"body": body})
						require.NoError(t, err)
						got, known := ResolvedStatementQueryType(result.Metadata)
						require.True(t, known)
						require.Equal(t, want, got, "fresh contexts retain the current statement's observation only")
					}
					if explicit {
						_, err := exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
