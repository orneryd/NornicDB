package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type issuedStorageEnvelope struct{ storage.Engine }

func (view issuedStorageEnvelope) GetInnerEngine() storage.Engine { return view.Engine }

// A plugin can rebuild an executor over its issued storage. That storage
// still belongs to the enclosing statement; nested transaction control cannot
// independently publish a write before the enclosing statement fails.
func TestFabricIssuedStorageCannotCommitNestedScript(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	fixture := newFabricAtomicityFixture(t)
	physical, err := fixture.mgr.GetStorage("one")
	require.NoError(t, err)
	_, err = NewStorageExecutor(physical).Execute(context.Background(), `BEGIN CREATE (:AcAtom {run: $run}) COMMIT`, map[string]interface{}{"run": "unowned-control"})
	require.NoError(t, err, "the same parameterized script must succeed on unowned native storage")
	require.Equal(t, int64(1), fixture.writes(t, "comp.one", "unowned-control"))
	for _, wrapped := range []bool{false, true} {
		for _, command := range []string{"BEGIN", "BEGIN TRANSACTION", "COMMIT", "ROLLBACK", `BEGIN CREATE (:AcAtom {run: $run}) COMMIT`, `BEGIN TRANSACTION CREATE (:AcAtom {run: $run}) COMMIT`} {
			name := command
			if wrapped {
				name = "wrapped/" + name
			}
			t.Run(name, func(t *testing.T) {
				reached := 0
				const procedure = "acatom.rebuilt"
				require.NoError(t, RegisterUserProcedure(ProcedureSpec{
					Name: procedure, Signature: procedure + "(run :: STRING) :: (w :: INTEGER)", Mode: ProcedureModeWrite,
					Params: []ProcedureParam{{Name: "run", Type: "STRING"}}, MinArgs: 1, MaxArgs: 1,
					Returns: []ProcedureColumn{{Name: "w", Type: "INTEGER"}},
				}, func(_ context.Context, executor *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
					reached++
					issued := executor.storage
					if wrapped {
						issued = issuedStorageEnvelope{issued}
					}
					rebuilt := NewStorageExecutor(issued)
					rebuilt.SetDatabaseManager(executor.dbManager)
					_, nestedErr := rebuilt.Execute(context.Background(), command, map[string]interface{}{"run": args[0]})
					if nestedErr != nil {
						return nil, nestedErr
					}
					return &ExecuteResult{Columns: []string{"w"}, Rows: [][]interface{}{{int64(1)}}}, nil
				}))
				run := t.Name()
				_, executionErr := fixture.exec.Execute(context.Background(), `USE comp CALL { USE comp.one CALL acatom.rebuilt($run) YIELD w RETURN w } CALL { USE comp.two RETURN 1 / $d AS z } RETURN w,z`, map[string]interface{}{"run": run, "d": int64(0)})
				require.ErrorContains(t, executionErr, "Neo.ClientError.Transaction.TransactionTypeNotSupported")
				require.Equal(t, 1, reached, "must exercise the public procedure callback")
				require.Zero(t, fixture.writes(t, "comp.one", run), "issued transaction storage cannot escape outer rollback")
			})
		}
	}
}
