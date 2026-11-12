package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Neo4j 5.26.30 Enterprise accepts the earliest token after rotation while
// expiring an ordinary event token. A controlled procedure advances retention
// between the two public CDC calls without scheduling or sleep assumptions.
func TestCDCEarliestTracksRetainedPopulationAcrossPruning(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
		_, err := exec.Execute(t.Context(), "CREATE (:CdcRetained {id:1})", nil)
		require.NoError(t, err)
		first := cdcID(t, exec, "db.cdc.earliest")
		expired := cdcRows(t, exec, first)[0]["id"].(string)
		_, err = exec.Execute(t.Context(), "CREATE (:CdcRetained {id:2})", nil)
		require.NoError(t, err)
		capture, _ := storage.ChangeCaptureOf(store)
		const procedure = "fixture.pruneCdc"
		cleanupAdmissionProcedure(t, procedure)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: procedure, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1, Returns: []ProcedureColumn{{Name: "start", Type: "STRING"}}}, func(ctx context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			_, err := capture.(interface {
				PruneChangeEvents(context.Context, string, time.Time) (int, error)
			}).PruneChangeEvents(ctx, "test", time.Now().Add(time.Hour))
			return &ExecuteResult{Columns: []string{"start"}, Rows: [][]interface{}{{args[0]}}}, err
		}))
		result, err := exec.Execute(t.Context(), "CALL db.cdc.earliest() YIELD id CALL fixture.pruneCdc(id) YIELD start CALL db.cdc.query(start,[]) YIELD event RETURN event.state.after.properties.id", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		require.Equal(t, first, cdcID(t, exec, "db.cdc.earliest"))
		require.Len(t, cdcRows(t, exec, first), 1, "the same earliest token reads the retained feed, not an old snapshot")
		_, err = exec.Execute(t.Context(), "CALL db.cdc.query($id)", map[string]interface{}{"id": expired})
		requireStatusCode(t, err, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier")

		// A second write and prune show the marker resolves against the live
		// retained feed each time: the unchanged token now reads only C.
		_, err = exec.Execute(t.Context(), "CREATE (:CdcRetained {id:3})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "CALL fixture.pruneCdc('post')", nil)
		require.NoError(t, err)
		require.Equal(t, first, cdcID(t, exec, "db.cdc.earliest"))
		result, err = exec.Execute(t.Context(), "CALL db.cdc.query($id) YIELD event RETURN event.state.after.properties.id", map[string]interface{}{"id": first})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	})
}
