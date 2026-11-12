package cypher

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestProfileCustomProcedureCatalogListsItsActualPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(ClearUserProcedures)
		exec, _ := newProfileRuntimeTestExecutor(t)
		store := exec.storage
		for _, name := range []string{"list", "show"} {
			_, plan := profileEquivalentObservedStatement(t, exec, "CALL apoc.custom."+name+"() YIELD name RETURN name")
			read := planOperatorOfType(plan.Root, profileKindNodeByLabelScan)
			require.NotNil(t, read)
			require.Zero(t, read.ActualRows)
			require.Zero(t, read.DBHits)
		}
		for _, query := range []string{
			`CALL apoc.custom.installProcedure('Report() :: (n :: INTEGER)', 'RETURN 7 AS n')`,
			`CREATE PROCEDURE plain.invisible() MODE READ AS RETURN 8 AS n`,
		} {
			_, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
		}
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: "bad-catalog", Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"record": "invalid base64 ***"}})
		require.NoError(t, err)
		for _, name := range []string{"list", "show"} {
			rows, plan := profileEquivalentObservedStatement(t, exec, "CALL apoc.custom."+name+"() YIELD name RETURN name")
			require.Equal(t, [][]interface{}{{"Report"}}, rows.Rows)
			read := planOperatorOfType(plan.Root, profileKindNodeByLabelScan)
			require.NotNil(t, read)
			require.EqualValues(t, 3, read.ActualRows, "valid, noncustom and invalid records were read before filtering")
			require.EqualValues(t, 3, read.DBHits)
			require.Equal(t, procedureCatalogLabel, read.Description)
		}
	})
}

func TestProfileCustomProcedureCatalogMutationsObserveIdentityReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, operation := range []string{"installProcedure", "declareProcedure", "dropProcedure"} {
			t.Run(operation, func(t *testing.T) {
				t.Cleanup(ClearUserProcedures)
				var plain *ExecuteResult
				for _, profiled := range []bool{false, true} {
					ClearUserProcedures()
					exec, _ := newProfileRuntimeTestExecutor(t)
					store := exec.storage
					if operation != "installProcedure" {
						_, err := exec.Execute(t.Context(), `CALL apoc.custom.installProcedure('Report() :: (n :: INTEGER)', 'RETURN 1 AS n')`, nil)
						require.NoError(t, err)
					}
					query := "CALL apoc.custom." + operation + "('Report() :: (n :: INTEGER)', 'RETURN 7 AS n')"
					if operation == "dropProcedure" {
						query = "CALL apoc.custom.dropProcedure('Report')"
					}
					if profiled {
						query = "PROFILE " + query
					}
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					if !profiled {
						plain = result
					} else {
						require.Equal(t, plain.Columns, result.Columns)
						require.Equal(t, plain.Rows, result.Rows)
						plan := executionPlanFromResult(t, result)
						requireWriteAccountingObserved(t, plan, query)
						read := planOperatorOfType(plan.Root, profileKindNodeByIdSeek)
						require.NotNil(t, read)
						require.EqualValues(t, 3, read.DBHits, "exact key, differently cased legacy key, then authoritative exact key")
						want := 2
						if operation == "installProcedure" {
							want = 0
						}
						require.EqualValues(t, want, read.ActualRows)
						kind := profileKindNodeCreateRequest
						if operation == "declareProcedure" {
							kind = profileKindNodeUpdateRequest
						}
						if operation == "dropProcedure" {
							kind = profileKindNodeDeleteRequest
						}
						request := requireRequestRecord(t, plan, kind, query)
						require.EqualValues(t, 1, request.ActualRows)
						require.EqualValues(t, 1, request.DBHits)
						require.EqualValues(t, 4, plan.TotalDBHits, "three point requests and one actual native mutation")
					}
					_, err = store.GetNode(t.Context(), procedureCatalogNodeID("custom.Report"))
					if operation == "dropProcedure" {
						require.ErrorIs(t, err, storage.ErrNotFound)
					} else {
						require.NoError(t, err)
						called, err := exec.Execute(t.Context(), "CALL custom.Report() YIELD n RETURN n", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(7)}}, called.Rows)
					}
				}
			})
		}
	})
}

func TestProfileCustomProcedureCatalogMigratesLegacyIdentityWithActualRequests(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(ClearUserProcedures)
		exec, _ := newProfileRuntimeTestExecutor(t)
		_, err := exec.Execute(t.Context(), "RETURN 1 AS initialized", nil)
		require.NoError(t, err)
		record := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 7 AS n", Signature: "custom.Report() :: (n :: INTEGER)"}
		blob, err := msgpack.Marshal(record)
		require.NoError(t, err)
		legacy := procedureCatalogNodeID("custom.report")
		_, err = exec.storage.CreateNode(t.Context(), &storage.Node{ID: legacy, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"record": base64.StdEncoding.EncodeToString(blob)}})
		require.NoError(t, err)
		result, err := exec.Execute(t.Context(), `PROFILE CALL apoc.custom.installProcedure('Report() :: (n :: INTEGER)', 'RETURN 8 AS n')`, nil)
		require.NoError(t, err)
		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "legacy catalog replacement")
		read := planOperatorOfType(plan.Root, profileKindNodeByIdSeek)
		require.NotNil(t, read)
		require.EqualValues(t, 4, read.DBHits)
		require.EqualValues(t, 2, read.ActualRows)
		for _, kind := range []string{profileKindNodeCreateRequest, profileKindNodeDeleteRequest, profileKindNodeUpdateRequest} {
			request := requireRequestRecord(t, plan, kind, "legacy catalog replacement")
			require.EqualValues(t, 1, request.DBHits)
			require.EqualValues(t, 1, request.ActualRows)
		}
		require.EqualValues(t, 7, plan.TotalDBHits)
		_, err = exec.storage.GetNode(t.Context(), legacy)
		require.ErrorIs(t, err, storage.ErrNotFound)
		called, err := exec.Execute(t.Context(), "CALL custom.Report() YIELD n RETURN n", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(8)}}, called.Rows)
	})
}

func TestProfileCustomProcedureCatalogInstallationDoesNotExecuteBody(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(ClearUserProcedures)
		exec, _ := newProfileRuntimeTestExecutor(t)
		calls := 0
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "profile.catalog_body", Signature: "profile.catalog_body() :: (n :: INTEGER)", Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "n", Type: "INTEGER"}}}, func(_ context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
			calls++
			return &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{int64(7)}}}, nil
		}))
		result, err := exec.Execute(t.Context(), `PROFILE CALL apoc.custom.installProcedure('notRun() :: (n :: INTEGER)', 'CALL profile.catalog_body() YIELD n RETURN n')`, nil)
		require.NoError(t, err)
		require.Zero(t, calls)
		requireWriteAccountingObserved(t, executionPlanFromResult(t, result), "installation compiles but does not execute stored text")
		called, err := exec.Execute(t.Context(), "PROFILE CALL custom.notRun() YIELD n RETURN n", nil)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, [][]interface{}{{int64(7)}}, called.Rows)
		require.False(t, executionPlanFromResult(t, called).TotalDBHitsObserved, "the subsequently invoked custom body remains opaque")
	})
}

func TestProfileCustomProcedureDropObservesNonemptyLegacyReload(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(ClearUserProcedures)
		exec, _ := newProfileRuntimeTestExecutor(t)
		for _, name := range []string{"Report", "Survivor"} {
			_, err := exec.Execute(t.Context(), "CALL apoc.custom.installProcedure('"+name+"() :: (n :: INTEGER)', 'RETURN 7 AS n')", nil)
			require.NoError(t, err)
		}
		record := persistedProcedureRecord{Name: "custom.Legacy", Mode: "READ", Body: "RETURN 8 AS n", Signature: "custom.Legacy() :: (n :: INTEGER)"}
		blob, err := msgpack.Marshal(record)
		require.NoError(t, err)
		legacy := procedureCatalogNodeID("custom.legacy")
		_, err = exec.storage.CreateNode(t.Context(), &storage.Node{ID: legacy, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"record": base64.StdEncoding.EncodeToString(blob)}})
		require.NoError(t, err)
		result, err := exec.Execute(t.Context(), "PROFILE CALL apoc.custom.dropProcedure('Report')", nil)
		require.NoError(t, err)
		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "drop and surviving catalog reload")
		point := planOperatorOfType(plan.Root, profileKindNodeByIdSeek)
		require.NotNil(t, point)
		require.EqualValues(t, 4, point.DBHits)
		require.EqualValues(t, 2, point.ActualRows)
		scan := planOperatorOfType(plan.Root, profileKindNodeByLabelScan)
		require.NotNil(t, scan)
		require.EqualValues(t, 2, scan.DBHits)
		require.EqualValues(t, 2, scan.ActualRows)
		require.EqualValues(t, 1, requireRequestRecord(t, plan, profileKindNodeCreateRequest, "legacy reload").DBHits)
		require.EqualValues(t, 2, requireRequestRecord(t, plan, profileKindNodeDeleteRequest, "drop and migration").DBHits)
		require.EqualValues(t, 9, plan.TotalDBHits)
		_, err = exec.storage.GetNode(t.Context(), legacy)
		require.ErrorIs(t, err, storage.ErrNotFound)
		_, err = exec.storage.GetNode(t.Context(), procedureCatalogNodeID("custom.Legacy"))
		require.NoError(t, err)
		for _, check := range []struct {
			name  string
			value int64
		}{{"Survivor", 7}, {"Legacy", 8}} {
			called, err := exec.Execute(t.Context(), "CALL custom."+check.name+"() YIELD n RETURN n", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{check.value}}, called.Rows)
		}
	})
}

func TestProfileCustomProcedurePipelinePreservesRowsAndMutationAccounting(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Cleanup(ClearUserProcedures)
		exec, _ := newProfileRuntimeTestExecutor(t)
		result, err := exec.Execute(t.Context(), "PROFILE UNWIND [1] AS ignored CALL apoc.custom.installProcedure('Pipeline() :: (n :: INTEGER)', 'RETURN 7 AS n') RETURN ignored AS x", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "pipeline catalog installation")
		require.EqualValues(t, 3, planOperatorOfType(plan.Root, profileKindNodeByIdSeek).DBHits)
		require.EqualValues(t, 1, requireRequestRecord(t, plan, profileKindNodeCreateRequest, "pipeline catalog installation").DBHits)
		require.EqualValues(t, 4, plan.TotalDBHits)
		called, err := exec.Execute(t.Context(), "CALL custom.Pipeline() YIELD n RETURN n", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7)}}, called.Rows)
	})
}
