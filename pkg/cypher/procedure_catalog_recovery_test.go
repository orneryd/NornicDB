package cypher

import (
	"context"
	"encoding/base64"
	"errors"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestProcedureCatalogMutationRecoversLegacyIdentity(t *testing.T) {
	for _, arrival := range []string{"failed startup migration", "visible after construction"} {
		for _, operation := range []string{"install lowercase", "drop lowercase", "drop exact"} {
			t.Run(arrival+"/"+operation, func(t *testing.T) {
				ClearUserProcedures()
				t.Cleanup(ClearUserProcedures)
				base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				fault := &procedureIdentityFaultEngine{Engine: base}
				seed := func() {
					record := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"}
					blob, err := msgpack.Marshal(record)
					require.NoError(t, err)
					_, err = base.CreateNode(&storage.Node{ID: procedureCatalogNodeID("custom.report"), Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(blob)}})
					require.NoError(t, err)
				}
				if arrival == "failed startup migration" {
					seed()
					fault.createErr = errors.New("startup catalog write unavailable")
				}
				exec := NewStorageExecutor(fault)
				fault.createErr = nil
				if arrival == "visible after construction" {
					seed()
				}
				var query string
				switch operation {
				case "install lowercase":
					query = "CALL apoc.custom.installProcedure('report() :: (v :: INTEGER)', 'RETURN 2 AS v')"
				case "drop lowercase":
					query = "CALL apoc.custom.dropProcedure('report')"
				case "drop exact":
					query = "CALL apoc.custom.dropProcedure('Report')"
				}
				_, err := exec.Execute(context.Background(), query, nil)
				if operation == "drop lowercase" {
					require.Error(t, err, "absent exact spelling must not delete a differently cased payload")
				} else {
					require.NoError(t, err)
				}
				ClearUserProcedures()
				exec = NewStorageExecutor(fault)
				nodes, err := base.GetNodesByLabel(procedureCatalogLabel)
				require.NoError(t, err)
				var names []string
				for _, node := range nodes {
					names = append(names, node.Properties["name"].(string))
				}
				sort.Strings(names)
				switch operation {
				case "drop exact":
					require.Empty(t, names)
					_, err = exec.Execute(context.Background(), "CALL custom.Report()", nil)
					require.Error(t, err)
				default:
					want := []string{"custom.Report"}
					if operation == "install lowercase" {
						want = append(want, "custom.report")
					}
					require.Equal(t, want, names)
					result, err := exec.Execute(context.Background(), "CALL custom.Report() YIELD v RETURN v", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					if operation == "install lowercase" {
						result, err = exec.Execute(context.Background(), "CALL custom.report() YIELD v RETURN v", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
					}
				}
			})
		}
	}
}

func TestProcedureCatalogMutationRecoveryFailureIsRetryable(t *testing.T) {
	for _, operation := range []string{"install", "drop"} {
		for _, stage := range []string{"read", "create", "delete"} {
			t.Run(operation+"/"+stage, func(t *testing.T) {
				ClearUserProcedures()
				t.Cleanup(ClearUserProcedures)
				base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				engine := &procedureIdentityFaultEngine{Engine: base}
				exec := NewStorageExecutor(engine)
				original := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"}
				blob, err := msgpack.Marshal(original)
				require.NoError(t, err)
				legacy := procedureCatalogNodeID("custom.report")
				_, err = base.CreateNode(&storage.Node{ID: legacy, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": original.Name, "record": base64.StdEncoding.EncodeToString(blob)}})
				require.NoError(t, err)
				fault := errors.New("mutation catalog " + stage + " unavailable")
				switch stage {
				case "read":
					engine.readErr = fault
				case "create":
					engine.createErr = fault
				case "delete":
					engine.deleteErr = fault
				}
				mutate := func() error {
					if operation == "drop" {
						return exec.dropPersistedProcedure(context.Background(), "custom.Report")
					}
					return exec.installPersistedProcedure(context.Background(), persistedProcedureRecord{Name: "custom.report", Mode: "READ", Body: "RETURN 2 AS v", Signature: "custom.report() :: (v :: INTEGER)"}, true)
				}
				require.ErrorIs(t, mutate(), fault)
				preserved, err := base.GetNode(legacy)
				require.NoError(t, err)
				record, err := decodeProcedureCatalogRecord(preserved)
				require.NoError(t, err)
				require.Equal(t, original.Name, record.Name)
				require.Equal(t, original.Body, record.Body)
				engine.readErr = nil
				engine.createErr = nil
				engine.deleteErr = nil
				require.NoError(t, mutate())
				ClearUserProcedures()
				exec = NewStorageExecutor(engine)
				nodes, err := base.GetNodesByLabel(procedureCatalogLabel)
				require.NoError(t, err)
				if operation == "drop" {
					require.Empty(t, nodes)
				} else {
					require.Len(t, nodes, 2)
					for _, tc := range []struct {
						name string
						want int64
					}{{"custom.Report", 1}, {"custom.report", 2}} {
						result, err := exec.Execute(context.Background(), "CALL "+tc.name+"() YIELD v RETURN v", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
					}
				}
			})
		}
	}
}

func TestProcedureCatalogMutationValidatesPayloadBeforeReuse(t *testing.T) {
	valid := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"}
	bytesFor := func(record persistedProcedureRecord) []byte {
		b, err := msgpack.Marshal(record)
		require.NoError(t, err)
		return b
	}
	for _, tc := range []struct {
		name    string
		payload interface{}
		valid   bool
	}{
		{"raw bytes", bytesFor(valid), true},
		{"unsupported payload", 17, false},
		{"invalid msgpack", base64.StdEncoding.EncodeToString([]byte{0xc1}), false},
		{"invalid definition", base64.StdEncoding.EncodeToString(bytesFor(persistedProcedureRecord{Name: "custom.Report", Mode: "INVALID", Body: "RETURN 1"})), false},
		{"unrelated key payload", base64.StdEncoding.EncodeToString(bytesFor(persistedProcedureRecord{Name: "custom.Other", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Other() :: (v :: INTEGER)"})), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearUserProcedures()
			t.Cleanup(ClearUserProcedures)
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			id := procedureCatalogNodeID("custom.report")
			_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": valid.Name, "record": tc.payload}})
			require.NoError(t, err)
			err = exec.installPersistedProcedure(context.Background(), persistedProcedureRecord{Name: "custom.report", Mode: "READ", Body: "RETURN 2 AS v", Signature: "custom.report() :: (v :: INTEGER)"}, true)
			if tc.valid {
				require.NoError(t, err)
				ClearUserProcedures()
				exec = NewStorageExecutor(store)
				result, err := exec.Execute(context.Background(), "CALL custom.Report() YIELD v RETURN v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			} else {
				require.Error(t, err)
				node, getErr := store.GetNode(id)
				require.NoError(t, getErr)
				require.Equal(t, tc.payload, node.Properties["record"], "invalid or unrelated payload must not be replaced")
			}
		})
	}
}
