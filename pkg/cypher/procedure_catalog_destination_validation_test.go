package cypher

import (
	"context"
	"encoding/base64"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"testing"
)

func seedProcedureDestination(t *testing.T, store storage.Engine, id storage.NodeID, record persistedProcedureRecord) {
	t.Helper()
	blob, err := msgpack.Marshal(record)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: id, Labels: []string{procedureCatalogLabel}, Properties: map[string]interface{}{"name": record.Name, "record": base64.StdEncoding.EncodeToString(blob)}})
	require.NoError(t, err)
}
func TestProcedureCatalogRejectsUnrelatedMigrationDestination(t *testing.T) {
	for _, operation := range []string{"install lowercase", "drop lowercase", "load"} {
		t.Run(operation, func(t *testing.T) {
			ClearUserProcedures()
			t.Cleanup(ClearUserProcedures)
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			legacy := procedureCatalogNodeID("custom.report")
			destination := procedureCatalogNodeID("custom.Report")
			seedProcedureDestination(t, store, legacy, persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"})
			seedProcedureDestination(t, store, destination, persistedProcedureRecord{Name: "custom.Other", Mode: "READ", Body: "RETURN 9 AS v", Signature: "custom.Other() :: (v :: INTEGER)"})
			beforeLegacy, err := store.GetNode(legacy)
			require.NoError(t, err)
			beforeDestination, err := store.GetNode(destination)
			require.NoError(t, err)
			switch operation {
			case "install lowercase":
				_, err = exec.Execute(context.Background(), "CALL apoc.custom.installProcedure('report() :: (v :: INTEGER)', 'RETURN 2 AS v')", nil)
			case "drop lowercase":
				_, err = exec.Execute(context.Background(), "CALL apoc.custom.dropProcedure('report')", nil)
			case "load":
				err = exec.loadPersistedProcedures()
			}
			assert.Error(t, err, "an unrelated destination must never supersede a valid legacy definition")
			after, err := store.GetNode(legacy)
			require.NoError(t, err, "valid legacy record must survive")
			require.Equal(t, beforeLegacy.Properties, after.Properties)
			after, err = store.GetNode(destination)
			require.NoError(t, err)
			require.Equal(t, beforeDestination.Properties, after.Properties)
		})
	}
}

func TestProcedureCatalogMigrationValidatesDestination(t *testing.T) {
	for _, kind := range []string{"newer exact", "wrong name", "invalid definition", "corrupt payload", "ordinary node"} {
		t.Run(kind, func(t *testing.T) {
			ClearUserProcedures()
			t.Cleanup(ClearUserProcedures)
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			original := persistedProcedureRecord{Name: "custom.Report", Mode: "READ", Body: "RETURN 1 AS v", Signature: "custom.Report() :: (v :: INTEGER)"}
			legacyID := procedureCatalogNodeID("custom.report")
			destinationID := procedureCatalogNodeID(original.Name)
			seedProcedureDestination(t, store, legacyID, original)
			destination := original
			destination.Body = "RETURN 9 AS v"
			if kind == "wrong name" {
				destination.Name = "custom.Other"
				destination.Signature = "custom.Other() :: (v :: INTEGER)"
			}
			if kind == "invalid definition" {
				destination.Mode = "INVALID"
			}
			seedProcedureDestination(t, store, destinationID, destination)
			node, err := store.GetNode(destinationID)
			require.NoError(t, err)
			if kind == "corrupt payload" {
				node.Properties["record"] = 17
				require.NoError(t, store.UpdateNode(node))
			}
			if kind == "ordinary node" {
				node.Labels = []string{"Ordinary"}
				require.NoError(t, store.UpdateNode(node))
			}
			before, err := store.GetNode(legacyID)
			require.NoError(t, err)
			superseded, err := exec.migrateProcedureCatalogIdentity(store, before, original)
			if kind == "newer exact" {
				require.NoError(t, err)
				require.True(t, superseded)
				_, err = store.GetNode(legacyID)
				require.ErrorIs(t, err, storage.ErrNotFound)
				require.NoError(t, exec.loadPersistedProcedures())
				result, err := exec.Execute(context.Background(), "CALL custom.Report() YIELD v RETURN v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(9)}}, result.Rows)
			} else {
				require.Error(t, err)
				require.False(t, superseded)
				after, getErr := store.GetNode(legacyID)
				require.NoError(t, getErr)
				require.Equal(t, before.Properties, after.Properties)
				after, getErr = store.GetNode(destinationID)
				require.NoError(t, getErr)
				require.Equal(t, node.Properties, after.Properties)
				require.Equal(t, node.Labels, after.Labels)
			}
		})
	}
}
