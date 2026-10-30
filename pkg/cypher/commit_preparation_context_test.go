package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// COMMIT must carry Execute's caller context through native preparation before
// acceptance. This exercises the public transaction consumer under both parsers.
func TestCommitStatementPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"pendingEntityAuthorityKeysLocked", "appendConstraintValueKeysWithContext", "validateConstraintContracts", "validateSnapshotIsolationConflicts", "GetConstraintsForLabelsWithContext", "GetPropertyTypeConstraintsForLabelsWithContext", "value-key-schema", "contract-image"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("commit_p", "P", "k"))
			if owner == "GetConstraintsForLabelsWithContext" || owner == "GetPropertyTypeConstraintsForLabelsWithContext" || owner == "value-key-schema" {
				for i := 0; i < 20; i++ {
					label := fmt.Sprintf("Other%d", i)
					require.NoError(t, schema.AddConstraint(storage.Constraint{Name: "exists_" + label, Type: storage.ConstraintExists, Label: label, Properties: []string{"k"}}))
					require.NoError(t, schema.AddPropertyTypeConstraint("type_"+label, label, "k", storage.PropertyTypeInteger))
				}
			}
			entries := []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}}
			if owner == "contract-image" {
				for len(entries) < 20 {
					entries = append(entries, entries[0])
				}
			}
			require.NoError(t, schema.AddConstraintContractBundle(storage.ConstraintContract{Name: "core_only", TargetEntityType: string(storage.ConstraintEntityNode), TargetLabelOrType: "P", Entries: entries}, nil, nil, false))
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			t.Cleanup(func() { _, _ = e.Execute(context.Background(), "ROLLBACK", nil) })
			run := func(query string) *ExecuteResult {
				t.Helper()
				r, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				return r
			}
			prepare := func() {
				run("BEGIN")
				run("UNWIND range(1,20) AS i CREATE (:P {k:i, team:'core'})")
			}
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			if owner == "value-key-schema" {
				ctx.owner, ctx.via, ctx.after = "GetConstraintsForLabelsWithContext", ".appendConstraintValueKeysWithContext", 3
			}
			if owner == "contract-image" {
				ctx.owner, ctx.via, ctx.after = "cloneConstraintContractWithContext", ".GetConstraintContractsForTargetWithContext", 3
			}
			result, err := e.Execute(ctx, "COMMIT", nil)
			if owner == "value-key-schema" || owner == "contract-image" {
				stored, readErr := b.GetStoredNodesByLabel("P")
				require.NoError(t, readErr)
				t.Logf("observations=%d commit=%v nodes=%d", ctx.hits, err, len(stored))
			}
			require.GreaterOrEqual(t, ctx.hits, 2, "public COMMIT must reach its preparation owner")
			require.ErrorIs(t, err, context.Canceled)
			if result != nil {
				require.Empty(t, result.Rows)
			}
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			stored, err := b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Empty(t, stored, "failed preparation cannot publish a node")
			prepare()
			run("COMMIT")
			require.Len(t, run("MATCH (n:P) RETURN n.k").Rows, 20)
		})
	}
}

func TestConnectedDeleteStatementPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"resolveConnectedDeletesLocked", "incidentEdgesLocked.func3", "checkNodeAdjacencyConflict.func1"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(query string) {
				t.Helper()
				_, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
			}
			run("UNWIND range(1,12) AS i CREATE (:P {k:i})")
			_, err = e.Execute(context.Background(), "CREATE (:Q {k:0})", nil)
			require.NoError(t, err)
			for i := 1; i <= 12; i++ {
				run(fmt.Sprintf("MATCH (n:P {k:%d}), (q:Q) CREATE (n)-[:R]->(q)", i))
			}
			prepare := func() { run("BEGIN"); run("MATCH (n:P)-[r:R]->(:Q) DELETE n WITH r DELETE r") }
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			_, err = e.Execute(ctx, "COMMIT", nil)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			stored, err := b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Len(t, stored, 12)
			prepare()
			run("COMMIT")
			stored, err = b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Empty(t, stored)
		})
	}
}
