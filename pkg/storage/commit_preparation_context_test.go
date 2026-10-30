package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// These owners prepare populations before holdPublication and physical writes.
// Accepted commit completion and notification delivery retain their own lifetime.
func TestCommitPreparationCancellation(t *testing.T) {
	for _, owner := range []string{
		"pendingEntityAuthorityKeysLocked", "entityCommitKeysWithContext",
		"appendConstraintValueKeysWithContext", "acquireUniqueConstraintCommitLocks",
		"sortIndexCandidatesWithContext", "validateConstraintContracts",
		"transactionTouchesConstraintContracts", "GetConstraintContractsForTargetWithContext", "contract-image",
		"currentOutgoingEdgesLocked", "currentIncomingEdgesLocked", "currentAdjacentEdgesLocked",
	} {
		t.Run(owner, func(t *testing.T) {
			b := createTestBadgerEngine(t)
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("commit_p", "P", "k"))
			entries := []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}}
			if owner == "contract-image" {
				for len(entries) < 20 {
					entries = append(entries, entries[0])
				}
			}
			require.NoError(t, schema.AddConstraintContractBundle(ConstraintContract{Name: "core_only", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "P", Entries: entries}, nil, nil, false))
			prepare := func() *BadgerTransaction {
				t.Helper()
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				for i := 0; i < 12; i++ {
					_, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:p%d", i)), Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(i), "team": "core"}})
					require.NoError(t, err)
				}
				for i := 0; i < 12; i++ {
					require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: "test:p0", EndNode: "test:p1", Type: "REL"}))
				}
				return tx
			}
			tx := prepare()
			ctx := preparationContext(t, owner, 2)
			if owner == "contract-image" {
				ctx.owner, ctx.via = "cloneConstraintContractWithContext", ".GetConstraintContractsForTargetWithContext"
			}
			err := tx.CommitContext(ctx)
			if owner == "contract-image" {
				rows, readErr := b.GetStoredNodesByLabel("P")
				require.NoError(t, readErr)
				t.Logf("observations=%d commit=%v stored=%d", ctx.hits, err, len(rows))
			}
			require.GreaterOrEqual(t, ctx.hits, 2, "cancel inside pre-publication preparation")
			require.ErrorIs(t, err, context.Canceled)
			_, err = b.GetStoredNode("test:p0")
			require.ErrorIs(t, err, ErrNotFound, "failed preparation cannot publish staged nodes")
			require.NoError(t, prepare().CommitContext(context.Background()), "fresh retry must release every authority/value reservation")
			_, err = b.GetStoredNode("test:p0")
			require.NoError(t, err)
		})
	}
}
