package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstraintAdmissionRejectsNewRuleBeforeUniqueLookup(t *testing.T) {
	for _, kind := range []ConstraintEntityType{ConstraintEntityNode, ConstraintEntityRelationship} {
		t.Run(string(kind), func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: true})
			schema := engine.GetSchemaForNamespace("test")
			const held = "hidden-held"
			if kind == ConstraintEntityNode {
				_, err := engine.CreateNode(t.Context(), &Node{
					ID: "test:holder", Labels: []string{"Person"}, Properties: map[string]any{"key": held},
				})
				require.NoError(t, err)
			} else {
				relatedEndpointNodes(t, engine)
				require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
					ID: "test:holder-edge", Type: "LINK", StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"key": held},
				}))
			}
			label := "Person"
			if kind == ConstraintEntityRelationship {
				label = "LINK"
			}
			denied := errors.New("fine-grained write forbidden")
			ctx := WithConstraintAdmission(t.Context(), denied)
			RecordConstraintAdmission(ctx, kind, []string{label}, nil) // Authorization saw no unique rule.
			var staged []*BadgerTransaction
			for _, value := range []string{held, "absent"} {
				tx, err := engine.BeginTransaction(t.Context(), OrdinaryBegin)
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetValidationSetting(DeferredConstraintValidation, true))
				if kind == ConstraintEntityNode {
					_, err = tx.CreateNode(ctx, &Node{ID: NodeID("test:staged-" + value), Labels: []string{label}, Properties: map[string]any{"key": value}})
				} else {
					err = tx.CreateEdge(ctx, &Edge{ID: EdgeID("test:staged-" + value), Type: label, StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"key": value}})
				}
				require.NoError(t, err)
				staged = append(staged, tx)
			}
			require.NoError(t, schema.AddConstraint(Constraint{
				Name: "new_key", Type: ConstraintUnique, EntityType: kind, Label: label, Properties: []string{"key"},
			}))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, schema))
			for _, value := range []string{held, "absent"} {
				if kind == ConstraintEntityNode {
					_, err := engine.CreateNode(ctx, &Node{ID: NodeID("test:probe-" + value), Labels: []string{label}, Properties: map[string]any{"key": value}})
					require.ErrorIs(t, err, denied)
					err = engine.BulkCreateNodes(ctx, []*Node{{ID: NodeID("test:bulk-probe-" + value), Labels: []string{label}, Properties: map[string]any{"key": value}}})
					require.ErrorIs(t, err, denied)
				} else {
					err := engine.CreateEdge(ctx, &Edge{ID: EdgeID("test:probe-" + value), Type: label, StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"key": value}})
					require.ErrorIs(t, err, denied)
					err = engine.BulkCreateEdges(ctx, []*Edge{{ID: EdgeID("test:bulk-probe-" + value), Type: label, StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"key": value}}})
					require.ErrorIs(t, err, denied)
				}
				tx, err := engine.BeginTransaction(ctx, OrdinaryBegin)
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				if kind == ConstraintEntityNode {
					_, err = tx.CreateNode(ctx, &Node{ID: NodeID("test:tx-probe-" + value), Labels: []string{label}, Properties: map[string]any{"key": value}})
				} else {
					err = tx.CreateEdge(ctx, &Edge{ID: EdgeID("test:tx-probe-" + value), Type: label, StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"key": value}})
				}
				if err == nil {
					err = tx.CommitContext(ctx)
				}
				require.ErrorIs(t, err, denied)
				_ = tx.Rollback() // A rejected commit may already have closed the transaction.
			}
			for _, tx := range staged {
				require.ErrorIs(t, tx.CommitContext(t.Context()), denied, "the commit context deliberately has no admission record")
				_ = tx.Rollback()
			}
		})
	}
}
