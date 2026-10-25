package storage_test

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func r11PrimitiveCommit(engine *storage.BadgerEngine, id string, rel bool, props map[string]any) error {
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = tx.SetNamespace("test"); err != nil {
		return err
	}
	if !rel {
		if _, err = tx.CreateNode(&storage.Node{ID: storage.NodeID("test:" + id), Labels: []string{"PRIMITIVE"}, Properties: props}); err != nil {
			return err
		}
	} else {
		for _, suffix := range []string{"a", "b"} {
			if _, err = tx.CreateNode(&storage.Node{ID: storage.NodeID("test:" + id + suffix), Labels: []string{"End"}}); err != nil {
				return err
			}
		}
		if err = tx.CreateEdge(&storage.Edge{ID: storage.EdgeID("test:" + id), StartNode: storage.NodeID("test:" + id + "a"), EndNode: storage.NodeID("test:" + id + "b"), Type: "PRIMITIVE", Properties: props}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Install a supported primitive contract, obtain its real public violation,
// and change only the returned error. Required-property enforcement must stay.
func TestReviewR11PrimitiveViolationDetached(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, route := range []string{"bundle", "reload"} {
			t.Run(fmt.Sprintf("relationship=%v/%s", rel, route), func(t *testing.T) {
				engine, err := storage.NewBadgerEngineInMemory()
				require.NoError(t, err)
				defer engine.Close()
				kind, entity, ct := storage.ConstraintContractKindPrimitiveNode, storage.ConstraintEntityNode, storage.ConstraintNodeKey
				expression := "(n.required) IS NODE KEY"
				if rel {
					kind, entity, ct = storage.ConstraintContractKindPrimitiveRelationship, storage.ConstraintEntityRelationship, storage.ConstraintRelationshipKey
					expression = "(r.required) IS RELATIONSHIP KEY"
				}
				contract := storage.ConstraintContract{Name: "primitive-owned", TargetEntityType: string(entity), TargetLabelOrType: "PRIMITIVE", Entries: []storage.ConstraintContractEntry{{Kind: kind, PrimitiveType: string(ct), Properties: []string{"required"}, Expression: expression}}}
				compiled, err := storage.CompileConstraintContract(contract)
				require.NoError(t, err)
				companion := storage.Constraint{Name: storage.ConstraintContractEntryName(contract.Name, 0), Type: ct, EntityType: entity, Label: "PRIMITIVE", Properties: []string{"required"}}
				schema := engine.GetSchemaForNamespace("test")
				require.NoError(t, schema.AddCompiledConstraintContractBundle(compiled, []storage.Constraint{companion}, nil, false))
				if route == "reload" {
					require.NoError(t, schema.ReplaceFromDefinition(schema.ExportDefinition()))
				}
				props := map[string]any{}
				if rel {
					require.NoError(t, r11PrimitiveCommit(engine, "seed", rel, map[string]any{"required": int64(1)}))
					props = map[string]any{"required": int64(1)}
				}
				first := r11PrimitiveCommit(engine, "rejected", rel, props)
				require.Error(t, first)
				var violation *storage.ConstraintViolationError
				require.True(t, errors.As(first, &violation))
				require.Equal(t, []string{"required"}, violation.Properties)
				violation.Properties[0] = "replacement"
				descriptions := schema.GetAllConstraintContracts()
				require.Len(t, descriptions, 1)
				require.Equal(t, []string{"required"}, descriptions[0].Entries[0].Properties)
				installed := schema.GetAllConstraints()
				require.Len(t, installed, 1)
				next := r11PrimitiveCommit(engine, "after-error-mutation", rel, map[string]any{"replacement": int64(2)})
				t.Logf("retained companion properties=%v; immutable contract properties=%v; next commit error=%v", installed[0].Properties, descriptions[0].Entries[0].Properties, next)
				if next == nil {
					t.Error("editing only a public violation let a later commit omit the contract's required property")
				}
				require.Equal(t, []string{"required"}, installed[0].Properties, "public violation aliases retained companion compile data")
			})
		}
	}
}
