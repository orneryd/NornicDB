package storage_test

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInstalledPrimitiveInputsAreDetached(t *testing.T) {
	for _, route := range []string{"standalone", "bundle", "compiled-bundle", "replace"} {
		t.Run(route, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer engine.Close()
			schema := engine.GetSchemaForNamespace("test")
			properties := []string{"required"}
			constraint := storage.Constraint{Name: "required", Type: storage.ConstraintNodeKey, Label: "PRIMITIVE", Properties: properties}
			contract := storage.ConstraintContract{Name: "owner", TargetEntityType: "NODE", TargetLabelOrType: "PRIMITIVE", Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintNodeKey), Properties: []string{"required"}}}}
			switch route {
			case "standalone":
				require.NoError(t, schema.AddConstraint(constraint))
			case "replace":
				require.NoError(t, schema.ReplaceFromDefinition(&storage.SchemaDefinition{Version: 1, Constraints: []storage.Constraint{constraint}}))
			case "bundle", "compiled-bundle":
				constraint.Name = storage.ConstraintContractEntryName(contract.Name, 0)
				if route == "bundle" {
					require.NoError(t, schema.AddConstraintContractBundle(contract, []storage.Constraint{constraint}, nil, false))
				} else {
					compiled, err := storage.CompileConstraintContract(contract)
					require.NoError(t, err)
					require.NoError(t, schema.AddCompiledConstraintContractBundle(compiled, []storage.Constraint{constraint}, nil, false))
				}
			}
			// Do not export/reload between installing and mutating the input.
			properties[0] = "replacement"
			require.Error(t, r11PrimitiveCommit(engine, "invalid", false, map[string]any{"replacement": int64(2)}))
			require.NoError(t, r11PrimitiveCommit(engine, "valid", false, map[string]any{"required": int64(2)}))
			require.Equal(t, []string{"required"}, schema.GetAllConstraints()[0].Properties)
		})
	}
}
