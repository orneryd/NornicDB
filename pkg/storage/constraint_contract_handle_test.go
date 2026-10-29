package storage_test

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func handleTestContract(expression string) storage.ConstraintContract {
	return storage.ConstraintContract{Name: "handle_rule", TargetEntityType: string(storage.ConstraintEntityNode), TargetLabelOrType: "HandlePerson",
		Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindBooleanNode, Expression: expression}}}
}

func commitHandlePerson(engine *storage.BadgerEngine, id storage.NodeID, age int64) error {
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.SetNamespace("test"); err != nil {
		return err
	}
	if _, err := tx.CreateNode(&storage.Node{ID: id, Labels: []string{"HandlePerson"}, Properties: map[string]any{"age": age}}); err != nil {
		return err
	}
	return tx.Commit()
}

// An installed contract belongs to the schema. Whatever a caller later does
// with its compiled handle, even through a pointer to it, the schema keeps the
// revision it installed, and commits keep enforcing it.
func TestConstraintContractInstalledRevisionOwnsHandle(t *testing.T) {
	for _, mode := range []string{"overwrite-compiled", "zero-handle"} {
		t.Run(mode, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer engine.Close()
			schema := engine.GetSchemaForNamespace("test")
			admitted, err := storage.CompileConstraintContract(handleTestContract("n.age >= 10"))
			require.NoError(t, err)
			require.NoError(t, schema.AddCompiledConstraintContractBundle(admitted, nil, nil, false))
			require.Error(t, commitHandlePerson(engine, "test:before", 5), "the installed rule rejects age 5")

			handle := &admitted
			if mode == "overwrite-compiled" {
				other, err := storage.CompileConstraintContract(handleTestContract("n.age >= 0"))
				require.NoError(t, err)
				*handle = other
			} else {
				*handle = storage.CompiledConstraintContract{}
			}
			installed := schema.GetAllConstraintContracts()
			require.Len(t, installed, 1)
			require.Equal(t, "n.age >= 10", installed[0].Entries[0].Expression, "the caller's handle is not the installed revision")
			require.Error(t, commitHandlePerson(engine, "test:after", 5), "the installed rule still rejects age 5")
			require.NoError(t, commitHandlePerson(engine, "test:adult", 15))
		})
	}
}

// The zero handle holds no contract: it cannot be installed or checked.
func TestConstraintContractZeroHandleIsRejected(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer engine.Close()
	var zero storage.CompiledConstraintContract
	require.Error(t, engine.GetSchemaForNamespace("test").AddCompiledConstraintContractBundle(zero, nil, nil, false))
	require.Error(t, storage.ValidateCompiledConstraintContractOnCreationForEngine(engine, zero))
	require.Empty(t, engine.GetSchemaForNamespace("test").GetAllConstraintContracts())
	require.Equal(t, storage.ConstraintContract{}, zero.Contract())
}

// Replacing or reloading a schema installs the definition as given, checked by
// real commits; editing the supplied definition afterwards changes nothing.
func TestConstraintContractReplacementAndReloadCommit(t *testing.T) {
	for _, mode := range []string{"replacement", "serialized-reload", "changed-kind", "previously-invalid"} {
		t.Run(mode, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer engine.Close()
			schema := engine.GetSchemaForNamespace("test")
			initial := handleTestContract("n.age >= 0")
			if mode == "previously-invalid" {
				initial.Entries[0].Expression = "unsupported()"
			}
			require.NoError(t, schema.AddConstraintContractBundle(initial, nil, nil, false))
			if mode == "changed-kind" {
				for _, id := range []storage.NodeID{"test:a", "test:b"} {
					_, err = engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{}})
					require.NoError(t, err)
				}
			}
			definition := schema.ExportDefinition()
			definition.ConstraintContracts[0].Entries[0].Expression = "n.age >= 10"
			if mode == "changed-kind" {
				contract := &definition.ConstraintContracts[0]
				contract.TargetEntityType, contract.TargetLabelOrType = string(storage.ConstraintEntityRelationship), "KNOWS"
				contract.Entries[0] = storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindBooleanRelationship, Expression: "r.w >= 10"}
			}
			if mode == "serialized-reload" {
				data, err := json.Marshal(definition)
				require.NoError(t, err)
				definition = &storage.SchemaDefinition{}
				require.NoError(t, json.Unmarshal(data, definition))
			}
			require.NoError(t, schema.ReplaceFromDefinition(definition))
			definition.ConstraintContracts[0].Entries[0].Expression = "unsupported()"
			for _, value := range []int64{5, 15} {
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				if mode == "changed-kind" {
					err = tx.CreateEdge(&storage.Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: map[string]any{"w": value}})
				} else {
					_, err = tx.CreateNode(&storage.Node{ID: "test:p", Labels: []string{"HandlePerson"}, Properties: map[string]any{"age": value}})
				}
				if err == nil {
					err = tx.Commit()
				}
				tx.Rollback()
				if value == 5 {
					require.ErrorContains(t, err, "constraint contract handle_rule violated")
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
