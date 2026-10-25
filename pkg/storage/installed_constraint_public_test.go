package storage_test

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func r73DomainWrite(engine *storage.BadgerEngine, id string, value any) error {
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = tx.SetNamespace("test"); err != nil {
		return err
	}
	if _, err = tx.CreateNode(&storage.Node{ID: storage.NodeID("test:" + id), Labels: []string{"D"}, Properties: map[string]any{"p": value}}); err != nil {
		return err
	}
	return tx.Commit()
}

func r73InstallDomain(t *testing.T, schema *storage.SchemaManager, route string, values []any) {
	t.Helper()
	c := storage.Constraint{Name: "domain", Type: storage.ConstraintDomain, Label: "D", Properties: []string{"p"}, AllowedValues: values}
	switch route {
	case "standalone":
		require.NoError(t, schema.AddConstraint(c))
	case "replace":
		require.NoError(t, schema.ReplaceFromDefinition(&storage.SchemaDefinition{Version: 1, Constraints: []storage.Constraint{c}}))
	case "bundle", "compiled-bundle":
		contract := storage.ConstraintContract{Name: "owner", TargetEntityType: "NODE", TargetLabelOrType: "D", Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintDomain), Properties: []string{"p"}}}}
		c.Name = storage.ConstraintContractEntryName(contract.Name, 0)
		if route == "bundle" {
			require.NoError(t, schema.AddConstraintContractBundle(contract, []storage.Constraint{c}, nil, false))
		} else {
			compiled, err := storage.CompileConstraintContract(contract)
			require.NoError(t, err)
			require.NoError(t, schema.AddCompiledConstraintContractBundle(compiled, []storage.Constraint{c}, nil, false))
		}
	}
}

func TestInstalledDomainTypedInputOwnership(t *testing.T) {
	// Every mutable scalar-list representation admitted by property_validation.
	for _, example := range []any{[]byte{1}, []string{"one"}, []int{1}, []int32{1}, []int64{9007199254740993}, []float32{1}, []float64{1}, []bool{false}} {
		t.Run(fmt.Sprintf("%T", example), func(t *testing.T) {
			value := reflect.MakeSlice(reflect.TypeOf(example), 1, 1)
			reflect.Copy(value, reflect.ValueOf(example))
			original := reflect.MakeSlice(value.Type(), 1, 1)
			reflect.Copy(original, value)
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer engine.Close()
			r73InstallDomain(t, engine.GetSchemaForNamespace("test"), "standalone", []any{value.Interface()})
			item := value.Index(0)
			switch item.Kind() {
			case reflect.String:
				item.SetString("two")
			case reflect.Bool:
				item.SetBool(true)
			case reflect.Uint8:
				item.SetUint(2)
			case reflect.Float32, reflect.Float64:
				item.SetFloat(2)
			default:
				item.SetInt(item.Int() + 1)
			}
			require.False(t, reflect.DeepEqual(value.Interface(), original.Interface()), "mutation must be effective")
			require.Error(t, r73DomainWrite(engine, "changed", value.Interface()), "changing installation input changed the rule")
			require.NoError(t, r73DomainWrite(engine, "original", original.Interface()))
		})
	}
}

func TestInstalledDomainPublicRoutes(t *testing.T) {
	for _, route := range []string{"standalone", "bundle", "compiled-bundle", "replace"} {
		for _, subject := range []string{"input", "getter", "definition", "violation"} {
			t.Run(route+"/"+subject, func(t *testing.T) {
				engine, err := storage.NewBadgerEngineInMemory()
				require.NoError(t, err)
				defer engine.Close()
				schema := engine.GetSchemaForNamespace("test")
				input := []int64{1}
				r73InstallDomain(t, schema, route, []any{input})
				switch subject {
				case "input":
					input[0] = 2
				case "getter":
					schema.GetAllConstraints()[0].AllowedValues[0].([]int64)[0] = 2
				case "definition":
					schema.ExportDefinition().Constraints[0].AllowedValues[0].([]int64)[0] = 2
				case "violation":
					err = r73DomainWrite(engine, "first", []int64{2})
					require.Error(t, err)
					var violation *storage.ConstraintViolationError
					require.True(t, errors.As(err, &violation))
					violation.Properties[0] = "replacement"
					require.Equal(t, "replacement", violation.Properties[0], "mutation must be effective")
				}
				require.Error(t, r73DomainWrite(engine, "changed", []int64{2}))
				require.NoError(t, r73DomainWrite(engine, "original", []any{int64(1)}), "typed/untyped list comparison changed")
				require.Equal(t, []int64{1}, schema.GetAllConstraints()[0].AllowedValues[0])
			})
		}
	}
}
