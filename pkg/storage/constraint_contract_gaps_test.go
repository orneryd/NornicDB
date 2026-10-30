package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func gapContract() (ConstraintContract, []Constraint, []PropertyTypeConstraint) {
	contract := ConstraintContract{
		Name: "person_contract", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintUnique), Property: "id", Properties: []string{"id"}},
			{Kind: ConstraintContractKindBooleanNode, Expression: "n.status IN ['a', 'b']"},
			{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintPropertyType), Property: "age", Properties: []string{"age"}, ExpectedType: string(PropertyTypeInteger)},
		},
	}
	unique := Constraint{Name: ConstraintContractEntryName(contract.Name, 0), Type: ConstraintUnique, EntityType: ConstraintEntityNode, Label: "Person", Properties: []string{"id"}}
	typed := PropertyTypeConstraint{Name: ConstraintContractEntryName(contract.Name, 2), EntityType: ConstraintEntityNode, Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger}
	return contract, []Constraint{unique}, []PropertyTypeConstraint{typed}
}

func TestDropConstraint_ContractRemovesCompiledEntries(t *testing.T) {
	sm := NewSchemaManager()
	contract, compiled, types := gapContract()
	require.NoError(t, sm.AddConstraintContractBundle(contract, compiled, types, false))
	require.Len(t, sm.GetAllPropertyTypeConstraints(), 1)
	_, hasUnique := sm.uniqueConstraints["Person:id"]
	require.True(t, hasUnique)

	require.NoError(t, sm.DropConstraint("person_contract"))
	require.Empty(t, sm.GetAllConstraintContracts())
	require.Empty(t, sm.constraints)
	require.Empty(t, sm.GetAllPropertyTypeConstraints())
	_, hasUnique = sm.uniqueConstraints["Person:id"]
	require.False(t, hasUnique, "the unique lookup owned by the compiled entry is released")

	err := sm.DropConstraint("person_contract")
	require.ErrorContains(t, err, "does not exist")
}

func TestDropConstraint_ContractToleratesEntryDroppedIndividually(t *testing.T) {
	sm := NewSchemaManager()
	contract, compiled, types := gapContract()
	require.NoError(t, sm.AddConstraintContractBundle(contract, compiled, types, false))
	require.NoError(t, sm.DropConstraint(ConstraintContractEntryName("person_contract", 0)))

	require.NoError(t, sm.DropConstraint("person_contract"))
	require.Empty(t, sm.GetAllConstraintContracts())
	require.Empty(t, sm.GetAllPropertyTypeConstraints())
}

func TestDropConstraint_ContractPersistFailureRestoresEverything(t *testing.T) {
	sm := NewSchemaManager()
	contract, compiled, types := gapContract()
	require.NoError(t, sm.AddConstraintContractBundle(contract, compiled, types, false))

	sm.persist = func(*SchemaDefinition) error { return errors.New("persist failed") }
	require.ErrorContains(t, sm.DropConstraint("person_contract"), "persist failed")

	require.Len(t, sm.GetAllConstraintContracts(), 1)
	require.Contains(t, sm.constraints, ConstraintContractEntryName("person_contract", 0))
	require.Len(t, sm.GetAllPropertyTypeConstraints(), 1)
	_, hasUnique := sm.uniqueConstraints["Person:id"]
	require.True(t, hasUnique)
}

func TestDropConstraint_PrimitivePersistFailureRestores(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyTypeConstraint("age_type", "Person", "age", PropertyTypeInteger))
	sm.persist = func(*SchemaDefinition) error { return errors.New("persist failed") }
	require.Error(t, sm.DropConstraint("age_type"))
	require.Len(t, sm.GetAllPropertyTypeConstraints(), 1)
}

func TestValidateConstraintContractExpressions(t *testing.T) {
	ok := ConstraintContract{Name: "ok", Entries: []ConstraintContractEntry{
		{Kind: ConstraintContractKindBooleanNode, Expression: "n.status IN ['a']"},
		{Kind: ConstraintContractKindBooleanNode, Expression: "COUNT { (n)-[:R]->() } <= 1"},
		{Kind: ConstraintContractKindBooleanRelationship, Expression: "startNode(r) <> endNode(r)"},
		{Kind: ConstraintContractKindPrimitiveNode, Expression: "n.id IS UNIQUE"},
	}}
	require.NoError(t, ValidateConstraintContractExpressions(ok))

	badNode := ConstraintContract{Name: "bad", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.a IS NULL OR n.a IN ['x']"}}}
	require.ErrorContains(t, ValidateConstraintContractExpressions(badNode), "unsupported node predicate")
	badRel := ConstraintContract{Name: "bad", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.a = 1 OR r.b = 2"}}}
	require.ErrorContains(t, ValidateConstraintContractExpressions(badRel), "constraint contract bad invalid")
}

func TestValidatePropertyType_List(t *testing.T) {
	strings := ListPropertyType(PropertyTypeString)
	ints := ListPropertyType(PropertyTypeInteger)
	require.Equal(t, PropertyType("LIST<STRING NOT NULL>"), strings)

	element, ok := ListElementPropertyType(ints)
	require.True(t, ok)
	require.Equal(t, PropertyTypeInteger, element)
	for _, notList := range []PropertyType{PropertyTypeString, "LIST<STRING>", "LIST< NOT NULL>"} {
		_, ok := ListElementPropertyType(notList)
		require.False(t, ok, string(notList))
	}

	valid := []struct {
		value interface{}
		typ   PropertyType
	}{
		{[]interface{}{"a", "b"}, strings},
		{[]string{"a"}, strings},
		{[]string{}, strings},
		{[]int64{1, 2}, ints},
		{[]interface{}{int64(1), float64(2)}, ints}, // whole floats from JSON/msgpack count as integers
		{[]float64{1.5}, ListPropertyType(PropertyTypeFloat)},
		{[]bool{true}, ListPropertyType(PropertyTypeBoolean)},
		{nil, strings},
	}
	for _, tc := range valid {
		require.NoError(t, ValidatePropertyType(tc.value, tc.typ), "%#v", tc.value)
	}

	invalid := []struct {
		value interface{}
		typ   PropertyType
	}{
		{"a", strings},
		{[]interface{}{"a", int64(1)}, strings},
		{[]interface{}{"a", nil}, strings},
		{[]float64{1.5}, ints},
		{map[string]interface{}{"a": 1}, strings},
	}
	for _, tc := range invalid {
		require.Error(t, ValidatePropertyType(tc.value, tc.typ), "%#v", tc.value)
	}
}
