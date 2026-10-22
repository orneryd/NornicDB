package storage

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func countConstraintContractCompiles(t *testing.T) *int {
	t.Helper()
	count := new(int)
	hook := func() { *count++ }
	require.Nil(t, constraintContractCompileHook.Swap(&hook))
	t.Cleanup(func() { constraintContractCompileHook.Store(nil) })
	return count
}

func ownsItemContract() ConstraintContract {
	return ConstraintContract{
		Name:              "owns_item",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanNode, Expression: "COUNT { (n)-[:OWNS]->(:Item) } >= 1"},
			{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"},
		},
	}
}

// A contract's predicates compile once per revision: once for a CREATE
// CONSTRAINT (admission, its scan of existing data and the stored schema),
// once per reload, and never when writes are checked.
func TestConstraintContractPlansCompileOncePerRevision(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer engine.Close()
	const people = 24
	for i := 0; i < people; i++ {
		person, item := NodeID(fmt.Sprintf("test:p%d", i)), NodeID(fmt.Sprintf("test:i%d", i))
		_, err := engine.CreateNode(&Node{ID: person, Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: item, Labels: []string{"Item"}, Properties: map[string]any{}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:o%d", i)), StartNode: person, EndNode: item, Type: "OWNS"}))
	}
	compiles := countConstraintContractCompiles(t)
	contract := ownsItemContract()
	entries := len(contract.Entries)

	require.NoError(t, ValidateConstraintContractOnCreationForEngine(engine, contract))
	require.Equal(t, entries, *compiles, "creation scan compiles each entry once for all %d entities", people)
	// The scan reads every entity: one without an item rejects the contract.
	_, err = engine.CreateNode(&Node{ID: "test:scanned", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	*compiles = 0
	require.ErrorContains(t, ValidateConstraintContractOnCreationForEngine(engine, contract), "constraint contract owns_item violated")
	require.Equal(t, entries, *compiles)
	require.NoError(t, engine.DeleteNode("test:scanned"))

	sm := engine.GetSchemaForNamespace("test")
	*compiles = 0
	compiled, err := CompileConstraintContract(contract)
	require.NoError(t, err)
	require.NoError(t, ValidateCompiledConstraintContractOnCreationForEngine(engine, compiled))
	require.NoError(t, sm.AddCompiledConstraintContractBundle(compiled, nil, nil, false))
	require.Equal(t, entries, *compiles, "one compilation per entry for the whole CREATE CONSTRAINT")
	require.NoError(t, sm.DropConstraint(contract.Name))
	*compiles = 0
	require.NoError(t, sm.AddConstraintContractBundle(contract, nil, nil, false))
	require.Equal(t, entries, *compiles, "a description is compiled once when stored")

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	*compiles = 0
	for i := 0; i < people; i++ {
		node, err := tx.currentNodeLocked(NodeID(fmt.Sprintf("test:p%d", i)))
		require.NoError(t, err)
		require.NoError(t, tx.validateConstraintContractsForNodeLocked(node))
	}
	lonely := &Node{ID: "test:lonely", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}}
	require.ErrorContains(t, tx.validateConstraintContractsForNodeLocked(lonely), "constraint contract owns_item violated")
	require.Zero(t, *compiles, "commit validation evaluates the stored revision")

	// A reload, from a snapshot or from persisted JSON, is a new revision of
	// the description it is given: it compiles once, then serves every check.
	encoded, err := json.Marshal(sm.ExportDefinition())
	require.NoError(t, err)
	persisted := &SchemaDefinition{}
	require.NoError(t, json.Unmarshal(encoded, persisted))
	for _, definition := range []*SchemaDefinition{sm.ExportDefinition(), persisted} {
		reloaded := NewSchemaManager()
		*compiles = 0
		require.NoError(t, reloaded.ReplaceFromDefinition(definition))
		require.Equal(t, entries, *compiles)
		*compiles = 0
		for _, revision := range reloaded.constraintContractRevisionsForTarget(ConstraintEntityNode, "Person") {
			require.Error(t, validateConstraintContractForNodeEngine(engine, revision, lonely))
		}
		require.Zero(t, *compiles)
	}
}

// Plans never change what a contract decides or reports: a stored revision
// and the expression evaluated directly agree, and an entry that cannot
// compile fails where it is evaluated, with the same error.
func TestConstraintContractPlanMatchesExpressionEvaluation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Item"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "OWNS", Properties: map[string]any{"w": int64(2)}}))
	node, err := engine.GetNode("test:a")
	require.NoError(t, err)
	edge, err := engine.GetEdge("test:e")
	require.NoError(t, err)
	view := engineConstraintView{engine}

	for _, expression := range []string{"COUNT { (n)-[:OWNS]->(:Item) } >= 1", "COUNT { (n)-[:`OWNS`]->(:Item) } > 1", "NOT EXISTS { (n)-[:OWNS]->() }", "n.team IN ['core']", "n.team = 'x'", "unsupported()"} {
		wantOK, wantErr := evaluateNodeConstraintContractExpression(view, node, expression)
		revision := compileConstraintContractRevision(ConstraintContract{Name: "c", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: expression}}})
		require.Equal(t, wantErr == nil, revision.plans[0].err == nil, expression)
		gotOK, gotErr := evaluateNodeConstraintContractPlan(view, node, &revision.plans[0])
		require.Equal(t, wantOK, gotOK, expression)
		require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr), expression)
	}
	for _, expression := range []string{"r.w IN [2]", "r.w >= 3", "startNode(r) <> endNode(r)", "startNode(r).team = endNode(r).team", "weird()"} {
		wantOK, wantErr := evaluateRelationshipConstraintContractExpression(view, edge, expression)
		revision := compileConstraintContractRevision(ConstraintContract{Name: "c", Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: expression}}})
		gotOK, gotErr := evaluateRelationshipConstraintContractPlan(view, edge, &revision.plans[0])
		require.Equal(t, wantOK, gotOK, expression)
		require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr), expression)
	}

	// Creation still rejects an unsupported predicate on an empty graph,
	// before any entity is read, with the validation error.
	invalid := ConstraintContract{Name: "bad", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Nobody",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "unsupported()"}}}
	want := ValidateConstraintContractExpressions(invalid)
	require.Error(t, want)
	require.EqualError(t, ValidateConstraintContractOnCreationForEngine(engine, invalid), want.Error())
	_, err = CompileConstraintContract(invalid)
	require.EqualError(t, err, want.Error())
}

// A contract description is editable data; its compiled revision is not.
// Whatever path a description takes (a getter copy, a snapshot, persisted
// JSON, a replaced definition, a compiled revision's input), what is checked
// is always the description as it is now.
func TestConstraintContractRevisionFollowsItsDescription(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	_, err := engine.CreateNode(&Node{ID: "test:p", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(5)}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:q", Labels: []string{"Person"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:r", StartNode: "test:p", EndNode: "test:q", Type: "KNOWS", Properties: map[string]any{"w": int64(5)}}))
	adult := ConstraintContract{Name: "age_rule", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person",
		Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 0"}}}
	schema := NewSchemaManager()
	require.NoError(t, schema.AddConstraintContractBundle(adult, nil, nil, false))
	stricter := func(contract ConstraintContract) ConstraintContract {
		contract.Entries[0].Expression = "n.age >= 10"
		return contract
	}
	violates := func(t *testing.T, contract ConstraintContract) {
		t.Helper()
		require.ErrorContains(t, ValidateConstraintContractOnCreationForEngine(engine, contract), "n.age >= 10")
	}

	t.Run("getter", func(t *testing.T) {
		violates(t, stricter(schema.GetAllConstraintContracts()[0]))
		violates(t, stricter(schema.GetConstraintContractsForTarget(ConstraintEntityNode, "Person")[0]))
		require.Equal(t, "n.age >= 0", schema.GetAllConstraintContracts()[0].Entries[0].Expression, "a copy's edit never reaches the stored revision")
	})
	t.Run("snapshot edit and replace", func(t *testing.T) {
		definition := schema.ExportDefinition()
		definition.ConstraintContracts[0] = stricter(definition.ConstraintContracts[0])
		next := NewSchemaManager()
		require.NoError(t, next.ReplaceFromDefinition(definition))
		stored := next.GetAllConstraintContracts()[0]
		require.Equal(t, "n.age >= 10", stored.Entries[0].Expression)
		violates(t, stored)
		revision := next.constraintContractRevisionsForTarget(ConstraintEntityNode, "Person")[0]
		node, err := engine.GetNode("test:p")
		require.NoError(t, err)
		require.ErrorContains(t, validateConstraintContractForNodeEngine(engine, revision, node), "n.age >= 10", "the replaced schema checks writes with the replaced expression")
	})
	t.Run("persisted reload", func(t *testing.T) {
		definition := schema.ExportDefinition()
		definition.ConstraintContracts[0] = stricter(definition.ConstraintContracts[0])
		encoded, err := json.Marshal(definition)
		require.NoError(t, err)
		reloaded := &SchemaDefinition{}
		require.NoError(t, json.Unmarshal(encoded, reloaded))
		next := NewSchemaManager()
		require.NoError(t, next.ReplaceFromDefinition(reloaded))
		violates(t, next.GetAllConstraintContracts()[0])
	})
	t.Run("changed kind", func(t *testing.T) {
		contract := schema.GetAllConstraintContracts()[0]
		contract.TargetEntityType, contract.TargetLabelOrType = string(ConstraintEntityRelationship), "KNOWS"
		contract.Entries[0] = ConstraintContractEntry{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.w >= 10"}
		require.ErrorContains(t, ValidateConstraintContractOnCreationForEngine(engine, contract), "r.w >= 10")
	})
	t.Run("previously invalid predicate", func(t *testing.T) {
		broken := ConstraintContract{Name: "broken_rule", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person",
			Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "unsupported()"}}}
		require.NoError(t, schema.AddConstraintContractBundle(broken, nil, nil, false))
		var stored ConstraintContract
		for _, contract := range schema.GetAllConstraintContracts() {
			if contract.Name == broken.Name {
				stored = contract
			}
		}
		require.Error(t, ValidateConstraintContractOnCreationForEngine(engine, stored), "the stored description is still invalid")
		violates(t, stricter(stored))
	})
	t.Run("compiled input edited afterwards", func(t *testing.T) {
		input := ConstraintContract{Name: "late_edit", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person",
			Entries: []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.age >= 0"}}}
		compiled, err := CompileConstraintContract(input)
		require.NoError(t, err)
		input.Entries[0].Expression = "unsupported()"
		require.Equal(t, "n.age >= 0", compiled.Contract().Entries[0].Expression, "a revision keeps its own copy")
		require.NoError(t, ValidateCompiledConstraintContractOnCreationForEngine(engine, compiled))
		_, err = CompileConstraintContract(input)
		require.Error(t, err, "compiling the edited description reports its current predicate")
		copied := compiled.Contract()
		copied.Entries[0].Expression = "n.age >= 10"
		require.NoError(t, ValidateCompiledConstraintContractOnCreationForEngine(engine, compiled), "editing a returned copy cannot change the revision")
		violates(t, copied)
	})
}
