package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type ccErrScanNodesEngine struct{ Engine }

func (e *ccErrScanNodesEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return nil, errors.New("scan nodes failed")
}

type ccErrScanEdgesEngine struct{ Engine }

func (e *ccErrScanEdgesEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return nil, errors.New("scan edges failed")
}

func TestValidateConstraintContractOnCreationForEngine_Branches(t *testing.T) {
	eng := newTestMemoryEngine(t)
	_, err := eng.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "KNOWS", Properties: map[string]any{"kind": "peer"}})
	require.NoError(t, err)

	nodeContract := ConstraintContract{
		Name:              "node_rule",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(eng, nodeContract))

	edgeContract := ConstraintContract{
		Name:              "edge_rule",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "KNOWS",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: "startNode(r) <> endNode(r)"}},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(eng, edgeContract))

	violatingNode := ConstraintContract{
		Name:              "node_fail",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['ops']"}},
	}
	err = ValidateConstraintContractOnCreationForEngine(eng, violatingNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "violated")

	unsupported := ConstraintContract{Name: "bad", TargetEntityType: "INVALID", TargetLabelOrType: "X"}
	err = ValidateConstraintContractOnCreationForEngine(eng, unsupported)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported constraint contract target entity type")

	err = ValidateConstraintContractOnCreationForEngine(&ccErrScanNodesEngine{Engine: eng}, nodeContract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning nodes")

	err = ValidateConstraintContractOnCreationForEngine(&ccErrScanEdgesEngine{Engine: eng}, edgeContract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning relationships")
}

func TestAddConstraintContractBundle_Branches(t *testing.T) {
	sm := NewSchemaManager()
	persistCalls := 0
	sm.persist = func(def *SchemaDefinition) error {
		persistCalls++
		if persistCalls == 2 {
			return errors.New("persist failed")
		}
		return nil
	}

	base := ConstraintContract{Name: "c1", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}
	require.NoError(t, sm.AddConstraintContractBundle(base, nil, nil, false))

	// ifNotExists idempotent branch.
	require.NoError(t, sm.AddConstraintContractBundle(base, nil, nil, true))

	// duplicate error branch.
	err := sm.AddConstraintContractBundle(base, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")

	// conflict with existing constraint name branch.
	sm.constraints["c_conflict"] = Constraint{Name: "c_conflict", Type: ConstraintUnique, Label: "Person", Properties: []string{"name"}}
	err = sm.AddConstraintContractBundle(ConstraintContract{Name: "c_conflict", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with an existing constraint name")

	// persist failure rolls back insertion.
	contractPersistFail := ConstraintContract{Name: "c2", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}
	err = sm.AddConstraintContractBundle(contractPersistFail, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist failed")
	_, exists := sm.constraintContracts["c2"]
	require.False(t, exists)

	// compiledConstraints path: addConstraintLocked failure should rollback.
	err = sm.AddConstraintContractBundle(
		ConstraintContract{Name: "c3", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"},
		[]Constraint{
			{Name: "dup_schema", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}},
			{Name: "dup_schema", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}},
		},
		nil,
		false,
	)
	require.Error(t, err)
	_, exists = sm.constraintContracts["c3"]
	require.False(t, exists)

	// compiledTypes path: addPropertyTypeConstraintValueLocked failure should rollback.
	err = sm.AddConstraintContractBundle(
		ConstraintContract{Name: "c4", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"},
		nil,
		[]PropertyTypeConstraint{
			{Name: "ptype_dup", Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger},
			{Name: "ptype_dup", Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger},
		},
		false,
	)
	require.Error(t, err)
	_, exists = sm.constraintContracts["c4"]
	require.False(t, exists)
}

func TestConstraintContractExpressionComparisonParseErrors_Propagate(t *testing.T) {
	t.Run("engine evaluator malformed literal", func(t *testing.T) {
		eng := newTestMemoryEngine(t)
		node := &Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"age": 10}}
		_, err := evaluateNodeConstraintContractExpressionEngine(eng, node, "n.age >= [not_a_literal]")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported literal")
	})

	t.Run("transaction evaluator malformed literal", func(t *testing.T) {
		engine := newTestEngine(t)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		node := &Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"age": 10}}
		ok, evalErr := tx.evaluateNodeConstraintContractExpressionLocked(node, "n.age >= [not_a_literal]")
		require.False(t, ok)
		require.Error(t, evalErr)
		require.Contains(t, evalErr.Error(), "unsupported literal")
	})
}

func TestNodeConstraintComparisonExpressions_EvaluatedInEngineAndTransaction(t *testing.T) {
	t.Run("engine evaluator comparison", func(t *testing.T) {
		eng := newTestMemoryEngine(t)
		node := &Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(10)}}

		ok, err := evaluateNodeConstraintContractExpressionEngine(eng, node, "n.age >= 10")
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("transaction evaluator comparison", func(t *testing.T) {
		engine := newTestEngine(t)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })

		node := &Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(10)}}
		ok, evalErr := tx.evaluateNodeConstraintContractExpressionLocked(node, "n.age >= 10")
		require.NoError(t, evalErr)
		require.True(t, ok)
	})
}

// TestConstraintContractEvaluation_EngineAndTransactionViewsAgree pins the
// shared constraintGraphView kernel: on committed data the Engine view and
// the locked-transaction view must return identical results and errors for
// every expression shape.
func TestConstraintContractEvaluation_EngineAndTransactionViewsAgree(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:alice", Labels: []string{"Person"}, Properties: map[string]any{"state": "active", "age": int64(30), "team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:bob", Labels: []string{"Person"}, Properties: map[string]any{"state": "banned", "team": "platform"}})
	require.NoError(t, err)
	for i, ep := range []string{"test:i1", "test:i2", "test:i3"} {
		_, err = engine.CreateNode(&Node{ID: NodeID(ep), Labels: []string{"Item"}, Properties: map[string]any{}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{
			ID: EdgeID("test:o-" + string(rune('1'+i))), StartNode: "test:alice",
			EndNode: NodeID(ep), Type: "OWNS", Properties: map[string]any{},
		}))
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:k", StartNode: "test:alice", EndNode: "test:bob", Type: "KNOWS", Properties: map[string]any{"v": "active", "score": int64(75)}}))

	alice, err := engine.GetNode("test:alice")
	require.NoError(t, err)
	bob, err := engine.GetNode("test:bob")
	require.NoError(t, err)
	edge, err := engine.GetEdge("test:k")
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	type outcome struct {
		ok      bool
		errText string
	}
	evalNode := func(view func() (bool, error)) outcome {
		ok, err := view()
		text := ""
		if err != nil {
			text = err.Error()
		}
		return outcome{ok: ok, errText: text}
	}
	engineNode := func(node *Node, expr string) outcome {
		return evalNode(func() (bool, error) {
			return evaluateNodeConstraintContractExpressionEngine(engine, node, expr)
		})
	}
	txNode := func(node *Node, expr string) outcome {
		return evalNode(func() (bool, error) {
			return tx.evaluateNodeConstraintContractExpressionLocked(node, expr)
		})
	}
	engineEdge := func(expr string) outcome {
		return evalNode(func() (bool, error) {
			return evaluateRelationshipConstraintContractExpressionEngine(engine, edge, expr)
		})
	}
	txEdge := func(expr string) outcome {
		return evalNode(func() (bool, error) {
			return tx.evaluateRelationshipConstraintContractExpressionLocked(edge, expr)
		})
	}

	for _, expr := range []string{
		"n.state IN ['active', 'pending']",
		"n.age >= 18",
		"n.team = 'core'",
		"COUNT { (n)-[:OWNS]->(:Item) } <= 5",
		"COUNT { (n)-[:OWNS]->(:Item) } < 3",
		"NOT EXISTS { (n)-[:OWNS]->() }",
		"NOT EXISTS { (n)-[:KNOWS]->() }",
		"something_weird()",
	} {
		require.Equal(t, engineNode(alice, expr), txNode(alice, expr), "node expr %q", expr)
		require.Equal(t, engineNode(bob, expr), txNode(bob, expr), "node expr %q on bob", expr)
	}

	for _, expr := range []string{
		"r.v IN ['active', 'pending']",
		"startNode(r) <> endNode(r)",
		"startNode(r).team = endNode(r).team",
		"r.score > 50",
		"r.score < 50",
		"something_weird()",
	} {
		require.Equal(t, engineEdge(expr), txEdge(expr), "edge expr %q", expr)
	}
}

// BenchmarkConstraintContractEvaluation_EngineView pins the converged
// evaluator cost (shared constraintGraphView kernel, count-pattern shape).
func BenchmarkConstraintContractEvaluation_EngineView(b *testing.B) {
	engine := runtimeFixtureOwner.Track(b, NewMemoryEngine(), ErrStorageClosed)
	defer engine.Close()
	if _, err := engine.CreateNode(&Node{ID: "test:alice", Labels: []string{"Person"}, Properties: map[string]any{}}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := engine.CreateNode(&Node{ID: NodeID("test:item-" + string(rune('1'+i))), Labels: []string{"Item"}, Properties: map[string]any{}}); err != nil {
			b.Fatal(err)
		}
		if err := engine.CreateEdge(&Edge{ID: EdgeID("test:o-" + string(rune('1'+i))), StartNode: "test:alice", EndNode: NodeID("test:item-" + string(rune('1'+i))), Type: "OWNS"}); err != nil {
			b.Fatal(err)
		}
	}
	alice, err := engine.GetNode("test:alice")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := evaluateNodeConstraintContractExpressionEngine(engine, alice, "COUNT { (n)-[:OWNS]->(:Item) } <= 5")
		if err != nil || !ok {
			b.Fatalf("eval failed: %v ok=%v", err, ok)
		}
	}
}
