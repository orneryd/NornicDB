package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ValidateConstraintContracts_EarlyOutAndPaths(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Team"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "MEMBER_OF", Properties: map[string]any{"kind": "primary"}}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))

	// No contracts yet => early-out branch.
	tx.pendingNodes["test:a"] = &Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}}
	require.NoError(t, tx.validateConstraintContracts())

	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraintContractBundle(ConstraintContract{
		Name:              "person_contract",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"},
		},
	}, nil, nil, false))
	require.NoError(t, sm.AddConstraintContractBundle(ConstraintContract{
		Name:              "edge_contract",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "MEMBER_OF",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanRelationship, Expression: "startNode(r).team = endNode(r).team"},
		},
	}, nil, nil, false))

	// Exercise affectedEdges + oldEdge path.
	tx.operations = append(tx.operations, Operation{Type: OpUpdateEdge, OldEdge: &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "MEMBER_OF"}})
	tx.pendingEdges["test:e2"] = &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "MEMBER_OF", Properties: map[string]any{"kind": "secondary"}}
	require.NoError(t, tx.validateConstraintContracts())
}

func TestBadgerTransaction_ValidateConstraintContracts_EngineClosedError(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))

	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraintContractBundle(ConstraintContract{
		Name:              "person_contract",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}, nil, nil, false))

	tx.pendingNodes["test:a"] = &Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}}
	require.NoError(t, engine.Close())
	err = tx.validateConstraintContracts()
	require.Error(t, err)
}
