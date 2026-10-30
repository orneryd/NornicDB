package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ConstraintContractsCountPendingEdges(t *testing.T) {
	engine := newTestEngine(t)
	contract := ConstraintContract{
		Name:              "person_must_own_item",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Definition:        "CREATE CONSTRAINT person_must_own_item FOR (n:Person) REQUIRE COUNT { (n)-[:OWNS]->(:Item) } >= 1",
		Entries: []ConstraintContractEntry{{
			Kind:       ConstraintContractKindBooleanNode,
			Expression: "COUNT { (n)-[:OWNS]->(:Item) } >= 1",
		}},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:alice", Labels: []string{"Person"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:item", Labels: []string{"Item"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:owns", StartNode: "test:alice", EndNode: "test:item", Type: "OWNS", Properties: map[string]any{}}))
	require.NoError(t, tx.Commit())

	created, err := engine.GetNode("test:alice")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:alice"), created.ID)
}

func TestBadgerTransaction_ConstraintContractsRejectPendingRelationship(t *testing.T) {
	engine := newTestEngine(t)
	contract := ConstraintContract{
		Name:              "same_team_rel",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "WORKS_WITH",
		Definition:        "CREATE CONSTRAINT same_team_rel FOR ()-[r:WORKS_WITH]-() REQUIRE startNode(r).team = endNode(r).team",
		Entries: []ConstraintContractEntry{{
			Kind:       ConstraintContractKindBooleanRelationship,
			Expression: "startNode(r).team = endNode(r).team",
		}},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:left", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:right", Labels: []string{"Person"}, Properties: map[string]any{"team": "platform"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:works-with", StartNode: "test:left", EndNode: "test:right", Type: "WORKS_WITH", Properties: map[string]any{}}))

	err = tx.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract same_team_rel violated")
	require.Contains(t, err.Error(), "startNode(r).team = endNode(r).team")
	_, err = engine.GetEdge("test:works-with")
	require.ErrorIs(t, err, ErrNotFound)
}
