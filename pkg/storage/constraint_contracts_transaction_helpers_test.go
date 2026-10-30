package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConstraintContracts_CurrentStateHelpers(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: map[string]any{"kind": "friend"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	tx.pendingNodes["test:p"] = &Node{ID: "test:p", Labels: []string{"Person"}, Properties: map[string]any{"team": "pending"}}
	tx.pendingEdges["test:ep"] = &Edge{ID: "test:ep", StartNode: "test:a", EndNode: "test:p", Type: "KNOWS", Properties: map[string]any{}}
	tx.deletedNodes["test:missing"] = struct{}{}
	tx.deletedEdges["test:e_deleted"] = struct{}{}

	n, err := tx.currentNodeLocked("test:p")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:p"), n.ID)

	n, err = tx.currentNodeLocked("test:missing")
	require.NoError(t, err)
	require.Nil(t, n)

	e, err := tx.currentEdgeLocked("test:ep")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:ep"), e.ID)

	e, err = tx.currentEdgeLocked("test:e_deleted")
	require.NoError(t, err)
	require.Nil(t, e)

	out, err := tx.currentOutgoingEdgesLocked("test:a")
	require.NoError(t, err)
	require.NotEmpty(t, out)

	in, err := tx.currentIncomingEdgesLocked("test:b")
	require.NoError(t, err)
	require.NotEmpty(t, in)

	adj, err := tx.currentAdjacentEdgesLocked("test:a")
	require.NoError(t, err)
	require.NotEmpty(t, adj)
}

func TestConstraintContracts_EvaluateExpressions_Locked(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:left", Labels: []string{"Person", "Team"}, Properties: map[string]any{"team": "core", "name": "L"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:right", Labels: []string{"Person", "Team"}, Properties: map[string]any{"team": "core", "name": "R"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "test:works", StartNode: "test:left", EndNode: "test:right", Type: "WORKS_WITH", Properties: map[string]any{"kind": "peer", "weight": 2.0}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	node, err := tx.currentNodeLocked("test:left")
	require.NoError(t, err)
	edge, err := tx.currentEdgeLocked("test:works")
	require.NoError(t, err)

	ok, err := tx.evaluateNodeConstraintContractExpressionLocked(node, "n.team IN ['core', 'ops']")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateNodeConstraintContractExpressionLocked(node, "COUNT { (n)-[:WORKS_WITH]->(:Team) } >= 1")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateNodeConstraintContractExpressionLocked(node, "NOT EXISTS { (n)-[:MISSING]->(:Team) }")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateNodeConstraintContractExpressionLocked(node, "not-a-supported-predicate")
	require.Error(t, err)
	require.False(t, ok)

	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "r.kind IN ['peer', 'lead']")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "startNode(r) <> endNode(r)")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "startNode(r).team = endNode(r).team")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "r.weight >= 1")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(edge, "unsupported relationship expr")
	require.Error(t, err)
	require.False(t, ok)

	badEdge := &Edge{ID: "test:bad", StartNode: "test:missing", EndNode: "test:right", Type: "WORKS_WITH"}
	ok, err = tx.evaluateRelationshipConstraintContractExpressionLocked(badEdge, "startNode(r).team = endNode(r).team")
	require.Error(t, err)
	require.False(t, ok)
}

func TestConstraintContracts_ValidateLocked_SuccessAndViolation(t *testing.T) {
	engine := newTestEngine(t)
	sm := engine.GetSchemaForNamespace("test")

	nodeContract := ConstraintContract{
		Name:              "person_team_allowed",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Definition:        "CREATE CONSTRAINT person_team_allowed FOR (n:Person) REQUIRE n.team IN ['core']",
		Entries: []ConstraintContractEntry{{
			Kind:       ConstraintContractKindBooleanNode,
			Expression: "n.team IN ['core']",
		}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(nodeContract, nil, nil, false))

	relContract := ConstraintContract{
		Name:              "same_team_rel",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "WORKS_WITH",
		Definition:        "CREATE CONSTRAINT same_team_rel FOR ()-[r:WORKS_WITH]-() REQUIRE startNode(r).team = endNode(r).team",
		Entries: []ConstraintContractEntry{{
			Kind:       ConstraintContractKindBooleanRelationship,
			Expression: "startNode(r).team = endNode(r).team",
		}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(relContract, nil, nil, false))

	_, err := engine.CreateNode(&Node{ID: "test:p1", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:p2", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "test:r1", StartNode: "test:p1", EndNode: "test:p2", Type: "WORKS_WITH", Properties: map[string]any{}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	node, err := tx.currentNodeLocked("test:p1")
	require.NoError(t, err)
	require.NoError(t, tx.validateConstraintContractsForNodeLocked(node))

	edge, err := tx.currentEdgeLocked("test:r1")
	require.NoError(t, err)
	require.NoError(t, tx.validateConstraintContractsForEdgeLocked(edge))

	node.Properties["team"] = "other"
	err = tx.validateConstraintContractsForNodeLocked(node)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract person_team_allowed violated")

	edge.StartNode = "test:p1"
	edge.EndNode = "test:p2"
	// Force violation by changing endpoint team through pending node shadow.
	tx.pendingNodes["test:p2"] = &Node{ID: "test:p2", Labels: []string{"Person"}, Properties: map[string]any{"team": "other"}}
	err = tx.validateConstraintContractsForEdgeLocked(edge)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract same_team_rel violated")
}

func TestConstraintContractNamespaceHelpers(t *testing.T) {
	ns, ok := constraintContractNamespaceForNode(&Node{ID: "demo:n1"})
	require.True(t, ok)
	require.Equal(t, "demo", ns)

	ns, ok = constraintContractNamespaceForEdge(&Edge{ID: "demo:e1"})
	require.True(t, ok)
	require.Equal(t, "demo", ns)

	ns, ok = constraintContractNamespaceForEdge(&Edge{ID: "e1", StartNode: "demo:s", EndNode: "x"})
	require.True(t, ok)
	require.Equal(t, "demo", ns)

	_, ok = constraintContractNamespaceForNode(nil)
	require.False(t, ok)
	_, ok = constraintContractNamespaceForEdge(&Edge{ID: "e1", StartNode: "s", EndNode: "t"})
	require.False(t, ok)
}
