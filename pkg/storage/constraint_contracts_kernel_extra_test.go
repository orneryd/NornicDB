package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/stretchr/testify/require"
)

// TestConstraintContracts_ValidateKernel_DeterministicOrderAndParity pins the
// shared validation kernel behind validateConstraintContractsForNodeLocked and
// validateConstraintContractsForEdgeLocked: contracts are collected in target
// order (label order for nodes, the single relationship type for edges) with
// the schema's name-sorted order per target, so the first violated contract is
// deterministic on both entity paths, and the invalid/violated error wrapping
// is identical.
func TestConstraintContracts_ValidateKernel_DeterministicOrderAndParity(t *testing.T) {
	engine := newTestEngine(t)
	sm := engine.GetSchemaForNamespace("test")

	nodeAlpha := ConstraintContract{
		Name:              "alpha_rule",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(nodeAlpha, nil, nil, false))
	nodeBeta := ConstraintContract{
		Name:              "beta_rule",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Employee",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(nodeBeta, nil, nil, false))

	edgeAlpha := ConstraintContract{
		Name:              "edge_alpha",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "WORKS_WITH",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.kind IN ['peer']"}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(edgeAlpha, nil, nil, false))
	edgeBeta := ConstraintContract{
		Name:              "edge_beta",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "WORKS_WITH",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.kind IN ['peer']"}},
	}
	require.NoError(t, sm.AddConstraintContractBundle(edgeBeta, nil, nil, false))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	node := &Node{ID: "test:p1", Labels: []string{"Person", "Employee"}, Properties: map[string]any{"team": "other"}}
	edge := &Edge{ID: "test:r1", StartNode: "test:p1", EndNode: "test:p2", Type: "WORKS_WITH", Properties: map[string]any{"kind": "boss"}}

	// Node path: target order follows label order, so alpha (Person) is the
	// first violated contract.
	err = tx.validateConstraintContractsForNodeLocked(node)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract alpha_rule violated")
	require.NotContains(t, err.Error(), "beta_rule")

	// Edge path: a single target in the schema's name order, so edge_alpha is
	// reported first — identical wrapping through the shared kernel.
	err = tx.validateConstraintContractsForEdgeLocked(edge)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract edge_alpha violated")
	require.NotContains(t, err.Error(), "edge_beta")

	// Reversing the label order flips the reported contract: the same kernel,
	// the same deterministic rule.
	node.Labels = []string{"Employee", "Person"}
	err = tx.validateConstraintContractsForNodeLocked(node)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract beta_rule violated")
	require.NotContains(t, err.Error(), "alpha_rule")
}

// BenchmarkConstraintContracts_ValidateLocked_Node pins the shared validation
// kernel cost (schema resolution + contract collection + evaluator) after the
// node/edge validator convergence.
func BenchmarkConstraintContracts_ValidateLocked_Node(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	sm := engine.GetSchemaForNamespace("test")
	contract := ConstraintContract{
		Name:              "team_allowed",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}
	if err := sm.AddConstraintContractBundle(contract, nil, nil, false); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: "test:p1", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}}); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.SetNamespace("test"); err != nil {
		b.Fatal(err)
	}
	node, err := tx.currentNodeLocked("test:p1")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tx.validateConstraintContractsForNodeLocked(node); err != nil {
			b.Fatal(err)
		}
	}
}
