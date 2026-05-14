package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// constraintContractsEngineFixture creates an engine, registers the
// supplied constraint contract under namespace "test", and pre-creates
// node/edge fixtures the test can either accept or fail validation
// against. Returns the engine; tear-down is handled by t.Cleanup.
func constraintContractsEngineFixture(t *testing.T) *MemoryEngine {
	t.Helper()
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestHasAllLabels(t *testing.T) {
	require.True(t, hasAllLabels([]string{"A", "B", "C"}, []string{"A", "B"}))
	require.True(t, hasAllLabels([]string{"A"}, nil), "empty required → trivially satisfied")
	require.False(t, hasAllLabels([]string{"A"}, []string{"A", "B"}))
	require.False(t, hasAllLabels(nil, []string{"A"}))
}

func TestEvaluateNodeConstraintContractExpressionEngine_PropertyIn_True(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	node := &Node{
		ID:         "test:n1",
		Labels:     []string{"Status"},
		Properties: map[string]any{"state": "active"},
	}
	got, err := evaluateNodeConstraintContractExpressionEngine(engine, node, "n.state IN ['active', 'pending']")
	require.NoError(t, err)
	require.True(t, got)
}

func TestEvaluateNodeConstraintContractExpressionEngine_PropertyIn_False(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	node := &Node{
		ID:         "test:n1",
		Labels:     []string{"Status"},
		Properties: map[string]any{"state": "deleted"},
	}
	got, err := evaluateNodeConstraintContractExpressionEngine(engine, node, "n.state IN ['active', 'pending']")
	require.NoError(t, err)
	require.False(t, got)
}

func TestEvaluateNodeConstraintContractExpressionEngine_CountPattern(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	// Set up alice with 3 outgoing OWNS edges.
	_, err := engine.CreateNode(&Node{
		ID: "test:alice", Labels: []string{"Person"},
		Properties: map[string]any{},
	})
	require.NoError(t, err)
	for i, ep := range []string{"test:b", "test:c", "test:d"} {
		_, err := engine.CreateNode(&Node{ID: NodeID(ep), Labels: []string{"Item"}, Properties: map[string]any{}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        EdgeID("test:e-" + string(rune('1'+i))),
			StartNode: "test:alice", EndNode: NodeID(ep), Type: "OWNS",
			Properties: map[string]any{},
		}))
	}

	alice, err := engine.GetNode("test:alice")
	require.NoError(t, err)

	// COUNT { (n)-[:OWNS]->(:Item) } <= 5 → true.
	got, err := evaluateNodeConstraintContractExpressionEngine(engine, alice, "COUNT { (n)-[:OWNS]->(:Item) } <= 5")
	require.NoError(t, err)
	require.True(t, got)

	// COUNT { (n)-[:OWNS]->(:Item) } < 3 → false (alice has exactly 3).
	got, err = evaluateNodeConstraintContractExpressionEngine(engine, alice, "COUNT { (n)-[:OWNS]->(:Item) } < 3")
	require.NoError(t, err)
	require.False(t, got)
}

func TestEvaluateNodeConstraintContractExpressionEngine_NotExistsPattern(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{
		ID: "test:lonely", Labels: []string{"Person"},
		Properties: map[string]any{},
	})
	require.NoError(t, err)
	lonely, err := engine.GetNode("test:lonely")
	require.NoError(t, err)

	// No outgoing OWNS edges → NOT EXISTS evaluates true.
	got, err := evaluateNodeConstraintContractExpressionEngine(engine, lonely, "NOT EXISTS { (n)-[:OWNS]->() }")
	require.NoError(t, err)
	require.True(t, got)
}

func TestEvaluateNodeConstraintContractExpressionEngine_UnsupportedExpression(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	node := &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{}}
	_, err := evaluateNodeConstraintContractExpressionEngine(engine, node, "something_weird()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

func TestEvaluateRelationshipConstraintContractExpressionEngine_PropertyIn(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	edge := &Edge{
		ID:        "test:e1",
		StartNode: "test:a", EndNode: "test:b",
		Type:       "STATUS",
		Properties: map[string]any{"v": "active"},
	}
	got, err := evaluateRelationshipConstraintContractExpressionEngine(engine, edge, "r.v IN ['active', 'pending']")
	require.NoError(t, err)
	require.True(t, got)
}

func TestEvaluateRelationshipConstraintContractExpressionEngine_DistinctEndpoints(t *testing.T) {
	engine := constraintContractsEngineFixture(t)

	// Self-loop edge.
	loop := &Edge{ID: "test:loop", StartNode: "test:n1", EndNode: "test:n1", Type: "REL"}
	got, err := evaluateRelationshipConstraintContractExpressionEngine(engine, loop, "startNode(r) <> endNode(r)")
	require.NoError(t, err)
	require.False(t, got, "self-loop must violate startNode <> endNode")

	// Non-loop.
	other := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "REL"}
	got, err = evaluateRelationshipConstraintContractExpressionEngine(engine, other, "startNode(r) <> endNode(r)")
	require.NoError(t, err)
	require.True(t, got)
}

func TestEvaluateRelationshipConstraintContractExpressionEngine_EndpointPropertyEquality(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"L"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"L"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:c", Labels: []string{"L"}, Properties: map[string]any{"team": "platform"}})
	require.NoError(t, err)

	matchEdge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "REL"}
	got, err := evaluateRelationshipConstraintContractExpressionEngine(engine, matchEdge, "startNode(r).team = endNode(r).team")
	require.NoError(t, err)
	require.True(t, got)

	mismatchEdge := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "REL"}
	got, err = evaluateRelationshipConstraintContractExpressionEngine(engine, mismatchEdge, "startNode(r).team = endNode(r).team")
	require.NoError(t, err)
	require.False(t, got)
}

func TestEvaluateRelationshipConstraintContractExpressionEngine_PropertyComparison(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	edge := &Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"score": int64(75)},
	}

	for _, c := range []struct {
		expr string
		want bool
	}{
		{"r.score > 50", true},
		{"r.score < 50", false},
		{"r.score >= 75", true},
		{"r.score <= 50", false},
		{"r.score = 75", true},
		{"r.score <> 75", false},
	} {
		got, err := evaluateRelationshipConstraintContractExpressionEngine(engine, edge, c.expr)
		require.NoError(t, err, "expr=%q", c.expr)
		require.Equal(t, c.want, got, "expr=%q", c.expr)
	}
}

func TestEvaluateRelationshipConstraintContractExpressionEngine_UnsupportedExpression(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "REL"}
	_, err := evaluateRelationshipConstraintContractExpressionEngine(engine, edge, "weird()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}

func TestIsDistinctEndpointsExpression_Variants(t *testing.T) {
	require.True(t, isDistinctEndpointsExpression("startNode(r) <> endNode(r)"))
	require.True(t, isDistinctEndpointsExpression("  startNode(  r  )  <>  endNode(  r  )  "))
	require.False(t, isDistinctEndpointsExpression("startNode(r) = endNode(r)"))
	require.False(t, isDistinctEndpointsExpression("notAStartNode(r) <> endNode(r)"))
	require.False(t, isDistinctEndpointsExpression(""))
}

func TestCountMatchingPatternEdgesEngine_Outgoing(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{ID: "test:src", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	for _, lbl := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&Node{ID: NodeID("test:" + lbl), Labels: []string{"Doc"}, Properties: map[string]any{}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        EdgeID("test:e-" + lbl),
			StartNode: "test:src", EndNode: NodeID("test:" + lbl),
			Type:       "READS",
			Properties: map[string]any{},
		}))
	}
	src, err := engine.GetNode("test:src")
	require.NoError(t, err)

	count, err := countMatchingPatternEdgesEngine(engine, src, contractPattern{
		Direction: "OUTGOING", RelationType: "READS", TargetLabels: []string{"Doc"},
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Wrong type → 0.
	count, err = countMatchingPatternEdgesEngine(engine, src, contractPattern{
		Direction: "OUTGOING", RelationType: "OWNS",
	})
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Target label mismatch → 0.
	count, err = countMatchingPatternEdgesEngine(engine, src, contractPattern{
		Direction: "OUTGOING", RelationType: "READS", TargetLabels: []string{"NoSuch"},
	})
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestCountMatchingPatternEdgesEngine_Incoming(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{ID: "test:tgt", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	for _, src := range []string{"a", "b"} {
		_, err := engine.CreateNode(&Node{ID: NodeID("test:" + src), Labels: []string{"Person"}, Properties: map[string]any{}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{
			ID:        EdgeID("test:e-" + src),
			StartNode: NodeID("test:" + src), EndNode: "test:tgt",
			Type:       "FOLLOWS",
			Properties: map[string]any{},
		}))
	}
	tgt, err := engine.GetNode("test:tgt")
	require.NoError(t, err)

	count, err := countMatchingPatternEdgesEngine(engine, tgt, contractPattern{
		Direction: "INCOMING", RelationType: "FOLLOWS", TargetLabels: []string{"Person"},
	})
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestValidateConstraintContractOnCreationForEngine_NodeContractPasses(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{
		ID: "test:n", Labels: []string{"Order"}, Properties: map[string]any{"state": "active"},
	})
	require.NoError(t, err)

	contract := ConstraintContract{
		Name:              "active_only",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Order",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanNode, Expression: "n.state IN ['active', 'pending']"},
		},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(engine, contract))
}

func TestValidateConstraintContractOnCreationForEngine_NodeContractViolates(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	_, err := engine.CreateNode(&Node{
		ID: "test:bad", Labels: []string{"Order"}, Properties: map[string]any{"state": "deleted"},
	})
	require.NoError(t, err)

	contract := ConstraintContract{
		Name:              "active_only",
		TargetEntityType:  "NODE",
		TargetLabelOrType: "Order",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanNode, Expression: "n.state IN ['active', 'pending']"},
		},
	}
	err = ValidateConstraintContractOnCreationForEngine(engine, contract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "active_only")
	require.Contains(t, err.Error(), "violated")
}

func TestValidateConstraintContractOnCreationForEngine_RelContractPasses(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	for _, n := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: n, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"v": int64(10)},
	}))

	contract := ConstraintContract{
		Name:              "score_min",
		TargetEntityType:  "RELATIONSHIP",
		TargetLabelOrType: "REL",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.v >= 5"},
		},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(engine, contract))
}

func TestValidateConstraintContractOnCreationForEngine_RelContractViolates(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	for _, n := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: n, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e", StartNode: "test:a", EndNode: "test:b",
		Type: "REL", Properties: map[string]any{"v": int64(1)},
	}))

	contract := ConstraintContract{
		Name:              "score_min",
		TargetEntityType:  "RELATIONSHIP",
		TargetLabelOrType: "REL",
		Entries: []ConstraintContractEntry{
			{Kind: ConstraintContractKindBooleanRelationship, Expression: "r.v >= 5"},
		},
	}
	err := ValidateConstraintContractOnCreationForEngine(engine, contract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "score_min")
}

func TestValidateConstraintContractOnCreationForEngine_UnsupportedTargetType(t *testing.T) {
	engine := constraintContractsEngineFixture(t)
	contract := ConstraintContract{
		Name:              "bogus",
		TargetEntityType:  "FOO",
		TargetLabelOrType: "X",
	}
	err := ValidateConstraintContractOnCreationForEngine(engine, contract)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "unsupported")
}
