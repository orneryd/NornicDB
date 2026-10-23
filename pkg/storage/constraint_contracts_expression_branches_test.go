package storage

import (
	"errors"
	"strings"
	"testing"
)

type constraintExprEngine struct {
	Engine
	outgoingFn func(NodeID) ([]*Edge, error)
	incomingFn func(NodeID) ([]*Edge, error)
	getNodeFn  func(NodeID) (*Node, error)
}

func (e *constraintExprEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.outgoingFn != nil {
		return e.outgoingFn(nodeID)
	}
	return e.Engine.GetOutgoingEdges(nodeID)
}

func (e *constraintExprEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.incomingFn != nil {
		return e.incomingFn(nodeID)
	}
	return e.Engine.GetIncomingEdges(nodeID)
}

func (e *constraintExprEngine) GetNode(id NodeID) (*Node, error) {
	if e.getNodeFn != nil {
		return e.getNodeFn(id)
	}
	return e.Engine.GetNode(id)
}

func TestConstraintContracts_ExpressionEngineBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	_, _ = base.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "A", "age": int64(10)}})
	_, _ = base.CreateNode(&Node{ID: "test:b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "A", "age": int64(12)}})
	_ = base.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: map[string]interface{}{"weight": int64(3), "kind": "friend"}})

	t.Run("node expression parser and unsupported branches", func(t *testing.T) {
		eng := &constraintExprEngine{Engine: base}
		node := &Node{ID: "test:a", Labels: []string{"Person"}, Properties: map[string]interface{}{"tier": "gold"}}

		ok, err := evaluateNodeConstraintContractExpressionEngine(eng, node, "n.tier IN ['gold','silver']")
		if err != nil || !ok {
			t.Fatalf("expected IN expression true, ok=%v err=%v", ok, err)
		}

		ok, err = evaluateNodeConstraintContractExpressionEngine(eng, node, "COUNT { (n)-[:KNOWS]->(:Person) } >= 1")
		if err != nil || !ok {
			t.Fatalf("expected COUNT expression true, ok=%v err=%v", ok, err)
		}

		ok, err = evaluateNodeConstraintContractExpressionEngine(eng, node, "NOT EXISTS { (n)-[:BLOCKED]->() }")
		if err != nil || !ok {
			t.Fatalf("expected NOT EXISTS expression true, ok=%v err=%v", ok, err)
		}

		_, err = evaluateNodeConstraintContractExpressionEngine(eng, node, "totally unsupported predicate")
		if err == nil || !strings.Contains(err.Error(), "unsupported node predicate") {
			t.Fatalf("expected unsupported-node-predicate error, got: %v", err)
		}
	})

	t.Run("node count expression propagates edge lookup errors", func(t *testing.T) {
		eng := &constraintExprEngine{
			Engine: base,
			outgoingFn: func(NodeID) ([]*Edge, error) {
				return nil, errors.New("outgoing failed")
			},
		}
		node := &Node{ID: "test:a", Labels: []string{"Person"}}

		_, err := evaluateNodeConstraintContractExpressionEngine(eng, node, "COUNT { (n)-[:KNOWS]->() } >= 1")
		if err == nil || !strings.Contains(err.Error(), "outgoing failed") {
			t.Fatalf("expected outgoing lookup error, got: %v", err)
		}
	})

	t.Run("relationship expression branches", func(t *testing.T) {
		eng := &constraintExprEngine{Engine: base}
		edge := &Edge{
			ID:         "test:e1",
			StartNode:  "test:a",
			EndNode:    "test:b",
			Type:       "KNOWS",
			Properties: map[string]interface{}{"weight": int64(3), "kind": "friend"},
		}

		ok, err := evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "r.kind IN ['friend','coworker']")
		if err != nil || !ok {
			t.Fatalf("expected relationship IN expression true, ok=%v err=%v", ok, err)
		}

		ok, err = evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "startNode(r) <> endNode(r)")
		if err != nil || !ok {
			t.Fatalf("expected distinct-endpoints expression true, ok=%v err=%v", ok, err)
		}

		ok, err = evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "startNode(r).age = endNode(r).age")
		if err != nil || ok {
			t.Fatalf("expected endpoint equality false, ok=%v err=%v", ok, err)
		}

		ok, err = evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "r.weight >= 3")
		if err != nil || !ok {
			t.Fatalf("expected relationship comparator expression true, ok=%v err=%v", ok, err)
		}

		_, err = evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "r.weight >= endNode.name")
		if err == nil || !strings.Contains(err.Error(), "unsupported literal") {
			t.Fatalf("expected parse error for malformed relationship comparator")
		}

		_, err = evaluateRelationshipConstraintContractExpressionEngine(eng, edge, "unsupported relationship predicate")
		if err == nil || !strings.Contains(err.Error(), "unsupported relationship predicate") {
			t.Fatalf("expected unsupported-relationship-predicate error, got: %v", err)
		}
	})

	t.Run("relationship endpoint lookup missing and error branches", func(t *testing.T) {
		engMissing := &constraintExprEngine{
			Engine: base,
			getNodeFn: func(NodeID) (*Node, error) {
				return nil, nil
			},
		}
		edge := &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS", Properties: map[string]interface{}{}}
		_, err := evaluateRelationshipConstraintContractExpressionEngine(engMissing, edge, "startNode(r).name = endNode(r).name")
		if err == nil || !strings.Contains(err.Error(), "missing relationship endpoint") {
			t.Fatalf("expected missing-endpoint error, got: %v", err)
		}

		engErr := &constraintExprEngine{
			Engine: base,
			getNodeFn: func(NodeID) (*Node, error) {
				return nil, errors.New("node lookup failed")
			},
		}
		_, err = evaluateRelationshipConstraintContractExpressionEngine(engErr, edge, "startNode(r).name = endNode(r).name")
		if err == nil || !strings.Contains(err.Error(), "node lookup failed") {
			t.Fatalf("expected node lookup error, got: %v", err)
		}
	})
}
