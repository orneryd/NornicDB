package cypher

import "testing"

// The pipeline executor must ACCEPT the query shape that executeCallTail
// wraps around a CALL-YIELD-CREATE tail:
//
//	MATCH (node) WHERE id(node) = $seed_id_node
//	WITH node
//	CREATE (m:TailProbe {name: node.originalText})
//	RETURN m.name AS probeName
//
// The pipeline must correctly resolve the $param, bind `node` from MATCH,
// carry it through WITH, and run CREATE + RETURN against it. This test pins
// the splitter's behaviour at the shape-recognition layer; executePipeline
// integration is covered by the real Call-YIELD-CREATE tests.
func TestPipelineAcceptsCallTailCreateWrapper_WithBinding(t *testing.T) {
	q := `MATCH (node) WHERE id(node) = $seed_id_node WITH node CREATE (m:TailProbe {name: node.originalText}) RETURN m.name AS probeName`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("pipeline splitter must accept the MATCH+WHERE+WITH+CREATE+RETURN shape used by executeCallTail")
	}
}

// A scalar YIELD'd by CALL (e.g. score) is projected through WITH as
// `$seed_score AS score`. After $param substitution this becomes
// `WITH node, 0.95 AS score` which the pipeline must accept and propagate.
func TestPipelineAcceptsCallTailCreateWrapper_WithScalarProjection(t *testing.T) {
	q := `MATCH (node) WHERE id(node) = $seed_id_node WITH node, $seed_score AS score CREATE (m:TailProbe {name: node.originalText, score: score}) RETURN m.name AS probeName`
	_, ok := canExecuteAsPipeline(q)
	if !ok {
		t.Fatalf("pipeline splitter must accept the scalar-projection variant of the CALL-tail wrapper")
	}
}
