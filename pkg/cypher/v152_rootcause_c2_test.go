package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// v152RCCSwapFunctionRight is the test-side corrected step: an equality whose
// function call is on the right is normalised to function-on-left before the
// unchanged production recognizer reads it.
func v152RCCSwapFunctionRight(clause string) string {
	eq := strings.Index(clause, "=")
	if eq <= 0 {
		return clause
	}
	left, right := strings.TrimSpace(clause[:eq]), strings.TrimSpace(clause[eq+1:])
	lower := strings.ToLower(right)
	if (strings.HasPrefix(lower, "id(") || strings.HasPrefix(lower, "elementid(")) && strings.HasSuffix(right, ")") {
		return right + " = " + left
	}
	return clause
}

// TestV152RCCRelationshipIDSeedOperandOrder: the relationship point-seed
// recognizer (match_index_seek.go:55-83, used by
// traversal_relationship_id_seed.go:34) accepts the id function only on the
// left of '='. The reversed operand order is refused at that step; the
// test-side normalisation alone makes the same production calls seed the edge.
func TestV152RCCRelationshipIDSeedOperandOrder(t *testing.T) {
	ctx := context.Background()
	store := v152PortRelationshipStore(t)
	v152PortRelationshipNode(t, store, "a", "Held", map[string]any{"id": "a"})
	v152PortRelationshipNode(t, store, "b", "Held", map[string]any{"id": "b"})
	for i := range 3 {
		v152PortRelationshipEdge(t, store, fmt.Sprintf("r%d", i), "a", "b", "R", map[string]any{"v": i})
	}
	exec := NewStorageExecutor(store)
	exec.cache = nil
	res := v152PortRelationshipRun(t, exec, "MATCH ()-[r:R {v:0}]->() RETURN elementId(r)", nil)
	require.Len(t, res.Rows, 1)
	element := fmt.Sprint(res.Rows[0][0])
	pattern := "(a)-[r:R]->(b)"
	forms := []struct {
		name, where string
		trigger      bool
	}{
		{"function_left_control", "elementId(r) = '" + element + "'", false},
		{"function_right_trigger", "'" + element + "' = elementId(r)", true},
		{"id_right_trigger", "'r0' = id(r)", true},
		{"id_left_control", "id(r) = 'r0'", false},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			variable, lookup, empty, ok := exec.idEqualityLookup(ctx, form.where, "5")
			edge, none, seeded := exec.relationshipIDSeedEdge(ctx, pattern, form.where)
			corrected := v152RCCSwapFunctionRight(form.where)
			cVariable, cLookup, _, cOK := exec.idEqualityLookup(ctx, corrected, "5")
			cEdge, _, cSeeded := exec.relationshipIDSeedEdge(ctx, pattern, corrected)
			edgeID := ""
			if edge != nil {
				edgeID = string(edge.ID)
			}
			cEdgeID := ""
			if cEdge != nil {
				cEdgeID = string(cEdge.ID)
			}
			t.Logf("RCC_RELID owner=pkg/cypher/match_index_seek.go:72-83 where=%q recognizer_ok=%v var=%q lookup=%q empty=%v seed_ok=%v seed_none=%v seed_edge=%q | corrected=%q recognizer_ok=%v var=%q lookup=%q seed_ok=%v seed_edge=%q",
				form.where, ok, variable, lookup, empty, seeded, none, edgeID, corrected, cOK, cVariable, cLookup, cSeeded, cEdgeID)
			require.True(t, cOK, "corrected copy must be recognised")
			require.True(t, cSeeded, "corrected copy must seed")
			require.Equal(t, "r0", strings.TrimPrefix(cEdgeID, "port:"), "corrected copy seeds the named relationship")
			assert.True(t, ok, "both commutative operand orders must be recognised by the seed recognizer")
			assert.True(t, seeded, "both commutative operand orders must seed the relationship")
		})
	}
}

// TestV152RCCChainedEndSeedGate: the end-seed path is closed for any chained
// pattern (traversal.go:452, :880, :900). The same bound end node seeds the
// one-hop pattern and is refused for the two-hop pattern at those gates.
func TestV152RCCChainedEndSeedGate(t *testing.T) {
	ctx := context.Background()
	store := v152PortRelationshipStore(t)
	for _, id := range []string{"a", "m", "b"} {
		v152PortRelationshipNode(t, store, id, "N", map[string]any{"id": id})
	}
	v152PortRelationshipEdge(t, store, "e1", "a", "m", "R", nil)
	v152PortRelationshipEdge(t, store, "e2", "m", "b", "R", nil)
	exec := NewStorageExecutor(store)
	exec.cache = nil
	end, err := store.GetNode(ctx, storage.NodeID("b"))
	require.NoError(t, err)
	mid, err := store.GetNode(ctx, storage.NodeID("m"))
	require.NoError(t, err)
	for _, shape := range []struct {
		name, pattern string
		endSeed       *storage.Node
		trigger       bool
	}{
		{"one_hop_control", "(x:N)-[:R]->(y:N)", end, false},
		{"one_hop_mid_control", "(x:N)-[:R]->(y:N)", mid, false},
		{"two_hop_trigger", "(x:N)-[:R]->(z:N)-[:R]->(y:N)", end, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			match := exec.parseTraversalPattern(ctx, shape.pattern)
			require.NotNil(t, match)
			reversed := reverseTraversalMatch(match)
			paths, seeded := exec.traverseFromEndSeeds(ctx, match, nil, []*storage.Node{shape.endSeed}, "")
			t.Logf("RCC_ENDSEED owner=pkg/cypher/traversal.go:452,880,900 pattern=%q is_chained=%v reversible=%v end_seeded=%v paths=%d",
				shape.pattern, match.IsChained, reversed != nil, seeded, len(paths))
			if !shape.trigger {
				require.True(t, seeded, "one-hop control walks back from the bound end")
				require.Len(t, paths, 1)
				return
			}
			require.True(t, match.IsChained, "the two-hop pattern is parsed as chained")
			assert.True(t, seeded, "a bound end node must be able to seed a chained pattern")
		})
	}
}

// TestV152RCCConditionalNestedCallRebind: the lowering of a conditional arm
// produces a nested CALL returning the imported name; the unchanged-import
// classifier (match_semantic_validation.go:1700-1717) answers false only for
// that nested form, which is the VariableAlreadyBound the public query gets.
func TestV152RCCConditionalNestedCallRebind(t *testing.T) {
	ctx := context.Background()
	for _, body := range []struct {
		name, text string
		trigger    bool
	}{
		{"one_level_control", "RETURN kept, 2 AS extra", false},
		{"one_level_filter_control", "FILTER true RETURN kept, 2 AS extra", false},
		{"nested_lowered_trigger", "FILTER true CALL (*) { RETURN kept, 2 AS extra } RETURN kept, extra", true},
		{"nested_renamed_trigger_shape", "FILTER true CALL (*) { RETURN kept AS k } RETURN k AS kept", false},
	} {
		t.Run(body.name, func(t *testing.T) {
			parsed := readQueryBody(ctx, body.text, 0, len(body.text))
			unchanged := callSubqueryReturnsOuterUnchanged(ctx, parsed.branches, "kept")
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rcc_cond"))
			query := "CYPHER 25 WITH 7 AS kept CALL (*) { " + body.text + " } RETURN *"
			_, execErr := exec.Execute(ctx, query, nil)
			t.Logf("RCC_COND owner=pkg/cypher/match_semantic_validation.go:1700-1717 body=%q returns_outer_unchanged=%v public_error=%v", body.text, unchanged, execErr)
			if body.name == "nested_renamed_trigger_shape" {
				// Returns a different value under kept: the classifier must refuse; control of the refusal branch.
				require.False(t, unchanged)
				return
			}
			if !body.trigger {
				require.True(t, unchanged, "control: unchanged import accepted")
				require.NoError(t, execErr)
				return
			}
			assert.True(t, unchanged, "a nested CALL that returns the imported name unchanged is not a rebind")
			assert.NoError(t, execErr, "the lowered nested form must execute")
		})
	}
}
