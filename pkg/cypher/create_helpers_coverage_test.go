package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateHelpers_ProcessRelationshipResolveAndSetMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	nodeVars := map[string]*storage.Node{}
	edgeVars := map[string]*storage.Edge{}
	result := &ExecuteResult{Stats: &QueryStats{}}

	_, err := exec.planCreatePatterns(ctx, "(a:Person {name:'alice'})", nodeVars, edgeVars, nil, result)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "a")
	assert.Equal(t, 1, result.Stats.NodesCreated)

	_, err = exec.planCreatePatterns(ctx, "(a)-[r:KNOWS {since: 2020}]->(b:Person {name:'bob'})", nodeVars, edgeVars, nil, result)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "b")
	require.Contains(t, edgeVars, "r")
	assert.Equal(t, "KNOWS", edgeVars["r"].Type)
	assert.Equal(t, int64(2020), edgeVars["r"].Properties["since"])

	// Reverse arrow branch: created edge should start at inline node c and end at a.
	_, err = exec.planCreatePatterns(ctx, "(a)<-[rb:BACK]-(c:Person {name:'carol'})", nodeVars, edgeVars, nil, result)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "c")
	require.Contains(t, edgeVars, "rb")
	assert.Equal(t, nodeVars["c"].ID, edgeVars["rb"].StartNode)
	assert.Equal(t, nodeVars["a"].ID, edgeVars["rb"].EndNode)

	// Invalid relationship shape.
	_, err = exec.planCreatePatterns(ctx, "(a)-[:BROKEN](b)", nodeVars, edgeVars, nil, result)
	require.Error(t, err)

	// An endpoint variable not in scope is a new node (Neo4j CREATE semantics).
	_, err = exec.planCreatePatterns(ctx, "(missing)-[:REL]->(a)", nodeVars, edgeVars, nil, result)
	require.NoError(t, err)
	require.Contains(t, nodeVars, "missing")

	// SET += on created entities goes through the shared SET applicator.
	row := pipelineRow{}
	for name, node := range nodeVars {
		row[name] = node
	}
	for name, edge := range edgeVars {
		row[name] = edge
	}
	stats, handled, err := exec.pipelineApplySet(ctx, []pipelineRow{row}, "SET a += {score: 5, active: true}")
	require.NoError(t, err)
	require.True(t, handled)
	assert.Equal(t, int64(5), nodeVars["a"].Properties["score"])
	assert.Equal(t, true, nodeVars["a"].Properties["active"])
	assert.Equal(t, 2, stats.PropertiesSet)

	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{
		"edgeProps": map[string]interface{}{"rank": int64(9)},
	})
	stats, _, err = exec.pipelineApplySet(ctxWithParams, []pipelineRow{row}, "SET r += $edgeProps")
	require.NoError(t, err)
	assert.Equal(t, int64(9), edgeVars["r"].Properties["rank"])
	assert.Equal(t, 1, stats.PropertiesSet)

	// Strict error branches.
	_, _, err = exec.pipelineApplySet(ctx, []pipelineRow{row}, "SET a + {x:1}")
	require.Error(t, err)

	_, _, err = exec.pipelineApplySet(context.WithValue(ctx, paramsKey, map[string]interface{}{"missing": int64(1)}), []pipelineRow{row}, "SET a += $missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a map")

	_, _, err = exec.pipelineApplySet(ctx, []pipelineRow{row}, "SET a += {broken")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse properties")

	_, handled, err = exec.pipelineApplySet(ctx, []pipelineRow{row}, "SET unknown += {x:1}")
	require.NoError(t, err)
	assert.False(t, handled, "an unbound target is left to the caller")
}

func TestCreateHelpers_ParsersAndValidators(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	require.NoError(t, exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name:'ok'})"))
	require.NoError(t, exec.validateCreatePatternPropertyMap(ctx, "(n:Person)"))

	err := exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name:'x'")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid property map syntax")

	err = exec.validateCreatePatternPropertyMap(ctx, "(n:Person {name})")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid property map syntax")

	pathVar, stripped := parseCreatePathAssignment("p = (a)-[:R]->(b)")
	assert.Equal(t, "p", pathVar)
	assert.Equal(t, "(a)-[:R]->(b)", stripped)

	pathVar, stripped = parseCreatePathAssignment("1bad = (a)-[:R]->(b)")
	assert.Equal(t, "", pathVar)
	assert.Equal(t, "1bad = (a)-[:R]->(b)", stripped)

	parts := exec.splitCreatePatterns("p=(a:A {txt:'x -> y'})-[r:REL]->(b:B), (c:C)")
	require.Len(t, parts, 2)
	assert.Equal(t, "p=(a:A {txt:'x -> y'})-[r:REL]->(b:B)", strings.TrimSpace(parts[0]))
	assert.Equal(t, "(c:C)", strings.TrimSpace(parts[1]))

	nodes := exec.splitNodePatterns("(a:A {txt:'(x)' }), (b:B {v:1})")
	require.Len(t, nodes, 2)
	assert.Contains(t, nodes[0], "(a:A")
	assert.Contains(t, nodes[1], "(b:B")

	relType, relProps, _ := exec.parseRelationshipTypeAndProps(ctx, "r:KNOWS {since: 2021, note: 'x'}", nil)
	assert.Equal(t, "KNOWS", relType)
	assert.Equal(t, int64(2021), relProps["since"])
	assert.Equal(t, "x", relProps["note"])

	relType, relProps, _ = exec.parseRelationshipTypeAndProps(ctx, "r", nil)
	assert.Equal(t, "", relType)
	assert.Empty(t, relProps)

	relType, relProps, _ = exec.parseRelationshipTypeAndProps(ctx, ":", nil)
	assert.Equal(t, "", relType)
	assert.Empty(t, relProps)

	source, rel, target, reverse, remainder, err := exec.parseCreateRelPatternWithVars("(a)-[r:KNOWS]->(b)-[:NEXT]->(c)")
	require.NoError(t, err)
	assert.Equal(t, "a", source)
	assert.Equal(t, "r:KNOWS", rel)
	assert.Equal(t, "b", target)
	assert.False(t, reverse)
	assert.Equal(t, "-[:NEXT]->(c)", remainder)

	source, rel, target, reverse, remainder, err = exec.parseCreateRelPatternWithVars("(a)<-[r:BACK]-(b)")
	require.NoError(t, err)
	assert.Equal(t, "a", source)
	assert.Equal(t, "r:BACK", rel)
	assert.Equal(t, "b", target)
	assert.True(t, reverse)
	assert.Equal(t, "", remainder)

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("a)-[r:REL]->(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must start with (")

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("(a)-[r:REL->(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched bracket")

	_, _, _, _, _, err = exec.parseCreateRelPatternWithVars("(a)-[r:REL]-(b)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ->(")
}

func TestCreateHelpers_ResolveOrCreateAndMultipleCreates(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	nodeVars := map[string]*storage.Node{}
	var plan createPlan
	ctx := context.Background()

	edgeVars := map[string]*storage.Edge{}

	// An endpoint variable not in scope is a new node (Neo4j CREATE semantics).
	missing, err := exec.planCreateEndpoint(ctx, "missingVar", nodeVars, edgeVars, &plan)
	require.NoError(t, err)
	require.NotNil(t, missing)
	require.Contains(t, nodeVars, "missingVar")
	assert.Len(t, plan.nodes, 1)

	n, err := exec.planCreateEndpoint(ctx, "x:Node {id:'x1'}", nodeVars, edgeVars, &plan)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Contains(t, nodeVars, "x")
	assert.Len(t, plan.nodes, 2)

	// Existing variable returns same node without creating a new one.
	same, err := exec.planCreateEndpoint(ctx, "x:Node {id:'different'}", nodeVars, edgeVars, &plan)
	require.NoError(t, err)
	assert.Equal(t, n.ID, same.ID)
	assert.Len(t, plan.nodes, 2)

	// Inline node without variable is created but not added to nodeVars.
	inline, err := exec.planCreateEndpoint(ctx, ":Leaf {k:1}", nodeVars, edgeVars, &plan)
	require.NoError(t, err)
	require.NotNil(t, inline)
	assert.Len(t, plan.nodes, 3)
	_, hasLeaf := nodeVars["Leaf"]
	assert.False(t, hasLeaf)

	// Nothing is written until the plan is applied.
	count, err := store.NodeCount(t.Context())
	require.NoError(t, err)
	assert.Zero(t, count)
	result := &ExecuteResult{Stats: &QueryStats{}}
	require.NoError(t, exec.applyCreatePlan(ctx, &plan, result))
	assert.Equal(t, 3, result.Stats.NodesCreated)
	count, err = store.NodeCount(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 3, count)

	assert.True(t, isSimpleVariable("abc_123"))
	assert.False(t, isSimpleVariable("a-b"))
	keys := getKeys(nodeVars)
	assert.Contains(t, keys, "x")

	res, err := exec.Execute(ctx, "CREATE (a:Person {name:'a'}) WITH a CREATE (b:Person {name:'b'}) CREATE (a)-[r:KNOWS]->(b) RETURN a.name AS aname, b.name AS bname, type(r) AS rt", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "a", res.Rows[0][0])
	assert.Equal(t, "b", res.Rows[0][1])
	assert.Equal(t, "KNOWS", res.Rows[0][2])
	assert.Equal(t, 2, res.Stats.NodesCreated)
	assert.Equal(t, 1, res.Stats.RelationshipsCreated)
}
