package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newConvergenceExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "convergence")
	return NewStorageExecutor(store), context.Background()
}

func requireSingleValue(t *testing.T, result *ExecuteResult, want interface{}) {
	t.Helper()
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	require.Equal(t, want, result.Rows[0][0])
}

func TestRelationshipSetExpressionsUseRelationshipScope(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:O {id:2}), (:I {sku:'a1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (o:O {id:2}), (i:I {sku:'a1'}) CREATE (o)-[:HAS {n:5}]->(i)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = h.n + 1 RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(6))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 10 RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(10))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 2 * h.n RETURN h.n AS n", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(20))

	result, err = exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.m = toString(h.n) + 'x' RETURN h.m AS m", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, "20x")

	readback, err := exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(:I) RETURN h.n AS n, h.m AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(20), "20x"}}, readback.Rows)
}

func TestSetMapMergeEvaluatesBoundExpressions(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:1, name:'n1'}), (:A {id:2, name:'n2'}), (:A {id:3, name:'n3'}), (:A {id:4, name:'n4'})", nil)
	require.NoError(t, err)
	rows := []interface{}{
		map[string]interface{}{"id": int64(1), "name": "n1"},
		map[string]interface{}{"id": int64(2), "name": "n2"},
	}
	result, err := exec.Execute(ctx, "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1), "n1"}, {int64(2), "n2"}}, result.Rows)

	result, err = exec.Execute(ctx, "UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name + 'x'} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(1), "n1x"}, {int64(2), "n2x"}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (a:A {id: 3}) SET a += {name: a.name + 'y', id2: a.id * 2} RETURN a.name AS n, a.id2 AS id2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"n3y", int64(6)}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (a:A {id: 4}) SET a += {name: toUpper(a.name), n2: 1 + 2} RETURN a.name AS n, a.n2 AS n2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"N4", int64(3)}}, result.Rows)
}

func TestRelationshipMergeAppliesCreateAndMatchAssignments(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:P {name:'Ann'}), (:P {name:'Dee'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON CREATE SET r.w = 1 RETURN r.w AS w", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(1))

	result, err = exec.Execute(ctx, "MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:KNOWS]->(d) ON MATCH SET r.w = r.w + 1 RETURN r.w AS w", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(2))

	readback, err := exec.Execute(ctx, "MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) RETURN r.w AS w, count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, readback.Rows)
}

func TestRelationshipMergeHonorsPatternDirection(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:DirectionFixture {id:'a'}), (b:DirectionFixture {id:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:DirectionFixture {id:'a'}), (b:DirectionFixture {id:'b'}) CREATE (b)-[:INCOMING]->(a), (b)-[:UNDIRECTED]->(a)", nil)
	require.NoError(t, err)

	incoming, err := exec.Execute(ctx, "MATCH (a:DirectionFixture {id:'a'}), (b:DirectionFixture {id:'b'}) MERGE (a)<-[r:INCOMING]-(b) RETURN startNode(r).id AS start, endNode(r).id AS end", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"b", "a"}}, incoming.Rows)
	require.Zero(t, incoming.Stats.RelationshipsCreated)

	undirectedMatch, err := exec.Execute(ctx, "MATCH (a:DirectionFixture {id:'a'}), (b:DirectionFixture {id:'b'}) MERGE (a)-[r:UNDIRECTED]-(b) RETURN startNode(r).id AS start, endNode(r).id AS end", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"b", "a"}}, undirectedMatch.Rows)
	require.Zero(t, undirectedMatch.Stats.RelationshipsCreated)

	undirectedCreate, err := exec.Execute(ctx, "MATCH (a:DirectionFixture {id:'a'}), (b:DirectionFixture {id:'b'}) MERGE (a)-[r:CREATED]-(b) RETURN startNode(r).id AS start, endNode(r).id AS end", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", "b"}}, undirectedCreate.Rows)
	require.Equal(t, 1, undirectedCreate.Stats.RelationshipsCreated)
}

func TestRelationshipMergeMatchesListIdentity(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:ListFixture {id:'a'}), (b:ListFixture {id:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:ListFixture {id:'a'}), (b:ListFixture {id:'b'}) CREATE (a)-[:LISTED {values:[1, 2, 3]}]->(b)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:ListFixture {id:'a'}), (b:ListFixture {id:'b'}) MERGE (a)-[r:LISTED {values:[1, 2, 3]}]->(b) RETURN r.values AS values", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]int64{1, 2, 3}}}, result.Rows)
	require.Zero(t, result.Stats.RelationshipsCreated)
}

func TestMutationPipelineCarriesBindingsAcrossMergeChains(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	created, err := exec.Execute(ctx, "CREATE (a:ChainFixture {id:'a'}), (b:ChainFixture {id:'b'}) MERGE (a)-[:FIRST]->(b) RETURN count(a) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, created, int64(1))
	require.Equal(t, 2, created.Stats.NodesCreated)
	require.Equal(t, 1, created.Stats.RelationshipsCreated)

	chained, err := exec.Execute(ctx, "MERGE (a:ChainFixture {id:'a'}) MERGE (b:ChainFixture {id:'b'}) MERGE (a)-[:SECOND]->(b) MERGE (a)-[:THIRD]->(b)", nil)
	require.NoError(t, err)
	require.Equal(t, 2, chained.Stats.RelationshipsCreated)

	readback, err := exec.Execute(ctx, "MATCH (:ChainFixture)-[r]->(:ChainFixture) RETURN count(r) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(3))
}

func TestMergePathBindingFlowsThroughMutationPipeline(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	result, err := exec.Execute(ctx, "MERGE (a {num:1}) MERGE (b {num:2}) MERGE path = (a)-[:LINK]->(b) RETURN path", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"path"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	pathMap, ok := result.Rows[0][0].(map[string]interface{})
	require.True(t, ok, "path result type: %T", result.Rows[0][0])
	path, ok := pathMap["_pathResult"].(PathResult)
	require.True(t, ok, "embedded path result type: %T", pathMap["_pathResult"])
	require.Len(t, path.Nodes, 2)
	require.Len(t, path.Relationships, 1)
	require.Equal(t, storage.NodeID(path.Nodes[0].ID), path.Relationships[0].StartNode)
	require.Equal(t, storage.NodeID(path.Nodes[1].ID), path.Relationships[0].EndNode)
}

func TestMergeUsesBindingsProjectedAcrossWithHorizons(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:AliasFixture {id:0})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (original:AliasFixture) WITH original AS source, original AS target MERGE (source)-[:SELF]->(target) RETURN source.id AS id", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(0))
	require.Equal(t, 1, result.Stats.RelationshipsCreated)

	chained, err := exec.Execute(ctx, "MATCH (original:AliasFixture) WITH original AS source MERGE (created:AliasCreated {id:1}) MERGE (source)-[:LINK]->(created) WITH source AS projected MERGE (createdAgain:AliasCreated {id:1}) MERGE (projected)-[:SECOND]->(createdAgain) RETURN projected.id AS id", nil)
	require.NoError(t, err)
	requireSingleValue(t, chained, int64(0))
	require.Equal(t, 1, chained.Stats.NodesCreated)
	require.Equal(t, 2, chained.Stats.RelationshipsCreated)
}

func TestMutationPipelineCarriesEveryRelationshipInChainedMatch(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:DeleteChain), (b:DeleteChain), (c:DeleteChain) CREATE (a)-[:LINK]->(b), (b)-[:LINK]->(c)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:DeleteChain)-[first:LINK]->(b:DeleteChain)-[second:LINK]->(c:DeleteChain) DELETE first, second, b, c MERGE (replacement:Replacement)", nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.RelationshipsDeleted)
	require.Equal(t, 2, result.Stats.NodesDeleted)
	require.Equal(t, 1, result.Stats.NodesCreated)
}

func TestBareUndirectedMatchPreservesGraphConnectivity(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:BareDirection {side:2}), (b:BareDirection {side:1}), (c:BareDirection {side:1}), (d:BareDirection {side:2}) CREATE (a)-[:LINK]->(b), (c)-[:LINK]->(d)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (left:BareDirection {side:2})--(right:BareDirection {side:1}) RETURN left, right", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
}

func TestRelationshipMergeEmitsEveryExistingMatch(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:MergeCardinality {side:'a'}), (b:MergeCardinality {side:'b'}) CREATE (a)-[:LINK]->(b), (a)-[:LINK]->(b)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:MergeCardinality {side:'a'}), (b:MergeCardinality {side:'b'}) MERGE (a)-[relationship:LINK]->(b) RETURN count(relationship) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(2))
	require.Zero(t, result.Stats.RelationshipsCreated)
}

func TestNodeMergeBindsZeroLengthPath(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	result, err := exec.Execute(ctx, "MERGE path = (node {num:1}) RETURN path", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"path"}, result.Columns)
	require.Len(t, result.Rows, 1)
	pathMap, ok := result.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	path, ok := pathMap["_pathResult"].(PathResult)
	require.True(t, ok)
	require.Len(t, path.Nodes, 1)
	require.Empty(t, path.Relationships)
	require.Equal(t, int64(1), path.Nodes[0].Properties["num"])
}

func TestNodeMergeUsesFreshlyCreatedPropertyBinding(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (source {num:1}) MERGE ({copied:source.num})", nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.NodesCreated)

	readback, err := exec.Execute(ctx, "MATCH (target {copied:1}) RETURN target.copied AS copied", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(1))
}

func TestRelationshipMergeAppliesCreateMutationsToBoundEntities(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {name:'A'}), (:B {name:'B'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:A), (b:B) MERGE (a)-[relationship:TYPE]->(b) ON CREATE SET relationship.name = 'linked', b.created = 1", nil)
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.RelationshipsCreated)
	require.Equal(t, 2, result.Stats.PropertiesSet)

	relReadback, err := exec.Execute(ctx, "MATCH ()-[relationship:TYPE]->() RETURN relationship.name AS name", nil)
	require.NoError(t, err)
	requireSingleValue(t, relReadback, "linked")
	dynamicReadback, err := exec.Execute(ctx, "MATCH ()-[relationship:TYPE]->() RETURN [key IN keys(relationship) | key + '->' + relationship[key]] AS entries", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"name->linked"}}}, dynamicReadback.Rows)
	nodeReadback, err := exec.Execute(ctx, "MATCH (node:B) RETURN node.created AS created", nil)
	require.NoError(t, err)
	requireSingleValue(t, nodeReadback, int64(1))
}

func TestCartesianMatchFiltersByElementIdentifiersBeforeMutation(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	created, err := exec.Execute(ctx, "CREATE (source:Memory {name:'source'}), (target:Memory {name:'target'}) RETURN elementId(source), elementId(target)", nil)
	require.NoError(t, err)
	require.Len(t, created.Rows, 1)
	sourceID := created.Rows[0][0]
	targetID := created.Rows[0][1]
	require.Contains(t, sourceID, "4:convergence:")
	require.Contains(t, targetID, "4:convergence:")

	matched, err := exec.Execute(ctx,
		"MATCH (source), (target) WHERE elementId(source) = $source AND elementId(target) = $target RETURN source.name, target.name",
		map[string]interface{}{"source": sourceID, "target": targetID},
	)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"source", "target"}}, matched.Rows)

	result, err := exec.Execute(ctx,
		"MATCH (source), (target) WHERE elementId(source) = $source AND elementId(target) = $target CREATE (source)-[relationship:LINK]->(target) SET relationship.strength = 0.75 RETURN elementId(relationship)",
		map[string]interface{}{"source": sourceID, "target": targetID},
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, 1, result.Stats.RelationshipsCreated)
	require.Contains(t, result.Rows[0][0], "5:convergence:")

	readback, err := exec.Execute(ctx, "MATCH ()-[relationship:LINK]->() RETURN relationship.strength", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{0.75}}, readback.Rows)
}

func TestRemovePropertyThenSetLabelPreservesMatchedScope(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:P {name:'Ann'}), (:P {name:'Bob'}), (:P {name:'Cid'}), (:P {name:'Dee', city:'Riga'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.name AS n, p.city AS city, labels(p) AS l", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Dee", nil, []interface{}{"P", "VIP"}}}, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH (p:VIP) RETURN count(p) AS vip", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(1))
}

func TestAssigningNullRemovesNodeProperty(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:I {sku:'a1', price:10.5, qty:3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (i:I {sku:'a1'}) SET i.price = null RETURN i.price AS p, keys(i) AS k", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Nil(t, result.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"sku", "qty"}, result.Rows[0][1])

	readback, err := exec.Execute(ctx, "MATCH (i:I {sku:'a1'}) RETURN keys(i) AS k", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"sku", "qty"}, readback.Rows[0][0])
}

func TestAssigningNullRemovesRelationshipProperty(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE ()-[:LINK {obsolete: 1, retained: 2}]->()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH ()-[relationship:LINK]->() SET relationship.obsolete = null RETURN keys(relationship) AS keys", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"retained"}, result.Rows[0][0])

	readback, err := exec.Execute(ctx, "MATCH ()-[relationship:LINK]->() RETURN keys(relationship) AS keys", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"retained"}, readback.Rows[0][0])
}

func TestWholeMapAssignmentOmitsNullProperties(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:Item {old: 1})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (item:Item) SET item = {kept: 2, omitted: null} RETURN keys(item) AS keys", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"kept"}, result.Rows[0][0])
}

func TestSetAcceptsParenthesizedEntityTargets(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (source:A)-[:LINK]->(target:B)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (node:A)-[relationship:LINK]->(:B) SET (node).name = 'neo4j', (relationship).name = 'neo4j' RETURN node.name, relationship.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"neo4j", "neo4j"}}, result.Rows)
}

func TestSetConcatenatesPropertyListsInEitherOrder(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	result, err := exec.Execute(ctx, "CREATE (node {numbers: [3, 4, 5]}) SET node.numbers = [1, 2] + node.numbers SET node.numbers = node.numbers + [6, 7] RETURN node.numbers", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7)}}}, result.Rows)
}

func TestSetRejectsUndefinedExpressionVariablesBeforeExecution(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	_, err := exec.Execute(ctx, "MATCH (node) SET node.name = missing RETURN node", nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, "UndefinedVariable", semanticError.Detail)
}

func TestSetRejectsMapsNestedInPropertyLists(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	_, err := exec.Execute(ctx, "CREATE (node) SET node.maplist = [{num: 1}]", nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.TypeError", semanticError.Code)
	require.Equal(t, "InvalidPropertyType", semanticError.Detail)
}

func TestSetAddsChainedLabels(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:4})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:A {id:4}) SET a:Extra:Hot RETURN labels(a) AS l", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])

	result, err = exec.Execute(ctx, "MATCH (a:A {id:4}) SET a:Extra, a:Hot RETURN labels(a) AS l", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])
}

func TestSetAppliesReturnWindowAfterAllNodeMutations(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:N {num:1}), (:N {num:2}), (:N {num:3}), (:N {num:4}), (:N {num:5})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:N) SET n.num = 42 RETURN n.num AS num SKIP 2 LIMIT 2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(42)}, {int64(42)}}, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH (n:N {num:42}) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(5))
}

func TestSetFeedsMutatedRowsThroughWithFilteringAndAggregation(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:N {num:1}), (:N {num:2}), (:N {num:3}), (:N {num:4}), (:N {num:5})", nil)
	require.NoError(t, err)

	filtered, err := exec.Execute(ctx, "MATCH (n:N) SET n.num = n.num + 1 WITH n WHERE n.num % 2 = 0 RETURN n.num AS num", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}}, filtered.Rows)

	aggregated, err := exec.Execute(ctx, "MATCH (n:N) SET n.num = n.num + 1 WITH sum(n.num) AS sum RETURN sum", nil)
	require.NoError(t, err)
	requireSingleValue(t, aggregated, int64(25))
}

func TestSetAppliesLabelsBeforeResultLimiting(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:N), (:N), (:N), (:N), (:N)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:N) SET n:Marked RETURN n LIMIT 0", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH (n:Marked) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(5))
}

func TestSetFeedsMutatedRelationshipsThroughAggregation(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE ()-[:R {num:1}]->(), ()-[:R {num:2}]->(), ()-[:R {num:3}]->(), ()-[:R {num:4}]->(), ()-[:R {num:5}]->()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH ()-[r:R]->() SET r.num = r.num + 1 RETURN sum(r.num) AS sum", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(20))
}

func TestRemoveAppliesResultWindowAfterAllNodeMutations(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:N {name:'a'}), (:N {name:'a'}), (:N {name:'a'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:N) REMOVE n.name RETURN n LIMIT 0", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH (n:N) RETURN count(n.name) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(0))
}

func TestRemoveFeedsLabelsThroughAggregation(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:N {num:1}), (:N {num:2}), (:N {num:3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:N) REMOVE n:N WITH sum(n.num) AS sum RETURN sum", nil)
	require.NoError(t, err)
	requireSingleValue(t, result, int64(6))

	readback, err := exec.Execute(ctx, "MATCH (n:N) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(0))
}

func TestRemoveFeedsRelationshipsThroughFiltering(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE ()-[:R {name:'a', num:1}]->(), ()-[:R {name:'a', num:2}]->(), ()-[:R {name:'a', num:3}]->()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH ()-[r:R]->() REMOVE r.name WITH r WHERE r.num % 2 = 0 RETURN r.num AS num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	readback, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r.name) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(0))
}

func TestRemoveIgnoresNullOptionalBindings(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	propertyResult, err := exec.Execute(ctx, "OPTIONAL MATCH (node:Missing) REMOVE node.value RETURN node", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, propertyResult.Rows)

	labelResult, err := exec.Execute(ctx, "OPTIONAL MATCH (node:Missing) REMOVE node:Missing RETURN node", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, labelResult.Rows)
}

func TestDeleteIgnoresNullOptionalPath(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	result, err := exec.Execute(ctx, "OPTIONAL MATCH path = ()-->() DETACH DELETE path", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	require.Equal(t, &QueryStats{}, result.Stats)

	_, err = exec.Execute(ctx, "CREATE (:Source)-[:LINK]->(:Target)", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "OPTIONAL MATCH path = (:Source)-[:LINK]->(:Target) DETACH DELETE path", nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.NodesDeleted)
	require.Equal(t, 1, result.Stats.RelationshipsDeleted)

	readback, err := exec.Execute(ctx, "MATCH (node) RETURN count(node) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, readback, int64(0))
}

func TestMergeReturnPreservesImplicitExpressionColumnNames(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	counted, err := exec.Execute(ctx, "MERGE (node) RETURN count(*) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"count"}, counted.Columns)
	requireSingleValue(t, counted, int64(1))

	property, err := exec.Execute(ctx, "MERGE (item:Item {value: 42}) RETURN item.value", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"item.value"}, property.Columns)
	requireSingleValue(t, property, int64(42))
}

func TestMergeRejectsInvalidPatternBindingsAndValues(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:Existing)", nil)
	require.NoError(t, err)

	tests := []struct {
		name   string
		query  string
		code   string
		detail string
	}{
		{
			name:   "already bound node",
			query:  "MATCH (node:Existing) MERGE (node)",
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "VariableAlreadyBound",
		},
		{
			name:   "parameter map predicate",
			query:  "MERGE (node $properties) RETURN node",
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "InvalidParameterUse",
		},
		{
			name:   "null match property",
			query:  "MERGE (:Item {value: null})",
			code:   "Neo.ClientError.Statement.SemanticError",
			detail: "MergeReadOwnWrites",
		},
		{
			name:   "relationship without type",
			query:  "MATCH (source), (target) MERGE (source)-->(target)",
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "NoSingleRelationshipType",
		},
		{
			name:   "relationship with multiple types",
			query:  "MATCH (source), (target) MERGE (source)-[:FIRST|SECOND]->(target)",
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "NoSingleRelationshipType",
		},
		{
			name:   "variable length relationship",
			query:  "MATCH (source), (target) MERGE (source)-[:LINK*1..2]->(target)",
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "CreatingVarLength",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, test.query, map[string]interface{}{"properties": map[string]interface{}{"value": 1}})
			require.Error(t, err)
			var semanticError *SemanticError
			require.ErrorAs(t, err, &semanticError)
			require.Equal(t, test.code, semanticError.Code)
			require.Equal(t, test.detail, semanticError.Detail)
		})
	}
}

func TestMergeValidationTracksBindingsAcrossClauseComposition(t *testing.T) {
	exec, _ := newConvergenceExecutor(t)

	require.NoError(t, exec.validateMergeSemanticScopes("MATCH (source), (target) MERGE (source)-[relationship:LINK]->(target)"))
	require.NoError(t, exec.validateMergeSemanticScopes("MERGE (node:Item {value: $value})"))
	require.NoError(t, exec.validateMergeSemanticScopes("RETURN 1"))

	tests := []struct {
		name  string
		query string
	}{
		{name: "decorated bound endpoint", query: "MATCH (source) MERGE (source:Extra)-[:LINK]->()"},
		{name: "bound relationship", query: "MATCH ()-[relationship:LINK]->() MERGE ()-[relationship]->()"},
		{name: "projected binding", query: "MATCH (source) WITH source AS projected MERGE (projected)"},
		{name: "unwind binding", query: "UNWIND [1] AS value MERGE (value)"},
		{name: "created binding", query: "CREATE (node) MERGE (node)"},
		{name: "path binding", query: "MATCH path = ()-[:LINK]->() MERGE path = (:Other)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := exec.validateMergeSemanticScopes(test.query)
			require.Error(t, err)
			var semanticError *SemanticError
			require.ErrorAs(t, err, &semanticError)
			require.Equal(t, "VariableAlreadyBound", semanticError.Detail)
		})
	}
}

func TestMergeActionsRejectUndefinedVariablesBeforeExecution(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)

	for _, query := range []string{
		"MERGE (node) ON CREATE SET missing.value = 1",
		"MERGE (node) ON MATCH SET missing.value = 1",
	} {
		clauses, ok := splitPipelineClauses(query)
		require.True(t, ok)
		require.Len(t, clauses, 1)
		require.Equal(t, query, clauses[0].text)
		require.Error(t, exec.validateMergeSemanticScopes(query))
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err)
		var semanticError *SemanticError
		require.ErrorAs(t, err, &semanticError)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
		require.Equal(t, "UndefinedVariable", semanticError.Detail)
	}
}

func TestMutationExpressionsAndClauseCompositionInExplicitTransactions(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	run := func(query string, params map[string]interface{}) *ExecuteResult {
		t.Helper()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}
		_, err = exec.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		return result
	}

	run("CREATE (:P {name:'Ann'}), (:P {name:'Dee', city:'Riga'}), (:A {id:1, name:'n1'}), (:A {id:4})", nil)
	run("MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) CREATE (a)-[:KNOWS {w:1}]->(d)", nil)

	result := run("MATCH (:P {name:'Ann'})-[r:KNOWS]->(:P {name:'Dee'}) SET r.w = r.w + 1 RETURN r.w AS w", nil)
	requireSingleValue(t, result, int64(2))

	rows := []interface{}{map[string]interface{}{"id": int64(1), "name": "updated"}}
	result = run("UNWIND $rows AS row MERGE (a:A {id:row.id}) SET a += {name:row.name} RETURN a.name AS name", map[string]interface{}{"rows": rows})
	requireSingleValue(t, result, "updated")

	result = run("MATCH (a:P {name:'Ann'}), (d:P {name:'Dee'}) MERGE (a)-[r:LIKES]->(d) ON CREATE SET r.weight = 3 RETURN r.weight AS weight", nil)
	requireSingleValue(t, result, int64(3))

	result = run("MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.city AS city, labels(p) AS labels", nil)
	require.Nil(t, result.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"P", "VIP"}, result.Rows[0][1])

	result = run("MATCH (a:A {id:1}) SET a.name = null RETURN keys(a) AS keys", nil)
	require.ElementsMatch(t, []interface{}{"id"}, result.Rows[0][0])

	result = run("MATCH (a:A {id:4}) SET a:Extra:Hot RETURN labels(a) AS labels", nil)
	require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])
}

func TestSetEvaluationFailureRollsBackEarlierAssignments(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:A {id:1})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (a:A {id:1}) SET a.transient = 1, a += {broken:} RETURN a", nil)
	require.Error(t, err)

	readback, err := exec.Execute(ctx, "MATCH (a:A {id:1}) RETURN a.transient AS transient, keys(a) AS keys", nil)
	require.NoError(t, err)
	require.Nil(t, readback.Rows[0][0])
	require.ElementsMatch(t, []interface{}{"id"}, readback.Rows[0][1])
}

func TestCommaSeparatedCreateClausesPreserveVariableScope(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, `
		CREATE (a:Fixture {name: 'a'}),
		       (b:Fixture {name: 'b'}),
		       (c:Fixture {name: 'c'})
		CREATE (a)-[:NEXT]->(b),
		       (b)-[:NEXT]->(c)
	`, nil)
	require.NoError(t, err)

	nodes, err := exec.Execute(ctx, "MATCH (n:Fixture) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, nodes, int64(3))

	relationships, err := exec.Execute(ctx, "MATCH ()-[r:NEXT]->() RETURN count(r) AS count", nil)
	require.NoError(t, err)
	requireSingleValue(t, relationships, int64(2))
}

func BenchmarkSetExecutionPaths(b *testing.B) {
	const query = "MATCH (node:SetBenchmark) SET node.value = node.value + 1 RETURN node.value AS value"
	benchmark := func(b *testing.B, execute func(*StorageExecutor, context.Context) error) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "set-benchmark")
		for index := 0; index < 100; index++ {
			_, err := store.CreateNode(&storage.Node{
				ID:     storage.NodeID(fmt.Sprintf("set-benchmark-%d", index)),
				Labels: []string{"SetBenchmark"},
				Properties: map[string]interface{}{
					"value": int64(index),
				},
			})
			require.NoError(b, err)
		}
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if err := execute(exec, ctx); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("converged_pipeline", func(b *testing.B) {
		benchmark(b, func(exec *StorageExecutor, ctx context.Context) error {
			_, handled, err := exec.executePipeline(ctx, query)
			if !handled && err == nil {
				return fmt.Errorf("converged SET pipeline did not handle benchmark query")
			}
			return err
		})
	})
	b.Run("residual_handler", func(b *testing.B) {
		benchmark(b, func(exec *StorageExecutor, ctx context.Context) error {
			_, err := exec.executeSet(ctx, query)
			return err
		})
	})
}

func TestConvergedSetPipelineHelpers(t *testing.T) {
	t.Run("clause kind detection scopes normalization", func(t *testing.T) {
		clauses := []pipelineClause{{kind: pipelineClauseMatch}, {kind: pipelineClauseSet}}
		require.True(t, pipelineHasClauseKind(clauses, pipelineClauseSet))
		require.False(t, pipelineHasClauseKind(clauses, pipelineClauseReturn))
	})

	t.Run("whitespace normalization preserves quoted content", func(t *testing.T) {
		require.Equal(t, "MATCH (n) SET n.text = 'a  b' RETURN n", normalizePipelineWhitespace("\nMATCH\t(n)  SET n.text = 'a  b'\rRETURN n\n"))
		require.Equal(t, "RETURN `a``b`, \"c\\\"d\"", normalizePipelineWhitespace("RETURN\t`a``b`,\n\"c\\\"d\""))
		require.Equal(t, "RETURN 1", normalizePipelineWhitespace("  RETURN 1  "))
	})

	t.Run("assignment validation rejects malformed forms", func(t *testing.T) {
		invalid := [][]string{
			{""},
			{"bad-target += {value: 1}"},
			{"node +="},
			{"node += {value:}"},
			{"bad-target = 1"},
			{"node ="},
			{"invalid"},
			{"node:"},
			{"node:1bad"},
		}
		for _, assignments := range invalid {
			require.Error(t, validatePipelineSetAssignments(assignments), assignments)
		}
		require.NoError(t, validatePipelineSetAssignments([]string{"node += properties", "node.value = 1", "node:Valid"}))
	})

	t.Run("mutation operation describes failing assignment", func(t *testing.T) {
		require.Equal(t, "node +=", pipelineSetOperation("node", []string{"node += {value: 1}"}))
		require.Equal(t, "node =", pipelineSetOperation("node", []string{"node = {value: 1}"}))
		require.Equal(t, "node.property =", pipelineSetOperation("node", []string{"node.value = 1"}))
		require.Equal(t, "node", pipelineSetOperation("node", []string{"other.value = 1"}))
	})
}
