package cypher

import (
	"context"
	"reflect"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A container a statement built before its DELETE reads the entity it holds
// the way a direct binding does (#907): the deleted entity is its view (no
// labels or properties, keys()/properties() empty), while a property or label
// read reached through the container is still EntityNotFound.
// deletedEntityViews (deleted_entity_projection.go) is the one owner of that
// replacement: a container holding a deleted entity is copied with the view
// in place, every container it did not have to change keeps its identity, and
// a Go/native handler's cycle does not send the walk round for ever.

// deletedEntityViewsFixture is the projection state of a statement that
// deleted one node and one relationship.
func deletedEntityViewsFixture() (*deletedEntityViews, *storage.Node, *storage.Edge) {
	node := &storage.Node{ID: "deleted-node", Labels: []string{"X"}, Properties: map[string]interface{}{"p": int64(1)}}
	edge := &storage.Edge{
		ID: "deleted-edge", Type: "R", StartNode: "deleted-node", EndNode: "other-node",
		Properties: map[string]interface{}{"w": int64(2)},
	}
	views := &deletedEntityViews{deleted: &deletedEntities{
		nodes: map[storage.NodeID]struct{}{storage.NodeID(node.IdentityKey()): {}},
		edges: map[storage.EdgeID]struct{}{storage.EdgeID(edge.IdentityKey()): {}},
	}}
	return views, node, edge
}

// requireSameMap requires clone to be the very map value source is.
func requireSameMap(t *testing.T, source, clone map[string]interface{}) {
	t.Helper()
	require.Equal(t, reflect.ValueOf(source).UnsafePointer(), reflect.ValueOf(clone).UnsafePointer())
}

// requireSameList requires clone to be the very list value source is.
func requireSameList(t *testing.T, source, clone []interface{}) {
	t.Helper()
	require.Equal(t, reflect.ValueOf(source).UnsafePointer(), reflect.ValueOf(clone).UnsafePointer())
}

// requireDeletedNodeView requires value to be the view of a deleted node: the
// same identity with no labels or properties.
func requireDeletedNodeView(t *testing.T, value interface{}, id storage.NodeID) {
	t.Helper()
	node, isNode := value.(*storage.Node)
	require.True(t, isNode, "%T", value)
	require.Equal(t, id, node.ID)
	require.Empty(t, node.Labels)
	require.Empty(t, node.Properties)
}

func TestDeletedEntityMapProjectionCopiesOnlyChangedEntries(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()
	healthyMap := map[string]interface{}{"x": int64(2)}
	healthyList := []interface{}{int64(1), int64(2)}
	value := map[string]interface{}{
		"entity":  node,
		"healthy": healthyMap,
		"numbers": healthyList,
		"nothing": nil,
	}

	replaced, changed := views.replace(context.Background(), value)
	require.True(t, changed)
	clone := replaced.(map[string]interface{})

	// The map holding the deleted entity is a copy; the map it did not have to
	// change, the list it did not have to change and a nil entry stay as they
	// were.
	require.NotEqual(t, uintptr(reflect.ValueOf(value).UnsafePointer()), uintptr(reflect.ValueOf(clone).UnsafePointer()))
	requireDeletedNodeView(t, clone["entity"], node.ID)
	requireSameMap(t, healthyMap, clone["healthy"].(map[string]interface{}))
	requireSameList(t, healthyList, clone["numbers"].([]interface{}))
	require.Nil(t, clone["nothing"])

	// The value the statement handed over is not mutated.
	require.Equal(t, node, value["entity"])
	require.Equal(t, []string{"X"}, node.Labels)
	require.Equal(t, map[string]interface{}{"p": int64(1)}, node.Properties)
}

func TestDeletedEntityMapProjectionKeepsUnchangedContainerIdentity(t *testing.T) {
	views, _, _ := deletedEntityViewsFixture()
	untouched := map[string]interface{}{
		"healthy": map[string]interface{}{"x": int64(2)},
		"numbers": []interface{}{int64(1)},
	}
	replaced, changed := views.replace(context.Background(), untouched)
	require.False(t, changed)
	requireSameMap(t, untouched, replaced.(map[string]interface{}))

	list := []interface{}{map[string]interface{}{"x": int64(2)}, []interface{}{int64(1)}}
	replaced, changed = views.replace(context.Background(), list)
	require.False(t, changed)
	requireSameList(t, list, replaced.([]interface{}))
}

func TestDeletedEntityMapProjectionCopiesNestedContainers(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()

	outer := map[string]interface{}{"inner": node}
	healthy := map[string]interface{}{"x": int64(2)}
	value := map[string]interface{}{"outer": outer, "healthy": healthy}

	replaced, changed := views.replace(context.Background(), value)
	require.True(t, changed)
	clone := replaced.(map[string]interface{})
	outerClone := clone["outer"].(map[string]interface{})
	requireDeletedNodeView(t, outerClone["inner"], node.ID)
	// The nested map changed, so it is a copy; the untouched sibling is not.
	require.NotEqual(t, uintptr(reflect.ValueOf(outer).UnsafePointer()), uintptr(reflect.ValueOf(outerClone).UnsafePointer()))
	requireSameMap(t, healthy, clone["healthy"].(map[string]interface{}))
	require.Equal(t, node, outer["inner"], "the nested input map is not mutated")
}

func TestDeletedEntityMapProjectionCopiesListsOfMaps(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()

	changedMap := map[string]interface{}{"entity": node}
	healthyList := []interface{}{int64(7)}
	value := []interface{}{changedMap, healthyList}

	replaced, changed := views.replace(context.Background(), value)
	require.True(t, changed)
	clone := replaced.([]interface{})
	requireDeletedNodeView(t, clone[0].(map[string]interface{})["entity"], node.ID)
	require.NotEqual(t, uintptr(reflect.ValueOf(changedMap).UnsafePointer()), uintptr(reflect.ValueOf(clone[0].(map[string]interface{})).UnsafePointer()))
	requireSameList(t, healthyList, clone[1].([]interface{}))
	require.Equal(t, node, changedMap["entity"], "the list's input map is not mutated")
}

func TestDeletedEntityMapProjectionKeepsAliases(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()

	sharedMap := map[string]interface{}{"entity": node}
	sharedList := []interface{}{sharedMap}
	value := map[string]interface{}{"a": sharedMap, "b": sharedMap, "c": sharedList, "d": sharedList}

	replaced, changed := views.replace(context.Background(), value)
	require.True(t, changed)
	clone := replaced.(map[string]interface{})
	requireSameMap(t, clone["a"].(map[string]interface{}), clone["b"].(map[string]interface{}))
	requireSameList(t, clone["c"].([]interface{}), clone["d"].([]interface{}))
	requireSameMap(t, clone["a"].(map[string]interface{}), clone["c"].([]interface{})[0].(map[string]interface{}))
	requireDeletedNodeView(t, clone["a"].(map[string]interface{})["entity"], node.ID)
}

func TestDeletedEntityMapProjectionPreservesCycleTopology(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()

	// A map that holds itself and the deleted entity: the copy is the cycle,
	// with the view in place (a Go/native handler may return either shape).
	selfCycle := map[string]interface{}{"entity": node}
	selfCycle["self"] = selfCycle
	replaced, changed := views.replace(context.Background(), selfCycle)
	require.True(t, changed)
	clone, isMap := replaced.(map[string]interface{})
	require.True(t, isMap, "%T", replaced)
	requireDeletedNodeView(t, clone["entity"], node.ID)
	requireSameMap(t, clone, clone["self"].(map[string]interface{}))
	require.NotEqual(t, uintptr(reflect.ValueOf(selfCycle).UnsafePointer()), uintptr(reflect.ValueOf(clone).UnsafePointer()))

	// A map and a list that hold each other, plus the deleted entity.
	mutualMap := map[string]interface{}{"entity": node}
	mutualList := []interface{}{mutualMap}
	mutualMap["list"] = mutualList
	replaced, changed = views.replace(context.Background(), mutualList)
	require.True(t, changed)
	listClone := replaced.([]interface{})
	mapClone := listClone[0].(map[string]interface{})
	requireDeletedNodeView(t, mapClone["entity"], node.ID)
	requireSameList(t, listClone, mapClone["list"].([]interface{}))

	// A cycle that holds no deleted entity keeps its identity.
	healthyCycle := map[string]interface{}{}
	healthyCycle["self"] = healthyCycle
	replaced, changed = views.replace(context.Background(), healthyCycle)
	require.False(t, changed)
	requireSameMap(t, healthyCycle, replaced.(map[string]interface{}))
}

func TestDeletedEntityMapProjectionKeepsMapKeyOrder(t *testing.T) {
	views, node, _ := deletedEntityViewsFixture()
	ctx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
	failure := ctx.Value(expressionFailureKey{}).(*expressionFailure)
	source := map[string]interface{}{"entity": node, "keep": int64(1)}
	failure.recordMapKeyOrder(source, []string{"entity", "keep"})

	replaced, changed := views.replace(ctx, source)
	require.True(t, changed)
	clone := replaced.(map[string]interface{})
	orders := snapshotMapKeyOrders(ctx)
	require.Equal(t, []string{"entity", "keep"}, orders[uintptr(reflect.ValueOf(clone).UnsafePointer())])
	require.Equal(t, []string{"entity", "keep"}, orders[uintptr(reflect.ValueOf(source).UnsafePointer())])
}

func TestDeletedEntityMapProjectionKeepsPathHandling(t *testing.T) {
	views, node, edge := deletedEntityViewsFixture()
	executor, _ := newTestExecutor(t, "deleted_container_unit")
	views.executor = executor
	ctx := context.Background()

	kept := &storage.Node{ID: "kept-node"}
	path := executor.pathToMapContext(ctx, PathResult{Nodes: []*storage.Node{node, kept}, Relationships: []*storage.Edge{edge}, Length: 1})
	replaced, changed := views.replace(ctx, path)
	require.True(t, changed)
	clone, isMap := replaced.(map[string]interface{})
	require.True(t, isMap, "%T", replaced)
	require.NotEqual(t, uintptr(reflect.ValueOf(path).UnsafePointer()), uintptr(reflect.ValueOf(clone).UnsafePointer()))
	require.Contains(t, clone, "_pathResult")
	nodes := clone["nodes"].([]interface{})
	require.Len(t, nodes, 2)
	requireDeletedNodeView(t, nodes[0], node.ID)
	require.Same(t, kept, nodes[1])
	relationships := clone["relationships"].([]interface{})
	require.Len(t, relationships, 1)
	require.Equal(t, "R", relationships[0].(map[string]interface{})["type"])
	parts := clone["_pathResult"].(PathResult)
	require.Same(t, kept, parts.Nodes[1])
	require.Nil(t, parts.Relationships[0].Properties, "a deleted relationship keeps no properties")
	require.Equal(t, node, path["nodes"].([]interface{})[0], "the input path map is not mutated")

	// A path holding no deleted entity keeps its identity too.
	healthy := executor.pathToMapContext(ctx, PathResult{Nodes: []*storage.Node{{ID: "kept"}}, Length: 0})
	replaced, changed = views.replace(ctx, healthy)
	require.False(t, changed)
	requireSameMap(t, healthy, replaced.(map[string]interface{}))
}

// TestDeletedEntityContainerProjectionMatchNeo4j is the public contract: a map
// or list the statement built before its DELETE reads the deleted entity as
// its view, on both parsers and both query-normalization settings.
func TestDeletedEntityContainerProjectionMatchNeo4j(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t, "deleted_container_projection")
		ctx := context.Background()
		inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer func() {
				_, err := exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}()
			return exec.Execute(ctx, query, nil)
		}
		requireFound := func(t *testing.T, result *ExecuteResult, want int) []interface{} {
			t.Helper()
			require.Len(t, result.Rows, want)
			return result.Rows[0]
		}
		// The relationship fixture lives outside the rolled-back statement each
		// case runs in, so every case reads the graph it was created as.
		_, err := exec.Execute(ctx, "CREATE (:DR {id: 1})-[:DCR {w: 2}]->(:DR {id: 2})", nil)
		require.NoError(t, err)

		t.Run("reported repro", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:X {p: 1}) WITH n, {entity: n} AS m DELETE n RETURN properties(m.entity), keys(m.entity), m")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
			mapValue, isMap := row[2].(map[string]interface{})
			require.True(t, isMap, "%T", row[2])
			requireDeletedNodeView(t, mapValue["entity"], mapValue["entity"].(*storage.Node).ID)
		})

		t.Run("map built before the DELETE", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC1 {p: 1}) WITH n, {entity: n} AS m DELETE n RETURN properties(m.entity) AS props, keys(m.entity) AS keys, m AS m")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
			mapValue, isMap := row[2].(map[string]interface{})
			require.True(t, isMap, "%T", row[2])
			requireDeletedNodeView(t, mapValue["entity"], mapValue["entity"].(*storage.Node).ID)
		})

		t.Run("nested map built before the DELETE", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC2 {p: 1}) WITH n, {outer: {inner: n}} AS m DELETE n RETURN properties(m.outer.inner) AS props, keys(m.outer.inner) AS keys, m AS m")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
			outer := row[2].(map[string]interface{})["outer"].(map[string]interface{})
			requireDeletedNodeView(t, outer["inner"], outer["inner"].(*storage.Node).ID)
		})

		t.Run("list of maps built before the DELETE", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC3 {p: 1}) WITH n, [{entity: n}] AS ms DELETE n RETURN properties(ms[0].entity) AS props, keys(ms[0].entity) AS keys, ms AS ms")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
			element := row[2].([]interface{})[0].(map[string]interface{})
			requireDeletedNodeView(t, element["entity"], element["entity"].(*storage.Node).ID)
		})

		t.Run("aliased entity in one map", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC4 {p: 1}) WITH n, {a: n, b: n} AS m DELETE n RETURN properties(m.a) AS a, properties(m.b) AS b")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, map[string]interface{}{}, row[1])
		})

		t.Run("healthy siblings in the same map", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC5 {p: 1}) WITH n, {entity: n, keep: 1, other: {x: 2}, list: [1, 2]} AS m DELETE n RETURN properties(m.entity) AS props, m AS m")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			mapValue := row[1].(map[string]interface{})
			requireDeletedNodeView(t, mapValue["entity"], mapValue["entity"].(*storage.Node).ID)
			require.Equal(t, int64(1), mapValue["keep"])
			require.Equal(t, map[string]interface{}{"x": int64(2)}, mapValue["other"])
			require.Equal(t, []interface{}{int64(1), int64(2)}, mapValue["list"])
		})

		t.Run("direct deleted node keeps its own view", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC6 {p: 1, q: 'a'}) DELETE n RETURN properties(n) AS props, keys(n) AS keys, n AS n")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
			requireDeletedNodeView(t, row[2], row[2].(*storage.Node).ID)
		})

		t.Run("map built after the DELETE", func(t *testing.T) {
			result, err := inRolledBackTransaction("CREATE (n:DC7 {p: 1}) DELETE n WITH n, {entity: n} AS m RETURN properties(m.entity) AS props, keys(m.entity) AS keys")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, map[string]interface{}{}, row[0])
			require.Equal(t, []interface{}{}, row[1])
		})

		t.Run("property read through a nested map is still EntityNotFound", func(t *testing.T) {
			for _, query := range []string{
				"CREATE (n:DC8 {p: 1}) WITH n, {entity: n} AS m DELETE n RETURN m.entity.p AS v",
				"CREATE (n:DC9 {p: 1}) WITH n, {outer: {inner: n}} AS m DELETE n RETURN m.outer.inner.p AS v",
				"CREATE (n:DC10 {p: 1}) WITH n, [{entity: n}] AS ms DELETE n RETURN ms[0].entity.p AS v",
			} {
				_, err := inRolledBackTransaction(query)
				require.Error(t, err, query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code, query)
			}
		})

		t.Run("deleted relationship in a map keeps its type and fails property reads", func(t *testing.T) {
			result, err := inRolledBackTransaction("MATCH (:DR {id: 1})-[r:DCR]->(:DR {id: 2}) WITH r, {rel: r} AS m DELETE r RETURN type(m.rel) AS t, m AS m")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			require.Equal(t, "DCR", row[0])
			relMap := row[1].(map[string]interface{})
			rel, isEdge := relMap["rel"].(*storage.Edge)
			require.True(t, isEdge, "%T", relMap["rel"])
			require.Equal(t, "DCR", rel.Type)
			require.Empty(t, rel.Properties)
			for _, query := range []string{
				"MATCH (:DR {id: 1})-[r:DCR]->(:DR {id: 2}) DELETE r RETURN properties(r) AS v",
				"MATCH (:DR {id: 1})-[r:DCR]->(:DR {id: 2}) DELETE r RETURN keys(r) AS v",
				"MATCH (:DR {id: 1})-[r:DCR]->(:DR {id: 2}) WITH r, {rel: r} AS m DELETE r RETURN properties(m.rel) AS v",
			} {
				_, err := inRolledBackTransaction(query)
				require.Error(t, err, query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code, query)
			}
		})

		t.Run("a path nested in a map is still rebuilt", func(t *testing.T) {
			result, err := inRolledBackTransaction("MATCH p = (:DR {id: 1})-[:DCR]->(:DR {id: 2}) WITH p, {path: p} AS m DETACH DELETE p RETURN m.path AS path")
			require.NoError(t, err)
			row := requireFound(t, result, 1)
			pathValue, isMap := row[0].(map[string]interface{})
			require.True(t, isMap, "%T", row[0])
			require.Contains(t, pathValue, "_pathResult")
			nodes := pathValue["nodes"].([]interface{})
			require.Len(t, nodes, 2)
			requireDeletedNodeView(t, nodes[0], nodes[0].(*storage.Node).ID)
			relationships := pathValue["relationships"].([]interface{})
			require.Len(t, relationships, 1)
			require.Equal(t, "DCR", relationships[0].(map[string]interface{})["type"])
		})
	})
}
