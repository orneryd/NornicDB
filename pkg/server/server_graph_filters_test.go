package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestGraphNeighborhoodEndpoint_ExcludePropertyPathFragmentsGraph mirrors the
// graphify context-threading bias: a Context node carrying the "context"
// property sits on the path between otherwise separate call chains. Excluding
// "Context.context" must remove the node, its incident edges, and fragment
// the result into the two disconnected call chains.
func TestGraphNeighborhoodEndpoint_ExcludePropertyPathFragmentsGraph(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for id, label := range map[string]string{
		"a": "Code", "b": "Code", "c": "Code", "d": "Code",
	} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}})
		require.NoError(t, err)
	}
	// The Context node threads every parser call: a -> x -> b links the two
	// otherwise independent chains a -> c and b -> d.
	// Mark the threading node with the context property so the dotted
	// "Context.context" path excludes it.
	_, err := engine.CreateNode(&storage.Node{ID: "x", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bd", StartNode: "b", EndNode: "d", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ax", StartNode: "a", EndNode: "x", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "xb", StartNode: "x", EndNode: "b", Type: "CALLS"}))
	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a"},
		"depth":    3,
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	base := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 5, base.Meta.NodeCount)
	require.Equal(t, 1, base.Meta.ComponentCount)

	resp = makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              3,
		"exclude_properties": []string{"Context.context"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 4, payload.Meta.NodeCount)
	require.Equal(t, 2, payload.Meta.EdgeCount)
	require.Equal(t, 2, payload.Meta.ComponentCount)

	nodeIDs := make(map[string]bool)
	for _, node := range payload.Nodes {
		nodeIDs[node.ID] = true
	}
	require.Equal(t, map[string]bool{"a": true, "b": true, "c": true, "d": true}, nodeIDs)
	for _, edge := range payload.Edges {
		require.NotContains(t, []string{"ax", "xb"}, edge.ID)
	}

	// The two call chains are the two disconnected components.
	require.Len(t, payload.Components, 2)
	require.Equal(t, "component", payload.Components[0].Meta.GeneratedFrom)
	require.Equal(t, 2, payload.Components[0].Meta.NodeCount)
	require.Equal(t, 1, payload.Components[0].Meta.EdgeCount)
	require.Equal(t, []string{"a", "c"}, []string{payload.Components[0].Nodes[0].ID, payload.Components[0].Nodes[1].ID})
	require.Equal(t, []string{"ac"}, []string{payload.Components[0].Edges[0].ID})
	require.Equal(t, []string{"b", "d"}, []string{payload.Components[1].Nodes[0].ID, payload.Components[1].Nodes[1].ID})
	require.Equal(t, []string{"bd"}, []string{payload.Components[1].Edges[0].ID})
}

// TestGraphNeighborhoodEndpoint_ExcludeRelationshipType removes matching
// edges while keeping the nodes they touched.
func TestGraphNeighborhoodEndpoint_ExcludeRelationshipType(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for _, id := range []string{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Code"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "IMPORTS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":                   []string{"a"},
		"depth":                      2,
		"exclude_relationship_types": []string{"IMPORTS"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 3, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	require.Equal(t, "ab", payload.Edges[0].ID)
	// c is now an isolated node: its own component.
	require.Equal(t, 2, payload.Meta.ComponentCount)
}

// TestGraphNeighborhoodEndpoint_IncludeProperties keeps only nodes and edges
// matching the scoped property path, both as a traversal gate and as the
// result; a node carrying the property under a different label must not
// match a scoped entry.
func TestGraphNeighborhoodEndpoint_IncludeProperties(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Code"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"Code"}})
	require.NoError(t, err)
	// e has the property but a different label: a scoped "Code.entry" filter
	// only gates Code nodes, so exclude its label explicitly to drop it.
	_, err = engine.CreateNode(&storage.Node{ID: "e", Labels: []string{"Topic"}, Properties: map[string]interface{}{"entry": true}})
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "cd", StartNode: "c", EndNode: "d", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ae", StartNode: "a", EndNode: "e", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "CALLS"}))

	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              3,
		"include_properties": []string{"Code.entry"},
		"exclude_labels":     []string{"Topic"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 2, payload.Meta.NodeCount)
	require.Equal(t, 1, payload.Meta.EdgeCount)
	for _, node := range payload.Nodes {
		require.Contains(t, node.Labels, "Code")
		require.Contains(t, node.Properties, "entry")
	}
	require.Equal(t, "ac", payload.Edges[0].ID)
	require.Equal(t, 1, payload.Meta.ComponentCount)
	require.Equal(t, []string{"a", "c"}, []string{payload.Components[0].Nodes[0].ID, payload.Components[0].Nodes[1].ID})

	// Combined include + exclude: the include gate keeps {a, c, e} (e is
	// irrelevant to the Code scope), then the exclude removes every
	// remaining node, leaving an empty fragmented result.
	resp = makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              3,
		"include_properties": []string{"Code.entry"},
		"exclude_labels":     []string{"Code", "Topic"},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	empty := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 0, empty.Meta.NodeCount)
	require.Equal(t, 0, empty.Meta.EdgeCount)
	require.Equal(t, 0, empty.Meta.ComponentCount)
}

// TestGraphComponents_IsolatedNodes assigns isolated nodes their own
// components and orders components by node count descending.
func TestGraphComponents_IsolatedNodes(t *testing.T) {
	collection := newGraphCollection()
	collection.addNode(&storage.Node{ID: "a", Labels: []string{"Code"}}, "")
	collection.addNode(&storage.Node{ID: "b", Labels: []string{"Code"}}, "")
	collection.addNode(&storage.Node{ID: "c", Labels: []string{"Code"}}, "")
	collection.addEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}, "")

	components := collection.components()
	require.Len(t, components, 2)
	require.Equal(t, "component", components[0].Meta.GeneratedFrom)
	require.Equal(t, 2, components[0].Meta.NodeCount)
	require.Equal(t, 1, components[0].Meta.EdgeCount)
	require.Equal(t, []string{"a", "b"}, []string{components[0].Nodes[0].ID, components[0].Nodes[1].ID})
	require.Equal(t, 1, components[1].Meta.NodeCount)
	require.Equal(t, 0, components[1].Meta.EdgeCount)
	require.Equal(t, []string{"c"}, []string{components[1].Nodes[0].ID})
}

func TestParseGraphPropertyFilter(t *testing.T) {
	require.Nil(t, parseGraphPropertyFilter(""))
	require.Nil(t, parseGraphPropertyFilter(" . . "))
	require.Equal(t, graphPropertyFilter{path: []string{"context"}}, *parseGraphPropertyFilter("context"))
	require.Equal(t, graphPropertyFilter{scope: "Context", path: []string{"context"}}, *parseGraphPropertyFilter("Context.context"))
	require.Equal(t, graphPropertyFilter{scope: "Context", path: []string{"context", "nested"}}, *parseGraphPropertyFilter(" Context . context . nested "))

	ctxValue := "ctx"
	require.Equal(t, graphPropertyFilter{path: []string{"context"}, value: &ctxValue}, *parseGraphPropertyFilter("context:ctx"))
	require.Equal(t, graphPropertyFilter{scope: "Context", path: []string{"context"}, value: &ctxValue}, *parseGraphPropertyFilter("Context.context:ctx"))
	require.Equal(t, graphPropertyFilter{path: []string{"context"}, value: &ctxValue}, *parseGraphPropertyFilter("context: ctx "))
	require.Equal(t, graphPropertyFilter{scope: "context", path: []string{"Context"}}, *parseGraphPropertyFilter("context.Context"))
	require.Nil(t, parseGraphPropertyFilter("context:"))
}

func TestPropertyPathExists(t *testing.T) {
	props := map[string]interface{}{
		"a": int64(1),
		"b": map[string]interface{}{"c": "v"},
	}
	require.True(t, propertyPathExists(props, []string{"a"}))
	require.True(t, propertyPathExists(props, []string{"b", "c"}))
	require.False(t, propertyPathExists(props, []string{"missing"}))
	require.False(t, propertyPathExists(props, []string{"a", "c"}))
	require.False(t, propertyPathExists(props, []string{"b", "c", "d"}))
	require.False(t, propertyPathExists(nil, []string{"a"}))
	require.False(t, propertyPathExists(props, nil))
}

func TestPropertyPathMatchesValues(t *testing.T) {
	props := map[string]interface{}{
		"text":  "ctx",
		"count": int64(7),
		"ratio": 2.5,
		"flag":  true,
		"empty": nil,
	}
	ctx := "ctx"
	require.True(t, propertyPathMatches(props, []string{"text"}, &ctx))
	require.False(t, propertyPathMatches(props, []string{"text"}, strPtr("other")))
	require.True(t, propertyPathMatches(props, []string{"count"}, strPtr("7")))
	require.True(t, propertyPathMatches(props, []string{"ratio"}, strPtr("2.5")))
	require.True(t, propertyPathMatches(props, []string{"flag"}, strPtr("true")))
	require.True(t, propertyPathMatches(props, []string{"empty"}, strPtr("null")))
	require.False(t, propertyPathMatches(props, []string{"count"}, strPtr("8")))
	require.True(t, propertyPathMatches(props, []string{"text"}, nil))
	require.False(t, propertyPathMatches(props, []string{"missing"}, nil))
}

func strPtr(value string) *string {
	return &value
}

func TestGraphFilterSet_ExclusionMatching(t *testing.T) {
	filters := newGraphFilterSet(nil, nil).withFilters(nil, []string{"Hidden"}, []string{"IMPORTS"}, []string{"context"})

	node := graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}}
	require.True(t, filters.excludeNodePayload(node))
	node = graphNodePayload{ID: "n", Labels: []string{"Other"}, Properties: map[string]interface{}{"context": "ctx"}}
	require.True(t, filters.excludeNodePayload(node), "unscoped path matches any label")
	node = graphNodePayload{ID: "n", Labels: []string{"Hidden"}, Properties: nil}
	require.True(t, filters.excludeNodePayload(node))

	edge := graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "IMPORTS"}
	require.True(t, filters.excludeEdgePayload(edge))
	edge = graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS", Properties: map[string]interface{}{"context": "ctx"}}
	require.True(t, filters.excludeEdgePayload(edge), "unscoped path matches any edge type")
	edge = graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS"}
	require.False(t, filters.excludeEdgePayload(edge))
	require.True(t, filters.hasExclusions())
	require.False(t, newGraphFilterSet(nil, nil).hasExclusions())

	// A scoped path only matches the named label or edge type.
	scoped := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, []string{"Context.context"})
	require.True(t, scoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.False(t, scoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Other"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.False(t, scoped.excludeEdgePayload(graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS", Properties: map[string]interface{}{"context": "ctx"}}))
	require.True(t, scoped.excludeEdgePayload(graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "Context", Properties: map[string]interface{}{"context": "ctx"}}))

	// A dotted entry also matches the node's symbol name exactly, so
	// "context.Context" hides the variable named context.Context even
	// though its labels carry no such scope.
	nameFilter := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, []string{"context.Context"})
	require.True(t, nameFilter.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "context.Context", "id": "pkg_context_context"}}))
	require.False(t, nameFilter.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "context.Background"}}))

	// ":value" constrains the property value.
	valueFilter := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, []string{"Context.context:ctx"})
	require.True(t, valueFilter.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.False(t, valueFilter.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "other"}}))
}

func TestGraphFilterSet_IncludePropertyGates(t *testing.T) {
	// Label-scoped include gates only nodes carrying that label; other
	// labels are unaffected (relevance model).
	filters := newGraphFilterSet(nil, nil).withFilters([]string{"Code.entry"}, nil, nil, nil)
	require.True(t, filters.allowNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}}))
	require.False(t, filters.allowNode(&storage.Node{ID: "b", Labels: []string{"Code"}}))
	require.True(t, filters.allowNode(&storage.Node{ID: "e", Labels: []string{"Topic"}, Properties: map[string]interface{}{"entry": true}}), "label scope does not gate other labels")
	require.True(t, filters.allowEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}), "label scope must not gate edges")

	// Type-scoped include gates edges: a CALLS edge without the property is
	// rejected, one with it passes, and other types are unaffected.
	edgeFilters := newGraphFilterSet(nil, nil).withFilters([]string{"CALLS.entry"}, nil, nil, nil)
	require.False(t, edgeFilters.allowEdge(&storage.Edge{ID: "x", StartNode: "a", EndNode: "b", Type: "CALLS"}))
	require.True(t, edgeFilters.allowEdge(&storage.Edge{ID: "y", StartNode: "a", EndNode: "b", Type: "CALLS", Properties: map[string]interface{}{"entry": true}}))
	require.True(t, edgeFilters.allowEdge(&storage.Edge{ID: "z", StartNode: "a", EndNode: "b", Type: "IMPORTS"}))
	require.True(t, edgeFilters.allowNode(&storage.Node{ID: "n", Labels: []string{"Code"}}), "type scope must not gate nodes")

	// Unscoped include gates nodes only; edges stay free.
	unscoped := newGraphFilterSet(nil, nil).withFilters([]string{"entry"}, nil, nil, nil)
	require.True(t, unscoped.allowNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}}))
	require.False(t, unscoped.allowNode(&storage.Node{ID: "b", Labels: []string{"Code"}}))
	require.True(t, unscoped.allowEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))
}
