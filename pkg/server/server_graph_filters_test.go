package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func propFilter(scope, property, value string) graphPropertyFilterRequest {
	entry := graphPropertyFilterRequest{Scope: scope, Property: property}
	if value != "" {
		entry.Value = &value
	}
	return entry
}

// TestGraphNeighborhoodEndpoint_ExcludeSymbolNameFragmentsGraph mirrors the
// graphify context-threading bias: a symbol named context.Context sits on the
// path between otherwise separate call chains. Excluding that name must
// remove the node, its incident edges, and fragment the result into the two
// disconnected call chains.
func TestGraphNeighborhoodEndpoint_ExcludeSymbolNameFragmentsGraph(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	for id, label := range map[string]string{
		"a": "Code", "b": "Code", "c": "Code", "d": "Code",
	} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}})
		require.NoError(t, err)
	}
	// The context symbol threads every parser call: a -> x -> b links the
	// two otherwise independent chains a -> c and b -> d.
	_, err := engine.CreateNode(&storage.Node{ID: "x", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "context.Context"}})
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
		"node_ids":      []string{"a"},
		"depth":         3,
		"exclude_names": []string{"context.Context"},
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

// TestGraphNeighborhoodEndpoint_ExcludePropertyWithValue constrains exclusion
// to the property value when the request carries one.
func TestGraphNeighborhoodEndpoint_ExcludePropertyWithValue(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"context": "ctx"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Code"}, Properties: map[string]interface{}{"context": "other"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))

	ctx := "ctx"
	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a"},
		"depth":    2,
		"exclude_properties": []map[string]interface{}{
			{"scope": "Code", "property": "context", "value": &ctx},
		},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)

	payload := decodeGraphPayload(t, resp.Body)
	require.Equal(t, 1, payload.Meta.NodeCount)
	require.Equal(t, "b", payload.Nodes[0].ID)
	require.Equal(t, 0, payload.Meta.EdgeCount)
	require.Equal(t, 1, payload.Meta.ComponentCount)

	other := "other"
	resp = makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids": []string{"a"},
		"depth":    2,
		"exclude_properties": []map[string]interface{}{
			{"scope": "Code", "property": "context", "value": &other},
		},
	}, "Bearer "+token)
	require.Equal(t, 200, resp.Code)
	payload = decodeGraphPayload(t, resp.Body)
	require.Equal(t, 1, payload.Meta.NodeCount)
	require.Equal(t, "a", payload.Nodes[0].ID)
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

// TestGraphNeighborhoodEndpoint_IncludeNamesAndProperties keeps only nodes
// matching the name gate and the applicable property gate.
func TestGraphNeighborhoodEndpoint_IncludeNamesAndProperties(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	engine := getDefaultStorage(t, server)

	_, err := engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "symA", "entry": true}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "symB"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "symC", "entry": true}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "e", Labels: []string{"Topic"}, Properties: map[string]interface{}{"label": "symE", "entry": true}})
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ae", StartNode: "a", EndNode: "e", Type: "CALLS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "CALLS"}))

	// Name gate: only symA, symC, symE may enter. Property gate: Code nodes
	// must carry entry. e (Topic) passes the name gate but is dropped by the
	// exclude_labels below; b fails the name gate and never enters.
	resp := makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              3,
		"include_names":      []string{"symA", "symC", "symE"},
		"include_properties": []map[string]interface{}{{"scope": "Code", "property": "entry"}},
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

	// Include then exclude: the include gates keep {a, c, e}, then the
	// exclude removes every remaining node, leaving an empty result.
	resp = makeRequest(t, server, "POST", defaultGraphPath(server, "neighborhood"), map[string]interface{}{
		"node_ids":           []string{"a"},
		"depth":              3,
		"include_names":      []string{"symA", "symC", "symE"},
		"include_properties": []map[string]interface{}{{"scope": "Code", "property": "entry"}},
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

func TestNewGraphPropertyFilter(t *testing.T) {
	require.Nil(t, newGraphPropertyFilter(graphPropertyFilterRequest{Scope: "Code"}))
	require.Nil(t, newGraphPropertyFilter(graphPropertyFilterRequest{Property: "  "}))

	plain := newGraphPropertyFilter(graphPropertyFilterRequest{Scope: " Code ", Property: " entry "})
	require.Equal(t, "Code", plain.scope)
	require.Equal(t, "entry", plain.property)
	require.Nil(t, plain.value)

	empty := ""
	valued := newGraphPropertyFilter(graphPropertyFilterRequest{Property: "context", Value: &empty})
	require.NotNil(t, valued)
	require.Nil(t, valued.value)

	ctx := "ctx"
	valued = newGraphPropertyFilter(graphPropertyFilterRequest{Property: "context", Value: &ctx})
	require.NotNil(t, valued.value)
	require.Equal(t, "ctx", *valued.value)
}

func TestPropertyMatches(t *testing.T) {
	props := map[string]interface{}{
		"text":  "ctx",
		"count": int64(7),
		"ratio": 2.5,
		"flag":  true,
		"empty": nil,
	}
	ctx := "ctx"
	require.True(t, propertyMatches(props, "text", &ctx))
	require.False(t, propertyMatches(props, "text", strPtr("other")))
	require.True(t, propertyMatches(props, "count", strPtr("7")))
	require.True(t, propertyMatches(props, "ratio", strPtr("2.5")))
	require.True(t, propertyMatches(props, "flag", strPtr("true")))
	require.True(t, propertyMatches(props, "empty", strPtr("null")))
	require.False(t, propertyMatches(props, "count", strPtr("8")))
	require.True(t, propertyMatches(props, "text", nil))
	require.False(t, propertyMatches(props, "missing", nil))
	require.False(t, propertyMatches(nil, "text", nil))
}

func strPtr(value string) *string {
	return &value
}

func TestGraphFilterSet_ExclusionMatching(t *testing.T) {
	filters := newGraphFilterSet(nil, nil).withFilters(
		nil, []string{"context.Context"}, nil, nil, []string{"Hidden"}, []string{"IMPORTS"},
	)

	node := graphNodePayload{ID: "n", Labels: []string{"Hidden"}, Properties: nil}
	require.True(t, filters.excludeNodePayload(node))
	node = graphNodePayload{ID: "n", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "context.Context"}}
	require.True(t, filters.excludeNodePayload(node), "name filter matches the symbol name")
	node = graphNodePayload{ID: "n", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "context.Background"}}
	require.False(t, filters.excludeNodePayload(node))

	edge := graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "IMPORTS"}
	require.True(t, filters.excludeEdgePayload(edge))
	edge = graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS"}
	require.False(t, filters.excludeEdgePayload(edge))
	require.True(t, filters.hasExclusions())
	require.False(t, newGraphFilterSet(nil, nil).hasExclusions())

	// Scoped property exclusion with a value constraint.
	scoped := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, []graphPropertyFilterRequest{
		propFilter("Context", "context", "ctx"),
	}, nil, nil)
	require.True(t, scoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.False(t, scoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "other"}}))
	require.False(t, scoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Other"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.False(t, scoped.excludeEdgePayload(graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS", Properties: map[string]interface{}{"context": "ctx"}}))
	require.True(t, scoped.excludeEdgePayload(graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "Context", Properties: map[string]interface{}{"context": "ctx"}}))

	// Unscoped property exclusion applies to any node or edge.
	unscoped := newGraphFilterSet(nil, nil).withFilters(nil, nil, nil, []graphPropertyFilterRequest{
		propFilter("", "context", ""),
	}, nil, nil)
	require.True(t, unscoped.excludeNodePayload(graphNodePayload{ID: "n", Labels: []string{"Context"}, Properties: map[string]interface{}{"context": "ctx"}}))
	require.True(t, unscoped.excludeEdgePayload(graphEdgePayload{ID: "e", Source: "a", Target: "b", Type: "CALLS", Properties: map[string]interface{}{"context": "ctx"}}))
}

func TestGraphFilterSet_IncludePropertyGates(t *testing.T) {
	// Label-scoped include gates only nodes carrying that label; other
	// labels are unaffected (relevance model).
	filters := newGraphFilterSet(nil, nil).withFilters(nil, nil, []graphPropertyFilterRequest{
		propFilter("Code", "entry", ""),
	}, nil, nil, nil)
	require.True(t, filters.allowNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}}))
	require.False(t, filters.allowNode(&storage.Node{ID: "b", Labels: []string{"Code"}}))
	require.True(t, filters.allowNode(&storage.Node{ID: "e", Labels: []string{"Topic"}, Properties: map[string]interface{}{"entry": true}}), "label scope does not gate other labels")
	require.True(t, filters.allowEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}), "label scope must not gate edges")

	// Type-scoped include gates edges: a CALLS edge without the property is
	// rejected, one with it passes, and other types are unaffected.
	edgeFilters := newGraphFilterSet(nil, nil).withFilters(nil, nil, []graphPropertyFilterRequest{
		propFilter("CALLS", "entry", ""),
	}, nil, nil, nil)
	require.False(t, edgeFilters.allowEdge(&storage.Edge{ID: "x", StartNode: "a", EndNode: "b", Type: "CALLS"}))
	require.True(t, edgeFilters.allowEdge(&storage.Edge{ID: "y", StartNode: "a", EndNode: "b", Type: "CALLS", Properties: map[string]interface{}{"entry": true}}))
	require.True(t, edgeFilters.allowEdge(&storage.Edge{ID: "z", StartNode: "a", EndNode: "b", Type: "IMPORTS"}))
	require.True(t, edgeFilters.allowNode(&storage.Node{ID: "n", Labels: []string{"Code"}}), "type scope must not gate nodes")

	// Unscoped include gates nodes only; edges stay free.
	unscoped := newGraphFilterSet(nil, nil).withFilters(nil, nil, []graphPropertyFilterRequest{
		propFilter("", "entry", ""),
	}, nil, nil, nil)
	require.True(t, unscoped.allowNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"entry": true}}))
	require.False(t, unscoped.allowNode(&storage.Node{ID: "b", Labels: []string{"Code"}}))
	require.True(t, unscoped.allowEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}))

	// Name gate: only listed symbol names may enter.
	nameFilters := newGraphFilterSet(nil, nil).withFilters([]string{"symA", "symB"}, nil, nil, nil, nil, nil)
	require.True(t, nameFilters.allowNode(&storage.Node{ID: "a", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "symA"}}))
	require.False(t, nameFilters.allowNode(&storage.Node{ID: "b", Labels: []string{"Code"}, Properties: map[string]interface{}{"label": "symC"}}))
	require.False(t, nameFilters.allowNode(&storage.Node{ID: "c", Labels: []string{"Code"}}))
	require.True(t, nameFilters.allowEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CALLS"}), "names gate nodes, not edges")
}
