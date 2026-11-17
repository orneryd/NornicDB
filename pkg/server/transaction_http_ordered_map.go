package server

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

type transactionHTTPValueState struct {
	rest         bool
	graph        *GraphResult
	mapKeyOrders map[uintptr][]string
	nodes        map[string]struct{}
	edges        map[string]struct{}
}

func (state *transactionHTTPValueState) addNode(node GraphNode) {
	if state.nodes == nil {
		state.nodes = make(map[string]struct{})
	}
	if _, exists := state.nodes[node.ElementID]; exists {
		return
	}
	state.nodes[node.ElementID] = struct{}{}
	state.graph.Nodes = append(state.graph.Nodes, node)
}

func (state *transactionHTTPValueState) addEdge(edge GraphRelationship) {
	if state.edges == nil {
		state.edges = make(map[string]struct{})
	}
	if _, exists := state.edges[edge.ElementID]; exists {
		return
	}
	state.edges[edge.ElementID] = struct{}{}
	state.graph.Relationships = append(state.graph.Relationships, edge)
}

// transactionHTTPPoint is a point as Neo4j's HTTP API writes it (#817):
// {"type":"Point","coordinates":[x,y(,z)],"crs":{"srid":…,"name":…,
// "type":"link","properties":{"href":…,"type":"ogcwkt"}}}, keys in that order.
func transactionHTTPPoint(point cypher.CypherPoint) valueconvert.OrderedJSONMap {
	properties := valueconvert.OrderedJSONMap{Keys: []string{"href", "type"}, Values: map[string]interface{}{"href": point.CRSHref(), "type": "ogcwkt"}}
	crs := valueconvert.OrderedJSONMap{Keys: []string{"srid", "name", "type", "properties"}, Values: map[string]interface{}{"srid": int64(point.SRID), "name": point.CRSName(), "type": "link", "properties": properties}}
	coordinates := make([]interface{}, 0, 3)
	for _, coordinate := range point.Coordinates() {
		coordinates = append(coordinates, transactionHTTPFloat(coordinate, 64))
	}
	return valueconvert.OrderedJSONMap{Keys: []string{"type", "coordinates", "crs"}, Values: map[string]interface{}{"type": "Point", "coordinates": coordinates, "crs": crs}}
}

// transactionHTTPVectorTypeNames are the vector classes Neo4j's HTTP API
// names, by coordinate type.
var transactionHTTPVectorTypeNames = map[cypher.VectorCoordinateType]string{
	cypher.VectorInteger64: "Int64Vector", cypher.VectorInteger32: "Int32Vector", cypher.VectorInteger16: "Int16Vector",
	cypher.VectorInteger8: "Int8Vector", cypher.VectorFloat64: "Float64Vector", cypher.VectorFloat32: "Float32Vector",
}

// transactionHTTPVector is a VECTOR as Neo4j 2026.09's HTTP API writes it:
// not its coordinates but {"typeName":"Int64Vector","incomparableType":true,
// "sequenceValue":false}, keys in that order (#907).
func transactionHTTPVector(vector cypher.CypherVector) valueconvert.OrderedJSONMap {
	return valueconvert.OrderedJSONMap{Keys: []string{"typeName", "incomparableType", "sequenceValue"}, Values: map[string]interface{}{
		"typeName": transactionHTTPVectorTypeNames[vector.Type], "incomparableType": true, "sequenceValue": false,
	}}
}
