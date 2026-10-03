package server

import (
	"bytes"
	"encoding/json"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

type transactionHTTPOrderedMap struct {
	keys   []string
	values map[string]interface{}
}

type transactionHTTPValueState struct {
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

func (value transactionHTTPOrderedMap) MarshalJSON() ([]byte, error) {
	var encoded bytes.Buffer
	encoded.WriteByte('{')
	for index, key := range value.keys {
		if index > 0 {
			encoded.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		item, err := json.Marshal(value.values[key])
		if err != nil {
			return nil, err
		}
		encoded.Write(name)
		encoded.WriteByte(':')
		encoded.Write(item)
	}
	encoded.WriteByte('}')
	return encoded.Bytes(), nil
}

// transactionHTTPPoint is a point as Neo4j's HTTP API writes it (#817):
// {"type":"Point","coordinates":[x,y(,z)],"crs":{"srid":…,"name":…,
// "type":"link","properties":{"href":…,"type":"ogcwkt"}}}, keys in that order.
func transactionHTTPPoint(point cypher.CypherPoint) transactionHTTPOrderedMap {
	properties := transactionHTTPOrderedMap{keys: []string{"href", "type"}, values: map[string]interface{}{"href": point.CRSHref(), "type": "ogcwkt"}}
	crs := transactionHTTPOrderedMap{keys: []string{"srid", "name", "type", "properties"}, values: map[string]interface{}{"srid": int64(point.SRID), "name": point.CRSName(), "type": "link", "properties": properties}}
	coordinates := make([]interface{}, 0, 3)
	for _, coordinate := range point.Coordinates() {
		coordinates = append(coordinates, transactionHTTPFloat(coordinate, 64))
	}
	return transactionHTTPOrderedMap{keys: []string{"type", "coordinates", "crs"}, values: map[string]interface{}{"type": "Point", "coordinates": coordinates, "crs": crs}}
}
