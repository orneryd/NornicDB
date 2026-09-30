package server

import (
	"bytes"
	"encoding/json"
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
